import click
import time
import pandas as pd
from io import BytesIO
from os import path

from s3 import S3
from notification import Notification
from location import Location
from product import Product


class Secex():
    def __init__(self, s3, csv_path):
        self.s3 = s3
        self.csv_path = csv_path
        self.filename = path.basename(csv_path)
        self.start = time.time()
        
        print self.filename
        self.open_df()

    def open_df(self):
        self.df = pd.read_csv(
            self.s3.read_csv(self.csv_path),
            sep=';',
            header=0,
            usecols=['CO_ANO', 'CO_MES', 'CO_PAIS', 'CO_PORTO', 'KG_LIQUIDO', 'VL_FOB', 'HS_07', 'MUN_IBGE'],
            converters={
                'CO_PAIS': str,
                'CO_PORTO': str,
                'HS_07': str,
                'MUN_IBGE': str,
                'KG_LIQUIDO': lambda x: int(float(x)),
                'VL_FOB': lambda x: int(float(x))
            }
        ).rename(columns={
            'CO_ANO': 'year',
            'CO_MES': 'month',
            'CO_PAIS': 'country',
            'CO_PORTO': 'port',
            'KG_LIQUIDO': 'kg',
            'VL_FOB': 'value',
            'HS_07': 'product',
            'MUN_IBGE': 'municipality'
        })

    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['type', 'year', 'month', 'continent', 'country', 'region', 'mesoregion', 'microregion', 'state', 'municipality', 'port', 'product_section', 'product_chapter', 'product', 'kg', 'value']
        )

        self.s3.resource.Object('dataviva-etl', path.join(output, self.filename)).put(Body=csv_buffer.getvalue())
        
        self.duration = time.time() - self.start
        self.duration_str = '%02d:%02d' % (self.duration / 60, self.duration % 60)
        print '  Saved.'
        print '  Time: %s' % self.duration_str

    def add_type(self):
        if 'import' in self.csv_path:
            self.df['type'] = 'import'
        
        elif 'export' in self.csv_path:
            self.df['type'] = 'export'

        else:
            print 'Filename must contain import or export.'
            raise Exception

        print '+ type'


@click.command()
@click.argument('input', default='redshift/raw_from_mysql/secex', type=click.Path())
@click.argument('output', default='redshift/raw_from_mysql/secex_formatted', type=click.Path())
def main(input, output):
    s3 = S3()
    notification = Notification()
    location = Location(s3)
    product = Product(s3)
    
    for csv_path in s3.get_keys(input):
        secex = Secex(s3, csv_path)
        secex.add_type()

        secex.df = location.add_columns(secex.df, continent=True)
        secex.df = product.add_columns(secex.df)

        secex.save(output)
        notification.send_email('sauloantuness@gmail.com', secex.filename, secex.duration_str)


if __name__ == '__main__':
    main()
