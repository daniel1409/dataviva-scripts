import click
import time
import pandas as pd
from io import BytesIO
from os import path

from s3 import S3
from notification import Notification
from location import Location
from product import Product

class Comtrade():
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
            sep=',',
            header=0,
            names=['year', 'country', 'PAIS_ORIGEM_DESC', 'COD_PRODUTO', 'weight', 'value', 'product'],
            usecols=['year', 'country', 'weight', 'value', 'product'],
            converters={
                'product': str,
                'country': lambda x: int(x) if x.isdigit() else '',
            },
            engine='c'
        )


    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'continent', 'country', 'product_section', 'product_chapter', 'product', 'weight', 'value'],
        )

        self.s3.resource.Object('dataviva-etl', path.join(output, self.filename)).put(Body=csv_buffer.getvalue())

        self.duration = time.time() - self.start
        self.duration_str = '%02d:%02d' % (self.duration / 60, self.duration % 60)
        print '  Saved.'
        print '  Time: %s' % self.duration_str


    def fix_country(self, location):
        self.df = pd.merge(
            self.df, 
            location.wld_df,
            left_on='country',
            right_on='id_num',
            how='left',
        )

        self.df.drop('country_x', 1, inplace=True)
        self.df.drop('id_num', 1, inplace=True)

        self.df.rename(columns={'country_y': 'country'}, inplace=True)

        print '- fix countries'


@click.command()
@click.argument('input', default='redshift/raw_from_mysql/comtrade/', type=click.Path())
@click.argument('output', default='redshift/raw_from_mysql/comtrade_formatted', type=click.Path())
def main(input, output):
    s3 = S3()
    product = Product(s3)
    location = Location(s3)
    notification = Notification()
    
    for csv_path in s3.get_keys(input):
        comtrade = Comtrade(s3, csv_path)
        comtrade.fix_country(location)

        comtrade.df = location.add_continent(comtrade.df)
        comtrade.df = product.add_columns(comtrade.df)

        comtrade.save(output)
        notification.send_email('sauloantuness@gmail.com', comtrade.filename, comtrade.duration_str)


if __name__ == '__main__':
    main()
