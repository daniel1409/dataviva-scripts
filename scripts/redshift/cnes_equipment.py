import click
import time
import pandas as pd
from io import BytesIO
from os import path

from s3 import S3
from notification import Notification
from location import Location
from product import Product


class CnesEquipment():
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
            names=['cnes', 'municipality', 'pf_pj', 'cpf_cnpj', 'niv_dep', 'cnpj_man', 'tp_unid', 'equipment_type', 'codequip', 'qt_exist', 'qt_uso', 'ind_sus', 'competen', 'regsaude', 'retencao_2', 'niv_hier_2', 'esfera'],
            usecols=['cnes', 'municipality'],
            converters={
                'cnes': str,
                'municipality': str,
            },
            engine='c'
        )

    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'region', 'mesoregion', 'microregion', 'state', 'municipality', 'cnes', 'equipment_type']
        )

        self.s3.resource.Object('dataviva-etl', path.join(output, self.filename)).put(Body=csv_buffer.getvalue())
        
        self.duration = time.time() - self.start
        self.duration_str = '%02d:%02d' % (self.duration / 60, self.duration % 60)
        print '  Saved.'
        print '  Time: %s' % self.duration_str

    def add_year(self):
        try:
            year = self.filename.replace('_','.').split('.')[2]
            self.df['year'] = int(year)
        except:
            print 'Filename must be in format "cnes_equipment_yyyy.csv".'

        print '+ year'    


@click.command()
@click.argument('input', default='redshift/raw_from_mysql/cnes/cnes_equipment', type=click.Path())
@click.argument('output', default='redshift/raw_from_mysql/cnes_formatted', type=click.Path())
def main(input, output):
    s3 = S3()
    notification = Notification()
    location = Location(s3)
    
    for csv_path in s3.get_keys(input):
        cnes_equipment = CnesEquipment(s3, csv_path)
        cnes_equipment.add_year()

        cnes_equipment.df = location.fix_municipality(cnes_equipment.df)
        cnes_equipment.df = location.add_columns(cnes_equipment.df)

        cnes_equipment.save(output)
        notification.send_email('sauloantuness@gmail.com', cnes_equipment.filename, cnes_equipment.duration_str)


if __name__ == '__main__':
    main()

