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
            names=['cnes', 'codmun', 'niv_dep', 'tp_unid', 'tipequip', 'codequip', 'qt_exist', 'qt_uso', 'ind_sus', 'competen1', 'regsaude'],
            converters={
                'cnes' : str,
                'codmun' : str,
                'niv_dep' : str,
                'tp_unid' : str,
                'tipequip' : str,
                'codequip' : str,
                'qt_exist' : str,
                'qt_uso' : str,
                'ind_sus' : str,
                'competen1' : int,
                'regsaude' : str,
            },
            engine='c'
        ).rename(columns={
            'cnes' : 'establishment',
            'codmun' : 'municipality',
            'niv_dep' : 'dependency_level',
            'tp_unid' : 'unit_type',
            'tipequip' : 'equipment_type',
            'codequip' : 'equipment_code',
            'qt_exist' : 'equipment_quantity',
            'qt_uso' : 'equipment_quantity_in_use',
            'ind_sus' : 'sus_availability_indicator',
            'competen1' : 'year',
            'regsaude' : 'health_region'
        })

    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'region', 'mesoregion', 'microregion', 'state', 'municipality', 'establishment', 'dependency_level', 'unit_type', 'equipment_type', 'equipment_code', 'equipment_quantity', 'equipment_quantity_in_use', 'sus_availability_indicator', 'health_region']
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

        cnes_equipment.df = location.fix_municipality(cnes_equipment.df)
        cnes_equipment.df = location.add_columns(cnes_equipment.df)

        cnes_equipment.save(output)
        # notification.send_email('sauloantuness@gmail.com', cnes_equipment.filename, cnes_equipment.duration_str)


if __name__ == '__main__':
    main()

