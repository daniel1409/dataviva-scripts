import click
import time
import pandas as pd
from io import BytesIO
from os import path

from s3 import S3
from notification import Notification
from location import Location


class CnesBed():
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
            names=['cnes', 'codmun', 'tp_unid', 'tp_leito', 'codleito', 'qt_exist', 'qt_contr', 'qt_sus', 'qt_nsus', 'competen1', 'regsaude'],
            converters={
                'cnes' : str,
                'codmun' : str,
                'tp_unid' : str,
                'tp_leito' : int,
                'codleito' : str,
                'qt_exist' : int,
                'qt_contr' : int,
                'qt_sus' : int,
                'qt_nsus' : int,
                'competen1' : int,
                'regsaude' : str
            },
            engine='c'
        ).rename(columns={
            'cnes' : 'establishment',
            'codmun': 'municipality',
            'tp_unid' : 'unit_type',
            'tp_leito' : 'bed_type',
            'codleito' : 'bed_type_per_specialty',
            'qt_exist' : 'number_existing_bed',
            'qt_contr' : 'number_existing_contract',
            'qt_sus' : 'number_sus_bed',
            'qt_nsus' : 'number_non_sus_bed',
            'competen1' : 'year',
            'regsaude' : 'health_region'
        })

    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'region', 'mesoregion', 'microregion', 'state', 'municipality', 'establishment', 'unit_type', 'bed_type', 'bed_type_per_specialty', 'number_existing_bed', 'number_existing_contract', 'number_sus_bed', 'number_non_sus_bed', 'health_region']
        )

        self.s3.resource.Object('dataviva-etl', path.join(output, self.filename)).put(Body=csv_buffer.getvalue())
        
        self.duration = time.time() - self.start
        self.duration_str = '%02d:%02d' % (self.duration / 60, self.duration % 60)
        print '  Saved.'
        print '  Time: %s' % self.duration_str

@click.command()
@click.argument('input', default='redshift/raw_from_mysql/cnes/cnes_bed', type=click.Path())
@click.argument('output', default='redshift/raw_from_mysql/cnes_formatted', type=click.Path())
def main(input, output):
    s3 = S3()
    notification = Notification()
    location = Location(s3)

    for csv_path in s3.get_keys(input):
        cnes_bed = CnesBed(s3, csv_path)

        cnes_bed.df = location.fix_municipality(cnes_bed.df)
        cnes_bed.df = location.add_columns(cnes_bed.df)
        cnes_bed.save(output)
        # notification.send_email('sauloantuness@gmail.com', cnes_bed.filename, cnes_bed.duration_str)


if __name__ == '__main__':
    main()
