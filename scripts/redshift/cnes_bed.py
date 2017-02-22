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
            names=['cnes','municipality','regsaude','micr_reg','pf_pj','cpf_cnpj','niv_dep','cnpj_man','esfera_a','retencao','tp_unid','niv_hier','bed_type','codleito','qt_exist','qt_contr','qt_sus','qt_nsus','competen'],
            usecols=['cnes', 'municipality', 'bed_type'],
            converters={
                'municipality': str,
                'cnes': str,
            },
            engine='c'
        )

    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'region', 'mesoregion', 'microregion', 'state', 'municipality', 'cnes', 'bed_type']
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
            print 'Filename must be in format "bed_yyyy.csv".'

        print '+ year'

@click.command()
@click.argument('input', default='redshift/raw_from_mysql/cnes/cnes_bed', type=click.Path())
@click.argument('output', default='redshift/raw_from_mysql/cnes_formatted', type=click.Path())
def main(input, output):
    s3 = S3()
    notification = Notification()
    location = Location(s3)

    for csv_path in s3.get_keys(input):
        cnes_bed = CnesBed(s3, csv_path)

        cnes_bed.add_year()

        cnes_bed.df = location.fix_municipality(cnes_bed.df)
        cnes_bed.df = location.add_columns(cnes_bed.df)
        cnes_bed.save(output)
        # notification.send_email('sauloantuness@gmail.com', cnes_bed.filename, cnes_bed.duration_str)


if __name__ == '__main__':
    main()
