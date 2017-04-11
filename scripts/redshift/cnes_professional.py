import click
import time
import pandas as pd
from io import BytesIO
from os import path

from s3 import S3
from notification import Notification
from location import Location
from occupation import Occupation
from product import Product


class CnesProfessional():
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
            names=['cnes', 'codmun', 'tp_unid', 'cbo', 'cns_prof', 'vinculac', 'prof_sus', 'horaoutr', 'horahosp', 'hora_amb', 'competen1', 'regsaude', 'niv_hier_2'],
            converters={
                'cnes' : str,
                'codmun': str,
                'tp_unid' : str,
                'cbo' : lambda x: str(x)[:4],
                'cns_prof' : str,
                'vinculac' : str,
                'prof_sus' : str,
                'horaoutr' : int,
                'horahosp' : int,
                'hora_amb' : int,
                'competen1' : int,
                'regsaude' : str,
                'niv_hier_2' : str,
            },
            engine='c'
        ).rename(columns={
            'cnes' : 'establishment',
            'codmun': 'municipality',
            'tp_unid' : 'unit_type',
            'cbo' : 'occupation_family',
            'cns_prof' : 'cns_number',
            'vinculac' : 'professional_link',
            'prof_sus' : 'sus_healthcare_professional',
            'horaoutr' : 'other_hours_worked',
            'horahosp' : 'hospital_hour',
            'hora_amb' : 'ambulatory_hour',
            'competen1' : 'year',
            'regsaude' : 'health_region',
            'niv_hier_2': 'hierarchy_level',
        })

    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'region', 'mesoregion', 'microregion', 'state', 'municipality', 'establishment', 'unit_type', 'occupation_group', 'occupation_family', 'cns_number', 'professional_link', 'sus_healthcare_professional', 'other_hours_worked', 'hospital_hour', 'ambulatory_hour', 'health_region', 'hierarchy_level']
        )

        self.s3.resource.Object('dataviva-etl', path.join(output, self.filename)).put(Body=csv_buffer.getvalue())
        
        self.duration = time.time() - self.start
        self.duration_str = '%02d:%02d' % (self.duration / 60, self.duration % 60)
        print '  Saved.'
        print '  Time: %s' % self.duration_str


@click.command()
@click.argument('input', default='redshift/raw_from_mysql/cnes/cnes_professional', type=click.Path())
@click.argument('output', default='redshift/raw_from_mysql/cnes_formatted', type=click.Path())
def main(input, output):
    s3 = S3()
    notification = Notification()
    location = Location(s3)
    occupation = Occupation(s3)
    
    for csv_path in s3.get_keys(input):
        cnes_professional = CnesProfessional(s3, csv_path)

        cnes_professional.df = location.fix_municipality(cnes_professional.df)
        cnes_professional.df = location.add_columns(cnes_professional.df)
        cnes_professional.df = occupation.add_columns(cnes_professional.df)

        cnes_professional.save(output)
        # notification.send_email('sauloantuness@gmail.com', cnes_professional.filename, cnes_professional.duration_str)


if __name__ == '__main__':
    main()
