import click
import time
import pandas as pd
from io import BytesIO
from os import path

from s3 import S3
from notification import Notification
from location import Location


class CnesEstablishment():
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
            names=['cnes', 'tp_unid', 'codmun', 'vinc_sus', 'tp_prest', 'nivate_a', 'nivate_h', 'urgemerg', 'atendamb', 'centrcir', 'centrobs', 'centrneo', 'atendhos', 'coletres', 'niv_dep1', 'regsaude', 'esfera', 'retencao_2', 'niv_hier_2', 'competen1'],
            converters={
                'cnes': str,
                'municipality': str,
                'sus_bond': str,
                'provider_type': str,
                'ambulatory_attention': str,
                'hospital_attention': str,
                'emergency_facilities': str,
                'ambulatory_care_facilities': str,
                'surgery_center_facilities': int,
                'obstetrical_center_facilities': int,
                'neonatal_unit_facilities': str,
                'hospital_care': str,
                'selective_waste_collection': str,
                'year': int,
                'dependency_level': str,
                'health_region': str,
                'administrative_sphere': str,
                'tax_withholding': str,
                'hierarchy_level': str,
                'unid_type': str,
            },
            engine='c'
        ).rename(columns={
            'tp_unid': 'unit_type',
            'codmun': 'municipality',
            'vinc_sus': 'sus_bond',
            'tp_prest': 'provider_type',
            'nivate_a': 'ambulatory_attention',
            'nivate_h': 'hospital_attention',
            'urgemerg': 'emergency_facilities',
            'atendamb': 'ambulatory_care_facilities',
            'centrobs': 'obstetrical_center_facilities',
            'centrneo': 'neonatal_unit_facilities',
            'atendhos': 'hospital_care',
            'coletres': 'selective_waste_collection',
            'niv_dep1': 'dependency_level',
            'regsaude': 'health_region',
            'esfera': 'administrative_sphere',
            'retencao_2': 'tax_withholding',
            'niv_hier_2': 'hierarchy_level',
            'competen1': 'year'
        })

    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'region', 'mesoregion', 'microregion', 'state', 'municipality', 'cnes', 'sus_bond', 'provider_type', 'ambulatory_attention', 'hospital_attention', 'emergency_facilities', 'ambulatory_care_facilities', 'surgery_center_facilities', 'obstetrical_center_facilities', 'neonatal_unit_facilities', 'hospital_care', 'selective_waste_collection', 'dependency_level', 'health_region', 'adminative_sphere', 'tax_withholding', 'hierarchy_level', 'unit_type']
        )

        self.s3.resource.Object('dataviva-etl', path.join(output, self.filename)).put(Body=csv_buffer.getvalue())
        
        self.duration = time.time() - self.start
        self.duration_str = '%02d:%02d' % (self.duration / 60, self.duration % 60)
        print '  Saved.'
        print '  Time: %s' % self.duration_str

@click.command()
@click.argument('input', default='redshift/raw_from_mysql/cnes/cnes_establishment', type=click.Path())
@click.argument('output', default='redshift/raw_from_mysql/cnes_formatted', type=click.Path())
def main(input, output):
    s3 = S3()
    notification = Notification()
    location = Location(s3)

    for csv_path in s3.get_keys(input):
        cnes_establishment = CnesEstablishment(s3, csv_path)

        cnes_establishment.df = location.fix_municipality(cnes_establishment.df)
        cnes_establishment.df = location.add_columns(cnes_establishment.df)

        cnes_establishment.save(output)
        # notification.send_email('sauloantuness@gmail.com', cnes_establishment.filename, cnes_establishment.duration_str)

if __name__ == '__main__':
    main()
