import click
import time
import pandas as pd
from io import BytesIO
from os import path

from s3 import S3
from location import Location
from occupation import Occupation
from economic_activity import EconomicActivity

class Rais():
    def __del__(self):
        total = time.time() - self.start

        print '  Time: %02d:%02d\n' % (total / 60, total % 60)

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
            names=['employee', 'establishment', 'municipality', 'gender', 'age', 'ethnicity', 'literacy', 'wage', 'occupation', 'cnae', 'establishment_size', 'simple', 'legal_nature'],
            converters={
                'employee': str,
                'establishment': str,
                'municipality': str,
                'occupation': str,
                'cnae': str
            },
            dtype={
                'employee': 'str',
                'establishment': 'str',
                'municipality': 'str',
                'gender': 'int8',
                'age': 'int8',
                'ethnicity': 'int8',
                'literacy': 'int8',
                'wage': 'float64',
                'occupation': 'str',
                'cnae': 'str',
                'establishment_size': 'int8',
                'simple': 'int8',
                'legal_nature': 'int8'
            },
            engine='c'
        )

    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'region', 'mesoregion', 'microregion', 'state', 'municipality', 'occupation', 'occupation_group', 'cnae', 'cnae_division', 'cnae_section', 'establishment', 'employee', 'ethnicity', 'establishment_size', 'gender', 'legal_nature', 'literacy', 'simple', 'age', 'wage']
        )

        self.s3.resource.Object('dataviva-etl', path.join(output, self.filename)).put(Body=csv_buffer.getvalue())
        print '  Saved.'

    def add_year(self):
        try:
            year = self.filename.replace('_','.').split('.')[1]
            self.df['year'] = int(year)
        except:
            print 'Filename must be in format "rais_yyyy.csv".'

        print '+ year'

    def fix_municipality(self, location):
        municipalities_6 = []
        municipalities_7 = []

        for _, row in location.municipalities_df.iterrows():
                id_ibge = row['municipality'][:-1]
                municipalities_6.append(id_ibge)
                municipalities_7.append(self.add_dv_to_id_ibge(id_ibge))

        municipalities_df = pd.DataFrame({
            'municipality': pd.Series(municipalities_6),
            'municipality7': pd.Series(municipalities_7)
        })

        self.df = pd.merge(
            self.df, 
            municipalities_df,
            on='municipality',
            how='left'
        )

        self.df.drop('municipality', 1, inplace=True)
        self.df.rename(
            columns={
                'municipality7': 'municipality'
            }, 
            inplace=True
        )

        print '- fix municipalities'


    def add_dv_to_id_ibge(self, id_ibge):
        invalid = {
            '220191': '2201919',
            '220225': '2202251',
            '220198': '2201988',
            '261153': '2611533',
            '311783': '3117836',
            '315213': '3152131',
            '430587': '4305871',
            '520393': '5203939',
            '520396': '5203962'
        }
        
        if invalid.get(id_ibge):
            return invalid.get(id_ibge)

        pesos = [1,2,1,2,1,2]
        ponderacao = []

        for i in range(6):
            ponderacao.append(int(id_ibge[i]) * pesos[i])

        ponderacao = map(lambda x: x if x < 10 else 1 + (x % 10), ponderacao)

        soma = reduce(lambda x, y: x + y, ponderacao)

        resto = soma % 10

        if resto:
            dv = 10 - resto
        else:
            dv = 0

        return id_ibge + str(dv)


@click.command()
@click.argument('input', default='redshift/raw_from_mysql/rais', type=click.Path())
@click.argument('output', default='redshift/raw_from_mysql/rais_formatted', type=click.Path())
def main(input, output):
    s3 = S3()
    location = Location(s3)
    occupation = Occupation(s3)
    economic_activity = EconomicActivity(s3)
    
    for csv_path in s3.get_keys(input):
        rais = Rais(s3, csv_path)
        rais.fix_municipality(location)
        rais.add_year()

        rais.df = location.add_columns(rais.df)
        rais.df = occupation.add_columns(rais.df)
        rais.df = economic_activity.add_columns(rais.df)

        rais.save(output)


if __name__ == '__main__':
    main()
