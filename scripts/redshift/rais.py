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
            usecols=['PIS', 'IDENTIFICAD', 'MUNICIPIO', 'SEXO', 'IDADE', 'ETNIA', 'ESCOLARIDADE', 'REMUNERACAO', 'CBO2002', 'CNAE20', 'TAM_ESTAB', 'SIMPLES', 'NATUR_JUR'],
            converters={
                'PIS': str,
                'IDENTIFICAD': str,
                'MUNICIPIO': str,
                'CBO2002': str,
                'CNAE20': str
            }
        ).rename(columns={
            'PIS': 'employee',
            'IDENTIFICAD': 'establishment',
            'MUNICIPIO': 'municipality',
            'SEXO': 'gender',
            'IDADE': 'age',
            'ETNIA': 'ethnicity',
            'ESCOLARIDADE': 'literacy',
            'REMUNERACAO': 'wage',
            'CBO2002': 'occupation',
            'CNAE20': 'cnae',
            'TAM_ESTAB': 'establishment_size',
            'SIMPLES': 'simple',
            'NATUR_JUR': 'legal_nature'
        })

    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'mesoregion', 'microregion', 'state', 'municipality', 'occupation', 'occupation_group', 'cnae', 'cnae_division', 'cnae_section', 'establishment', 'employee', 'ethnicity', 'establishment_size', 'gender', 'legal_nature', 'literacy', 'simple', 'age', 'wage']
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
        municipalities = {}

        for _, row in location.municipalities_df.iterrows():
                id_ibge = row['municipality'][:-1]
                municipalities[id_ibge] = add_dv_to_id_ibge(id_ibge)

        self.df['municipality'].replace(municipalities, inplace=True)


def add_dv_to_id_ibge(id_ibge):
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
@click.argument('input', default='redshift/teste/input/rais', type=click.Path())
@click.argument('output', default='redshift/teste/output/rais', type=click.Path())
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
