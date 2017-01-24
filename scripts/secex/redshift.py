import boto3
import click
import pandas as pd
from io import BytesIO
from os import path, environ

class S3():
    def __init__(self):
        self.client = boto3.client(
            's3',
            aws_access_key_id=environ['S3_ACCESS_KEY'],
            aws_secret_access_key=environ['S3_SECRET_KEY']
        )

        self.resource = boto3.resource(
            's3',
            aws_access_key_id=environ['S3_ACCESS_KEY'],
            aws_secret_access_key=environ['S3_SECRET_KEY']
        )

    def read_csv(self, csv_path):
        csv = self.client.get_object(Bucket='dataviva-etl', Key=csv_path)['Body']

        return csv

    def get_keys(self, prefix):
        keys = []

        for obj in self.client.list_objects(Bucket='dataviva-etl', Prefix=prefix)['Contents']:
            if obj['Size'] > 0:
                keys.append(obj['Key'])

        return keys

    def save_df(self, df, prefix):
        csv_buffer = BytesIO()

        df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
        )

        self.resource.Object('dataviva-etl', prefix).put(Body=csv_buffer.getvalue())
        print 'Saved.'


class Location():
    def __init__(self):
        self.municipalities_df = self.open_municipalities_df()
        self.continents_df = self.open_continents_df()

    def open_municipalities_df(self):
        df = pd.read_csv(
            s3.read_csv('attrs/attrs_municipios.csv'),
            sep=';',
            header=0,
            usecols=['mesorregiao_id', 'microrregiao_id', 'municipio_id'],
            converters={
                'mesorregiao_id': str,
                'microrregiao_id': str,
                'municipio_id': str,
            }
        ).rename(columns={
            'mesorregiao_id': 'mesoregion',
            'microrregiao_id': 'microregion',
            'municipio_id': 'municipality'
        })

        return df

    def open_continents_df(self):
        df = pd.read_csv(
            s3.read_csv('attrs/attrs_continente.csv'),
            sep=';',
            header=0,
            usecols=['continente_id', 'mdic_country_id'],
            converters={
                'continente_id': str,
                'mdic_country_id': lambda x: str(x).zfill(3)
            }
        ).rename(columns={
            'continente_id': 'continent',
            'mdic_country_id': 'country'
        })

        return df

    def add_microregion_and_mesoregion(self, df):
        df = pd.merge(
                df, 
                self.municipalities_df,
                on='municipality',
                how='left',
                indicator=True
            )

        missing = set()

        for _, row in df.iterrows():
            if row['_merge'] == 'left_only':
                missing.add(row['municipality'])
        
        if missing:
            print 'Municipalities without microregion and mesoregion: ', list(missing)

        df = df.drop('_merge', 1)

        print '+ microregion and mesoregion'

        return df

    def add_continent(self, df):
        df = pd.merge(
                df, 
                self.continents_df,
                on='country',
                how='left',
                indicator=True
            )

        missing = set()

        for _, row in df.iterrows():
            if row['_merge'] == 'left_only':
                missing.add(row['country'])
        
        if missing:
            print 'Countries without continent: ', list(missing)

        df = df.drop('_merge', 1)

        print '+ continent'

        return df

    def add_columns(self, df):
        df = self.add_microregion_and_mesoregion(df)
        df = self.add_continent(df)
        return df


class Product():
    def __init__(self):
        self.products_df = self.open_products_df()

    def open_products_df(self):
        df = pd.read_csv(
            s3.read_csv('redshift/attrs/attrs_hs.csv'),
            sep=';',
            header=0,
            names=['id', 'name_pt', 'name_en', 'profundidade_id', 'profundidade'],
            usecols=['id'],
            converters={
                'id': str
            }
        )

        product = []
        product_section = []
        product_chapter = []

        for id in df['id'].tolist():
            if len(id) == 6:
                product.append(id[2:])
                product_section.append(id[:2])
                product_chapter.append(id[2:4])

        df = pd.DataFrame({
            'product': pd.Series(product),
            'product_section': pd.Series(product_section),
            'product_chapter': pd.Series(product_chapter)
        })

        return df

    def add_section_and_chapter(self, df):
        df = pd.merge(
                df, 
                self.products_df,
                on='product',
                how='left',
                indicator=True
            )

        missing = set()

        for _, row in df.iterrows():
            if row['_merge'] == 'left_only':
                missing.add(row['product'])
        
        if missing:
            print 'Products without section and chapter: ', list(missing)

        df = df.drop('_merge', 1)

        print '+ section and chapter'

        return df

    def add_columns(self, df):
        df = self.add_section_and_chapter(df)
        return df


class Secex():
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.filename = path.basename(csv_path)
        print self.filename

        self.open_df()

    def open_df(self):
        self.df = pd.read_csv(
            s3.read_csv(self.csv_path),
            sep=';',
            header=0,
            usecols=['CO_ANO', 'CO_MES', 'CO_PAIS', 'CO_PORTO', 'KG_LIQUIDO', 'VL_FOB', 'HS_07', 'UF_IBGE', 'MUN_IBGE'],
            converters={
                'CO_PAIS': str,
                'CO_PORTO': str,
                'HS_07': str,
                'UF_IBGE': str,
                'MUN_IBGE': str,
            }
        ).rename(columns={
            'CO_ANO': 'year',
            'CO_MES': 'month',
            'CO_PAIS': 'country',
            'CO_PORTO': 'port',
            'KG_LIQUIDO': 'kg',
            'VL_FOB': 'value',
            'HS_07': 'product',
            'UF_IBGE': 'state',
            'MUN_IBGE': 'municipality'
        })


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
    for csv_path in s3.get_keys(input):
        secex = Secex(csv_path)

        secex.add_type()
        
        location = Location()
        secex.df = location.add_columns(secex.df)

        product = Product()
        secex.df = product.add_columns(secex.df)
        
        s3.save_df(secex.df, path.join(output, secex.filename))
        print


s3 = S3()

if __name__ == '__main__':
    main()
