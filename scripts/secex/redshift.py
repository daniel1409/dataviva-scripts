import boto3
import pandas as pd
from io import BytesIO
from os import path, environ

def read_csv_from_s3(csv_path):
        client = boto3.client(
            's3',
            aws_access_key_id=environ['S3_ACCESS_KEY'],
            aws_secret_access_key=environ['S3_SECRET_KEY']
        )

        keys = [obj['Key'] for obj in client.list_objects(Bucket='dataviva-etl', Prefix=csv_path)['Contents']]

        obj = client.get_object(Bucket='dataviva-etl', Key=csv_path)

        return obj['Body']


def get_keys_from_s3(prefix):
    client = boto3.client(
        's3',
        aws_access_key_id=environ['S3_ACCESS_KEY'],
        aws_secret_access_key=environ['S3_SECRET_KEY']
    )

    keys = []

    for obj in client.list_objects(Bucket='dataviva-etl', Prefix=prefix)['Contents']:
        if obj['Size'] > 0:
            keys.append(obj['Key'])

    return keys


class Location():

    def __init__(self):
        self.municipalities_df = self.open_municipalities_df()


    def open_municipalities_df(self):
        columns = ['uf_id', 'uf_name', 'mesoregion', 'mesorregiao_name', 'microregion', 'microrregiao_name', 'municipality', 'municipio_name', 'municipio_id_mdic']
        df = pd.read_csv(
            read_csv_from_s3('attrs/attrs_municipios.csv'),
            sep=';',
            names=columns,
            header=0,
            usecols=['mesoregion', 'microregion', 'municipality'],
            converters={
                'mesoregion': str,
                'microregion': str,
                'municipality': str,
            }
        )

        return df


    def add_columns_microregion_and_mesoregion_to_df(self, df):
        import pdb; pdb.set_trace()
        df = pd.merge(
                df, 
                self.municipalities_df,
                on='municipality',
                how='left'
            )
        import pdb; pdb.set_trace()
        return df



    def add_columns(self, df):
        df = self.add_columns_microregion_and_mesoregion_to_df(df)
        return df


class Secex():
    
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.filename = path.basename(csv_path)
        self.open_df()

    def open_df(self):

        self.df = pd.read_csv(
            read_csv_from_s3(self.csv_path),
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

    def save_df(self):
        csv_buffer = BytesIO()
        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
        )
        
        s3 = boto3.resource(
            's3',
            aws_access_key_id=environ['S3_ACCESS_KEY'],
            aws_secret_access_key=environ['S3_SECRET_KEY']
        )

        s3.Object('dataviva-etl', 'redshift/teste/output/secex/' + self.filename).put(Body=csv_buffer.getvalue())



    def add_type(self):
        if 'import' in self.csv_path:
            self.df['type'] = 'import'
        else:
            self.df['type'] = 'export'



def main():

    for csv_path in get_keys_from_s3('redshift/teste/input/secex'):
        secex = Secex(csv_path)
        print secex.filename

        secex.add_type()
        print 'Added type.'
        
        location = Location()
        secex.df = location.add_columns(secex.df)
        print 'Added location.'
        
        secex.save_df()
        print 'Saved.'

        print



if __name__ == '__main__':
    main()