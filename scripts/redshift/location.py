import pandas as pd


class Location():
    def __init__(self, s3):
        self.s3 = s3
        self.municipalities_df = self.open_municipalities_df()
        self.continents_df = self.open_continents_df()

    def open_municipalities_df(self):
        df = pd.read_csv(
            self.s3.read_csv('attrs/attrs_municipios.csv'),
            sep=';',
            header=0,
            usecols=['uf_id', 'mesorregiao_id', 'microrregiao_id', 'municipio_id'],
            converters={
                'uf_id': str,
                'mesorregiao_id': str,
                'microrregiao_id': str,
                'municipio_id': str,
            }
        ).rename(columns={
            'uf_id': 'state',
            'mesorregiao_id': 'mesoregion',
            'microrregiao_id': 'microregion',
            'municipio_id': 'municipality'
        })

        regions = []

        for state in df['state']:
            regions.append(state[0])

        df['region'] = regions

        return df

    def open_continents_df(self):
        df = pd.read_csv(
            self.s3.read_csv('attrs/attrs_continente.csv'),
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

    def add_region_mesoregion_microregion_and_state(self, df):
        df = pd.merge(
                df, 
                self.municipalities_df,
                on='municipality',
                how='left'
            )

        print '+ region, mesoregion, microregion and state'

        return df

    def add_continent(self, df):
        df = pd.merge(
                df, 
                self.continents_df,
                on='country',
                how='left'
            )

        print '+ continent'

        return df

    def add_columns(self, df, continent=False):
        df = self.add_region_mesoregion_microregion_and_state(df)

        if continent:
            df = self.add_continent(df)

        return df
