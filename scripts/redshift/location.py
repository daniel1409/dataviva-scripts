import pandas as pd


class Location():
    def __init__(self, s3):
        self.s3 = s3
        self.municipalities_df = self.open_municipalities_df()
        self.continents_df = self.open_continents_df()
        self.wld_df = self.open_wld_df()

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

    def open_wld_df(self):
        df = pd.read_csv(
            self.s3.read_csv('attrs/attrs_wld.csv'),
            sep=';',
            header=0,
            usecols=['id_num', 'id_mdic'],
            converters={
                'id_num': lambda x: int(x) if x.isdigit() else '',
                'id_mdic': lambda x: str(x).zfill(3) if x.isdigit() else ''
            }
        ).rename(columns={
            'id_mdic': 'country'
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

    def fix_municipality(self, df):
        municipalities_6 = []
        municipalities_7 = []

        for _, row in self.municipalities_df.iterrows():
                id_ibge = row['municipality']
                municipalities_6.append(id_ibge[:-1])
                municipalities_7.append(id_ibge)

        municipalities_df = pd.DataFrame({
            'municipality': pd.Series(municipalities_6),
            'municipality7': pd.Series(municipalities_7)
        })

        df = pd.merge(
            df, 
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
        return df


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

