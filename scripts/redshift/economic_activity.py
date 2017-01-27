import pandas as pd


class EconomicActivity():
    def __init__(self, s3):
        self.s3 = s3
        self.economic_activities_df = self.open_economic_activities_df()

    def open_economic_activities_df(self):
        df = pd.read_csv(
            self.s3.read_csv('attrs/attrs_cnae.csv'),
            sep=',',
            header=0,
            names=['id', 'name_en', 'name_pt'],
            usecols=['id'],
            converters={
                'id': str
            }
        )

        cnae = []
        cnae_division = []
        cnae_section = []

        for id in df['id'].tolist():
            if len(id) == 6:
                cnae.append(id[1:])
                cnae_division.append(id[1:3])
                cnae_section.append(id[0])

        df = pd.DataFrame({
            'cnae': pd.Series(cnae),
            'cnae_division': pd.Series(cnae_division),
            'cnae_section': pd.Series(cnae_section)
        })

        return df

    def add_division_and_section(self, df):
        df = pd.merge(
                df, 
                self.economic_activities_df,
                on='cnae',
                how='left'
            )

        print '+ class, division and section'

        return df

    def add_columns(self, df):
        df = self.add_division_and_section(df)
        return df
