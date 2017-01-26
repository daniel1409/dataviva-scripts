import pandas as pd


class Occupation():
    def __init__(self, s3):
        self.s3 = s3
        self.occupations_df = self.open_occupations_df()

    def open_occupations_df(self):
        df = pd.read_csv(
            self.s3.read_csv('attrs/attrs_cbo.csv'),
            sep=';',
            header=0,
            names=['id', 'name_en', 'name_pt'],
            usecols=['id'],
            converters={
                'id': str
            }
        )

        occupation = []
        occupation_group = []

        for id in df['id'].tolist():
            if len(id) == 4:
                occupation.append(id)
                occupation_group.append(id[0])

        df = pd.DataFrame({
            'occupation': pd.Series(occupation),
            'occupation_group': pd.Series(occupation_group)
        })

        return df

    def add_group(self, df):
        df = pd.merge(
                df, 
                self.occupations_df,
                on='occupation',
                how='left',
                indicator=True
            )

        missing = set()

        for _, row in df.iterrows():
            if row['_merge'] == 'left_only':
                missing.add(row['occupation'])
        
        if missing:
            print 'Occupations without group: ', list(missing)

        df = df.drop('_merge', 1)

        print '+ occupation group'

        return df

    def add_columns(self, df):
        df = self.add_group(df)
        return df

