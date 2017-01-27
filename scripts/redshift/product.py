import pandas as pd


class Product():
    def __init__(self, s3):
        self.s3 = s3
        self.products_df = self.open_products_df()

    def open_products_df(self):
        df = pd.read_csv(
            self.s3.read_csv('redshift/attrs/attrs_hs.csv'),
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
                how='left'
            )

        print '+ section and chapter'

        return df

    def add_columns(self, df):
        df = self.add_section_and_chapter(df)
        return df

