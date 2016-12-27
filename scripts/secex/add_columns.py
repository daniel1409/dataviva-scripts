import click
import bz2
import pandas as pd
from os import path, makedirs, walk

@click.command()
@click.argument('input_path', type=click.Path(exists=True))
@click.argument('output_path', type=click.Path())
def main(input_path, output_path):
    """
    input_path can be a path to a single file or a path to a directory
    input_path must be a path to a directory
    """
    for file_path in file_paths(input_path):
        print file_path
        df = open_secex_df(file_path)
        df = add_columns_microregion_and_mesoregion_to_df(df)
        df = add_columns_section_and_chapter_to_df(df)
        df = add_column_continent_to_df(df)
        save_df(df, file_path, output_path)

def add_column_continent_to_df(secex_df):
    countries_and_continents = get_countries_and_continents()
    continents = []
    countries_without_continents = set()

    unclassified = 'xx'

    for country_id in secex_df['country']:
        continent_id = countries_and_continents.get(country_id, unclassified)
        continents.append(continent_id)

        if continent_id == unclassified:
            countries_without_continents.add(country_id)

    print 'Countries without continents:', list(countries_without_continents)

    secex_df['continent'] = continents

    return secex_df

def get_countries_and_continents():
    continents_df = open_continents_df()
    continents = {}
    for index, row in continents_df.iterrows():
        continents[row['mdic_country_id']] = row['continente_id']

    return continents

def add_columns_microregion_and_mesoregion_to_df(secex_df):
    municipalities_df = open_municipalities_df()
    result_df = pd.merge(secex_df, municipalities_df, left_on='municipality', right_on='municipio_id_mdic', how='left')
    result_df = result_df.drop('municipio_id_mdic', 1)
    result_df = result_df.rename(columns = {'mesorregiao_id': 'mesoregion', 'microrregiao_id': 'microregion'})

    return result_df

def add_columns_section_and_chapter_to_df(secex_df):
    sections_and_chapters = get_sections_and_chapters()

    sections = []
    chapters = []

    products_without_section_and_chapter = set()
    
    unspecified = {
        'section': '99',
        'chapter': '99'
    }
    
    for product_id in secex_df['product']:
        section_and_chapter = sections_and_chapters.get(product_id, unspecified)

        if section_and_chapter == unspecified:
            products_without_section_and_chapter.add(product_id)

        sections.append(section_and_chapter['section'])
        chapters.append(section_and_chapter['chapter'])


    secex_df['section'] = sections
    secex_df['chapter'] = chapters

    print 'Products without sections and chapters:', list(products_without_section_and_chapter)

    return secex_df

def get_sections_and_chapters():
    sections_and_chapters = {}

    hs_df = open_hs_df()

    for product_id in hs_df['id'].tolist():
        if len(product_id) != 6:
            continue

        product = product_id[2:]

        sections_and_chapters[product] = {
            'section': product_id[:2],
            'chapter': product_id[2:4]
        }

    return sections_and_chapters

def open_municipalities_df():
    PATH_TO_CSV = r'C:\dev\projects\dataviva-scripts\data\attrs\attrs_municipios.csv'

    columns = ['uf_id', 'uf_name', 'mesorregiao_id', 'mesorregiao_name', 'microrregiao_id', 'microrregiao_name', 'municipio_id', 'municipio_name', 'municipio_id_mdic']
    df = pd.read_csv(
        PATH_TO_CSV,
        sep=';',
        header=0,
        names=columns,
        usecols=['mesorregiao_id', 'microrregiao_id', 'municipio_id_mdic'],
        converters={
            'mesorregiao_id': str,
            'microrregiao_id': str,
            'municipio_id_mdic': str,
        }
    )

    return df

def open_hs_df():
    PATH_TO_CSV = r'C:\dev\projects\dataviva-scripts\data\attrs\attrs_hs.csv'

    columns = ['id', 'name_pt', 'name_en', 'profundidade_id', 'profundidade']
    df = pd.read_csv(
        PATH_TO_CSV,
        sep=';',
        header=0,
        names=columns,
        usecols=['id'],
        converters={
            'id': str
        }
    )

    return df

def open_continents_df():
    PATH_TO_CSV = r'C:\dev\projects\dataviva-scripts\data\attrs\attrs_continente.csv'

    columns = ['continente_id', 'mdic_country_id', 'continente_name_en', 'continente_name_pt']
    df = pd.read_csv(
        PATH_TO_CSV,
        sep=';',
        header=0,
        names=columns,
        usecols=['continente_id', 'mdic_country_id'],
        converters={
            'continente_id': str,
            'mdic_country_id': lambda x: str(x).zfill(3)
        }
    )

    return df

def open_secex_df(file_path):
    columns = ['year', 'month', 'product', 'country', 'state', 'port', 'municipality', 'kg', 'value', 'type']
    df = pd.read_csv(
        file_path,
        sep=";",
        header=None,
        names=columns,
        converters={
            "product": str,
            'country': str,
            'state': str,
            'port': str,
            'municipality': str,
        },
        engine='python'
    )

    return df

def save_df(df, file_path, output_path):
    if not path.exists(output_path):
        makedirs(output_path)

    file_output_path = path.join(output_path, path.basename(file_path))
    df.to_csv(
        # file_output_path,
        bz2.BZ2File(file_output_path + '.bz2', 'wb'),
        sep="|",
        index=False,
        header=False,
        columns=['type', 'year', 'month', 'continent', 'country', 'state', 'mesoregion', 'microregion', 'municipality', 'port', 'section', 'chapter', 'product', 'kg', 'value']
    )


def file_paths(folder):
    if path.isfile(folder):
        return [folder]

    files = []

    for dirpath,_,filenames in walk(folder):
       for f in filenames:
           files.append(path.abspath(path.join(dirpath, f)))

    files.sort()
    return files


if __name__ == "__main__":
    main()