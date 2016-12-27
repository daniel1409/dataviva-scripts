import click
import time
import pandas as pd
from os import path, makedirs, walk

@click.command()
@click.argument('folder', type=click.Path(exists=True))
@click.argument('secex_type')
@click.argument('output_path', default='output', type=click.Path())
def main(folder, secex_type, output_path):
    for file_path in file_paths(folder):
        print file_path
        generate_csv(file_path, secex_type, output_path)

def file_paths(folder):
    if path.isfile(folder):
        return [folder]

    files = []

    for dirpath,_,filenames in walk(folder):
       for f in filenames:
           files.append(path.abspath(path.join(dirpath, f)))

    files.sort()
    return files


def generate_csv(file_path, secex_type, output_path):
    columns = ['year', 'month', 'product', 'country', 'state', 'port', 'municipality', 'kg', 'value']
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

    df['type'] = secex_type

    if not path.exists(output_path):
        makedirs(output_path)

    new_file_path = path.abspath(path.join(output_path, 'secex_{0}_{1}.csv'.format(secex_type, df['year'][0])))
    df.to_csv(new_file_path, sep=";", header=None, index=False)

if __name__ == "__main__":
    main()