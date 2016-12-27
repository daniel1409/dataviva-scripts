import click
import pandas as pd
from os import path, makedirs, walk

@click.command()
@click.argument('input_file_path', type=click.Path(exists=True))
@click.argument('output_path', type=click.Path())

def main(input_file_path, output_path):
    for year in [ "2011", "2012", "2013"]:
        file_path = input_file_path.replace("2008", year)
        filename = path.basename(file_path)
        print filename

        file_output_path = path.join(output_path, filename)

        change_separator(file_path, file_output_path)


def change_separator(file_path, file_output_path):
    columns = ['year', 'month', 'product', 'country', 'state', 'port', 'municipality', 'kg', 'value']
    df = pd.read_csv(
        file_path,
        sep=',',
        header=0,
        names=columns,
        converters={
            'product': str,
            'country': str,
            'state': str,
            'port': str,
            'municipality': str
        },
        engine='python'
    )

    df.to_csv(file_output_path, sep=';', header=None, index=False)


if __name__ == "__main__":
    main()