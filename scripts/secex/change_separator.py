import click
import pandas as pd
from os import path, makedirs, walk

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.argument('current_separator', default=',')
@click.argument('new_separator', default=';', type=click.Path())


def main(file_path, current_separator, new_separator):
    for year in ["2008", "2009", "2011", "2012", "2013"]:
        print file_path.replace("2008", year)
        change_separator(file_path.replace("2008", year), current_separator, new_separator)


    # change_separator(file_path, current_separator, new_separator)
        




def change_separator(file_path, current_separator, new_separator):
    columns = ['year', 'month', 'product', 'country', 'state', 'port', 'municipality', 'value', 'kg']
    df = pd.read_csv(
        file_path,
        sep=str(current_separator),
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

    df.to_csv(file_path, sep=new_separator, header=None, index=False)


if __name__ == "__main__":
    main()