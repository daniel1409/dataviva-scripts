# -*- coding: utf-8 -*-

import click
import os
import bz2
import pandas as pd

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.argument('output_path', default='output', type=click.Path())
def main(file_path, output_path):
    df = pd.read_csv(file_path, sep=";", header=0)

    year = df['Year'][0]

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    new_file_path = os.path.join(output_path, "rais_{0}.csv.bz2".format(year))
    df.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep=";", index=False, header=False)

if __name__ == "__main__":
    main()	