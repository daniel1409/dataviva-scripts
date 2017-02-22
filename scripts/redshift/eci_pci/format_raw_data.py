# -*- coding: utf-8 -*-
"""
    Format BACI data for DB entry
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Example Usage
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    python scripts/redshift/eci_pci/format_raw_data.py data/comtrade/comtrade_2014.csv -y 2014 -o data/comtrade/

"""

''' Import statements '''
import os, sys, time, click, bz2
import pandas as pd
import numpy as np
from calc_rca import calc_rca

file_path = os.path.dirname(os.path.realpath(__file__))
ps_calcs_lib_path = os.path.abspath(os.path.join(file_path, "../../../lib/ps_calcs"))
sys.path.insert(0, ps_calcs_lib_path)
import ps_calcs

@click.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True, type=int)
@click.option('-o', '--output_dir', help='output directory', type=click.Path(), default="data/comtrade")
def main(input_file, year, output_dir):

    output_dir = os.path.abspath(os.path.join(output_dir, str(year)))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    store = pd.HDFStore(os.path.join(output_dir,'yodp.h5'))

    try:
        comtrade = store.get('comtrade')
    except KeyError:
        df = pd.read_csv(input_file, sep='|', converters={
            "product": str,
            "country": str,
        })
        comtrade = df.groupby(['product', 'country']).sum()
        store.put('comtrade', comtrade)

    rca = calc_rca(comtrade)
    rca[rca >= 1] = 1
    rca[rca < 1] = 0

    eci, pci = ps_calcs.complexity(rca)

    output(eci, 'eci', output_dir)
    output(pci, 'pci', output_dir)

def output(df, name, output_dir):
    file_path = os.path.abspath(os.path.join(output_dir, "%s.tsv.bz2" % name))
    pd.DataFrame(df, columns=[name]).to_csv(bz2.BZ2File(file_path, 'wb'), sep="\t", index=True, float_format="%.3f")

if __name__ == "__main__":
    main()