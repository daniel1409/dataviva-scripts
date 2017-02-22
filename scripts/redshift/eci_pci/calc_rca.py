import sys, os
import pandas as pd
import numpy as np

file_path = os.path.dirname(os.path.realpath(__file__))
ps_calcs_lib_path = os.path.abspath(os.path.join(file_path, "../../../lib/ps_calcs"))
sys.path.insert(0, ps_calcs_lib_path)
import ps_calcs

def calc_rca(comtrade):

    ubiquity_required = 20
    diversity_required = 200
    value_required = 50000000

    '''trim country list by diversity'''
    diversity = comtrade.reset_index()
    diversity = diversity["country"].value_counts()
    diversity = diversity[diversity > diversity_required]

    '''trim country list by export value'''
    value = comtrade.groupby(level=['country']).sum()
    value = value['value']
    value = value[value > value_required]

    countries = set(diversity.index).intersection(set(value.index))

    '''trim product list by ubiquity'''
    ubiquity = comtrade.reset_index()
    ubiquity = ubiquity[ubiquity['value'] > 0]
    ubiquity = ubiquity["product"].value_counts()
    ubiquity = ubiquity[ubiquity > ubiquity_required]

    products = set(ubiquity.index)

    '''re-calculate rcas'''
    countries_to_drop = set(comtrade.index.get_level_values('country')).difference(countries)
    products_to_drop = set(comtrade.index.get_level_values('product')).difference(products)

    comtrade = comtrade.drop(list(countries_to_drop), axis=0, level='country')
    comtrade = comtrade.drop(list(products_to_drop), axis=0, level='product')

    comtrade = comtrade.reset_index()
    comtrade = comtrade.pivot(index="country", columns="product", values="value")
    rca = ps_calcs.rca(comtrade)

    return rca.fillna(0)
