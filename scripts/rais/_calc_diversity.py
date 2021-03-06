import sys
import pandas as pd
import numpy as np

def calc_diversity(diversity_tbl, return_tbl, index_col, diversity_col, year, depths):
    
    def get_deepest(column):
        if column == "cnae_id": return depths["cnae"][-1]
        if column == "bra_id": return depths["bra"][-1]
        if column == "cbo_id": return depths["cbo"][-1]
    
    # filter table by deepest length
    diversity_tbl = diversity_tbl.reset_index()
    max_depth = get_deepest(diversity_col)
    deepest_criterion = diversity_tbl[diversity_col].str.len() == max_depth
    diversity_tbl = diversity_tbl[deepest_criterion]
    
    '''
        GET DIVERSITY
    '''
    diversity = diversity_tbl.pivot(index=index_col, columns=diversity_col, values="num_jobs").fillna(0)
    diversity[diversity >= 1] = 1
    diversity[diversity < 1] = 0
    diversity = diversity.sum(axis=1)
    
    '''
        GET EFFECTIVE DIVERSITY
    '''
    entropy = diversity_tbl.pivot(index=index_col, columns=diversity_col, values="num_jobs").fillna(0)
    entropy = entropy.T / entropy.T.sum()
    entropy = entropy * np.log(entropy)
    
    entropy = entropy.sum() * -1
    es = pd.Series([np.e]*len(entropy), index=entropy.index)
    effective_diversity = es ** entropy

    '''
        ADD DIVERSITY TO RETURN TABLE
    '''
    prefix = diversity_col.replace("_id", "")
    for tbl_col in [(diversity, "{0}_diversity".format(prefix)), 
                    (effective_diversity, "{0}_diversity_eff".format(prefix))]:
        tbl, col = tbl_col
        tbl = pd.DataFrame(tbl, columns=[col])
        tbl["year"] = int(year)
        tbl = tbl.reset_index()
        tbl = tbl.set_index(["year", index_col])
        return_tbl[col] = tbl[col]

    return return_tbl
