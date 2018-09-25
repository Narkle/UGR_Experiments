"""
PyFIM (apriori)
"""

from fim import apriori

_features = ['sa', 'da', 'sp', 'dp', 'flg', 'type']

class RuleMining:
    """Extracts association rules from filtered flows, wrapper class to pyFIM
    """

    def __init__(self):
        """
        Arguments:
            minSup {number} -- min support (percentage (%) or count)
        Keyword Arguments:
            isCount {bool} -- True if absolute number, percentage if False (default: {False})
        """


    def netflow_to_transc(self, df, features=_features):
        return [["{0},{1}".format(f, row[f]) for f in features] for _, row in df.iterrows()]


    def mine(self, df, supp, zmin, zmax, is_count=False):
        """return association rules from netflow df
        
        Arguments:
            df {dataframe} -- netflow dataframe
            supp {number} -- [description]
            zmin {number} -- minimum number in itemset
            zmax {number} -- max number in itemset
        
        Keyword Arguments:
            is_count {bool} -- true if minsup is absolute number, else percentage (default: {False})
        """
        transacts = self.netflow_to_transc(df)
        supp = -supp if is_count else supp # (positive: percentage, negative: absolute number)
        return apriori(transacts, target='m', supp=supp, zmin=zmin, zmax=zmax)
