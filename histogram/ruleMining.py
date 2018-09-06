"""
documentation for SAM: http://www.borgelt.net/sam.html
"""

import os
import subprocess

ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) # This is your Project Root

class RuleMining:

    def __init__(self, minSup):
        """
        Arguments:
            minSup {number} -- minimum number of flows
        """
        self.minSup = minSup


    def translatetoSAMinput(self, df, features = ['sa', 'da', 'sp', 'dp']):
        lines = []
        for _, row in df.iterrows():
            line = " ".join(["{0}:{1}".format(f, row[f]) for f in features])
            lines.append(line)
        return "\n".join(lines)


    def invokeMaximalSAM(self, inputStr, minSup=1):
        inFileName = 'temp_binning.in'
        outFileName = 'temp_binning.out'
        
        inFile = open(inFileName, 'w+')
        inFile.write(inputStr)
        inFile.close()
        
        subprocess.call([ROOT_DIR + '/sam', '-m', '-m3', '-S' + str(minSup), inFileName, outFileName])
        lines = open(outFileName).readlines()
        
        result = {}
        for line in lines:
            *itemset, supp = line.split()
            supp = float(supp[1:-1])
            result[tuple(sorted(itemset))] = supp
        
        os.remove(inFileName)
        os.remove(outFileName)
        
        return result


    def FIM(self, df):
        samInput = self.translatetoSAMinput(df)
        samOutput = self.invokeMaximalSAM(samInput, self.minSup)
        return samOutput
