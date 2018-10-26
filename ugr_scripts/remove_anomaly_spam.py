#!/usr/bin/python3
"""
Remove Anomaly Spam Flows
Usage
./remove_anom_spam.py [csv file] [out csv file] 
"""
import argparse
import os

cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]
header = "te,td,sa,da,sp,dp,pr,flg,fwd,stos,pkt,byt,type\n"

parser = argparse.ArgumentParser()
parser.add_argument('f', help='ugr netflow csv', type=str)

def get_outfilename(filename):
    *name_split, ext = filename.split('.') 
    return "%s_%s.%s" % (".".join(name_split), 'remove_anom_spam', ext)

args = parser.parse_args()
path = args.f

filename = os.path.split(path)[-1]
out = get_outfilename(filename)

nLines = 0

with open(path) as csvfile:
    print('[+] opened %s' % path)
    with open(out, 'w+') as outfile:
        outfile.write(header)
        first = True
        for line in csvfile:
            if first:
                first = False
                continue
            flow_type = line.split(',')[-1].strip()
            if flow_type != 'anomaly-spam':
                outfile.write(line)
                nLines += 1

print('[+] Done filtering anomalous flows, wrote %s lines' % nLines)

