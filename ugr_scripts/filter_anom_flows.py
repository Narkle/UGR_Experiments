#!/usr/bin/python3
"""
Filters anomalous flows, any flow that does not have type: background
Usage
./filter_anom_flows [csv file] [out csv file] 
"""
import argparse
import os

cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]
header = "te,td,sa,da,sp,dp,pr,flg,fwd,stos,pkt,byt,type\n"

parser = argparse.ArgumentParser()
parser.add_argument('f', help='ugr netflow csv', type=str)
# parser.add_argument('o', help='name of filtered csv', type=str)
parser.add_argument("--blacklist", help="do not include blacklist in filtered anom", action='store_true')


def get_outfilename(filename):
    *name_split, ext = filename.split('.') 
    return "%s_%s.%s" % (".".join(name_split), 'anom', ext)

args = parser.parse_args()
path = args.f

filename = os.path.split(path)[-1]
out = get_outfilename(filename)

nLines = 0

filters = ['background\n']

if args.blacklist:
    filters.append('blacklist\n')

with open(path) as csvfile:
    print('[+] opened %s' % path)
    with open(out, 'w+') as outfile:
        outfile.write(header)
        first = True
        for line in csvfile:
            if first:
                first = False
                continue
            flow_type = line.split(',')[-1]
            if flow_type not in filters:
                outfile.write(line)
                nLines += 1

print('[+] Done filtering anomalous flows, wrote %s lines' % nLines)
