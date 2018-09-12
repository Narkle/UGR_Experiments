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
parser.add_argument('o', help='name of filtered csv', type=str)

args = parser.parse_args()
path = args.f
out = args.o

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
            flow_type = line.split(',')[-1]
            if flow_type != 'background\n':
                outfile.write(line)
                nLines += 1

print('[+] Done filtering anomalous flows, wrote %s lines' % nLines)
