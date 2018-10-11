#!/usr/bin/python3
"""
samples netflow records
Usage
./sample_csv [csv file] --R [1 out of rate] --D

Note: skip the first line since for some reason it is a trash line
"""
import argparse
import os
import random

cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]
header = "te,td,sa,da,sp,dp,pr,flg,fwd,stos,pkt,byt,type\n"
parser = argparse.ArgumentParser()
parser.add_argument('f', help='ugr netflow csv', type=str)
parser.add_argument('--D', help='destination folder', type=str)
parser.add_argument('--R', help='sampling rate (1 out of N)', type=int)
parser.add_argument('--S', help='number of lines to skip', type=int)

args = parser.parse_args()
path = args.f
filename = os.path.split(path)[-1]
outdir = args.D if args.D else './'
rate = args.R if args.R else 1000
nSkip = args.S if args.S else 1

nLines = 0

def get_outfilename(filename, rate):
    *name_split, ext = filename.split('.') 
    return "%s_%s.%s" % (".".join(name_split), rate, ext)


with open(path) as csvfile:
    print("[+] opened %s" % path)
    outfilepath = os.path.join(outdir, get_outfilename(filename, rate))
    with open(outfilepath, 'w+') as outfile:
        outfile.write(header)
        for idx, line in enumerate(csvfile):
            if idx < nSkip:
                continue
            if rate == 1 or random.randint(1, rate) == 1:
                outfile.write(line)
                nLines += 1

print('[+] Done sampling, wrote %s lines' % nLines)