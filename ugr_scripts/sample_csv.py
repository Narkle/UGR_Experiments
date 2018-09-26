#!/usr/bin/python3
"""
samples netflow records
Usage
./sample_csv [csv file] --R [1 out of rate] --D
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

args = parser.parse_args()
path = args.f
filename = os.path.split(path)[-1]
outdir = args.D if args.D else './'
rate = args.R if args.R else 1000
nLines = 0

def get_outfilename(filename, rate):
    *name_split, ext = filename.split('.') 
    return "%s_%s.%s" % (".".join(name_split), rate, ext)

with open(path) as csvfile:
    print("[+] opened %s" % path)
    outfilepath = os.path.join(outdir, get_outfilename(filename, rate))
    with open(outfilepath, 'w+') as outfile:
        outfile.write(header)
        first = True
        for line in csvfile:
            if first:
                first = False
                continue
            if random.randint(1, rate) == 1:
                outfile.write(line)
                nLines += 1

print('[+] Done sampling, wrote %s lines' % nLines)