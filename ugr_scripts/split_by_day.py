#!/usr/bin/python3
"""
splits ugr netflow csv dump by day
Usage
./split_by_day [csv file]

Notes:

te	                td	    sa	            da	            sp	    dp	    pr	flg	    fwd	stos	pkt	byt	    type
2016-06-20 00:07:11	3.324	42.219.145.241	79.28.21.23	    25	    60052	TCP	.AP.SF	0	0	    29	2172	background
2016-06-20 00:07:14	1.96	42.219.156.185	108.66.255.250	54726	25	    TCP	.APRS.	0	0	    10	827	    background

"""
import argparse
import os

cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]
colIdx = {c:i for i, c in enumerate(cols)}
header = "te,td,sa,da,sp,dp,pr,flg,fwd,stos,pkt,byt,type\n"
parser = argparse.ArgumentParser()
parser.add_argument('f', help='ugr netflow csv', type=str)
parser.add_argument('--D', help='destination folder', type=str)

args = parser.parse_args()
path = args.f
filename = os.path.split(path)[-1]
outdir = args.D if args.D else './'

print("outdir", outdir)

def get_outfilename(year, month, day):
    return "%s-%s-%s.csv" % (year, month, day)

curr_year, curr_month, curr_day = None, None, None
outfile = None
outfilename = None

firstLine = True

with open(path) as csvfile:
    for line in csvfile:
        if firstLine:
            firstLine = False
            continue
        row = line.split(',')
        datetime = row[colIdx['te']]
        year, month, day = datetime.split()[0].split('-')
        if curr_month != month or curr_day != day:
            curr_year, curr_month, curr_day = year, month, day
            if outfile is not None:
                outfile.close()
                print('[+] closed file: %s' % outfilename)
            outfilename = get_outfilename(curr_year, curr_month, curr_day)
            print('[+] creating file: %s' % outfilename)
            outfile = open(os.path.join(outdir, outfilename), 'w+')
            outfile.write(header)
        else:
            outfile.write(line)
            

if outfile is not None:
    outfile.close() 
    print('[+] closed file: %s' % outfilename)
print('[+] Done')