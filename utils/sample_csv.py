'''
For UGS Data, samples from csv
'''
import csv
import random
import argparse

parser = argparse.ArgumentParser(description = "Samples UGR Netflow data")
parser.add_argument('-f', metavar='filename', type=str, help='path to csv file')
parser.add_argument('-o', metavar='filename', type=str, help='path to out csv file')
parser.add_argument('-r', metavar='int', type=int, help='sample rate (1 out of XXX)')
args = vars(parser.parse_args())

rate = args['r']
filename = args['f']
outfilename = args['o'] 

counter = 0
header = ["te","td","sa","da","sp","dp","pr","flg","fwd","stos", "pkt", "byt", "type"]
nRowsWriten = 0 

print("[+] Opening", filename)

with open(filename) as f:
	with open(outfilename, 'w+') as out:
		reader = csv.reader(f)
		writer = csv.writer(out)
		writer.writerow(header)
		for row in reader:
			if random.randint(1,rate) == 1:
				writer.writerow(row)
				nRowsWriten += 1

print("[+] Done writing to", outfilename,nRowsWriten,"rows writted")
