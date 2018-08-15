'''
For UGS Data, filter SMTP related records
port 25, 587, 465
'''
import csv
import argparse

parser = argparse.ArgumentParser(description = "Filters SMTP related records from UGR Netflow data")
parser.add_argument('-f', metavar='filename', type=str, help='path to csv file')
parser.add_argument('-o', metavar='filename', type=str, help='path to out csv file')
args = parser.parse_args()

filename = vars(args)['f']
outname = vars(args)['o']
SMTP_ports = [25, 587, 465]

header = ["te","td","sa","da","sp","dp","pr","flg","fwd","stos", "pkt", "byt", "type"]
spIdx = 4
dpIdx = 5


with open(filename) as f:
	with open(outname, 'w+') as out:
		reader = csv.reader(f)
		writer = csv.writer(out)
		writer.writerow(header)
		for row in reader:
			print(row)
			tokens = row.split()
			sp = tokens[spIdx]
			dp = tokens[dpIdx]
			if sp in SMTP_ports or dp in SMTP_ports:
				writer.writerow(row)
				nRowsWriten += 1
print("[+] Done writing to", outname,nRowsWriten,"rows writted")
