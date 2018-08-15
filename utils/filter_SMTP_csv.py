'''
For UGS Data, filter SMTP related records
port 25, 587, 465
'''

import pandas as pd
import argparse

parser = argparse.ArgumentParser(description = "Filters SMTP related records from UGR Netflow data")
parser.add_argument('-f', metavar='filename', type=str, help='path to csv file')
parser.add_argument('-o', metavar='filename', type=str, help='path to out csv file')
args = parser.parse_args()

filename = vars(args)['f']
outname = vars(args)['o']
SMTP_ports = [25, 587, 465]

chunksize = 10 ** 6
df = None
counter = 0
for chunk in pd.read_csv(filename, chunksize=chunksize):
	chunk = chunk[(chunk['sp'].isin(SMTP_ports)) | (chunk['dp'].isin(SMTP_ports))]
	if df is None:
		df = chunk
	else:
		df = df.append(chunk) 
	counter += 1
	print("[+] {} chunks read".format(counter))
df.to_csv(outname, index=False)