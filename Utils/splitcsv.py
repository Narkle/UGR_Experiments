'''
For UGS Data, splits large csv into dates
Note that totalLines is hardcoded. To verify that all lines of csv
file are read. Seems to be the case for 20GB files
'''
import csv
import random
import datetime
from dateutil.parser import parse
counter = 0
header = ["te","td","sa","da","sp","dp","pr","flg","fwd","stos", "pkt", "byt", "type"]

csvHandles = {}
fileHandles = {}
totalLines = 1750775597

with open("csv_week4_anon_cut_labeled.csv") as f:
	reader = csv.reader(f)
	numLines = 0
	for row in reader:
		numLines += 1
		date = row[0].split()[0]
		# collect data on the 21st
		if date == '2016-06-20':
			continue
		if date == '2016-06-21':
			continue
		if date not in csvHandles:
			w = open("{0}.csv".format(date), 'w+')
			fileHandles[date] = w
			csvHandles[date] = csv.writer(w)
			csvHandles[date].writerow(header)

		csvHandles[date].writerow(row)
		if numLines % 1000000 == 0:
			print("[+] {}%".format(100*numLines/totalLines))
