'''
6-20-16_SMTP is too large, for now focus on 1400 to 1800 window as there are minimal
attacks from  141400 to 1600, and a lot of attacks afterwards
'''
import csv
import random
import datetime
from dateutil.parser import parse
import pandas as pd
counter = 0
header = ["te","td","sa","da","sp","dp","pr","flg","fwd","stos", "pkt", "byt", "type"]

csvHandles = {}
fileHandles = {}
totalLines = 1750775597

startTime = parse("2016-06-20 14:00:00")
endTime = parse("2016-06-20 18:00:00")

df = pd.DataFrame()
chunksize = 10 ** 6
counter = 0

for chunk in pd.read_csv("2016-06-20_SMTP.csv", chunksize=chunksize):
	chunk['te'] = pd.to_datetime(chunk['te'])
	chunk = chunk[(chunk['te'] >= startTime) & (chunk['te'] <= endTime)]
	df = df.append(chunk)

	counter += 1
	print("[+] Done with chunk",counter)

df.to_csv("2016-06-20_SMTP_1400_1800.csv")