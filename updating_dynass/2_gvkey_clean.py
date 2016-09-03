'''
input: compustat gvkey data from WRDS
need to make "begin year" and "end year" using fiscal years
output: cleaned gvkey data
'''
import pandas as pd
from collections import defaultdict


def make_int(input):
	assert type(input) == list
	for i in range(len(input)):
		try:
			input[i] = int(input[i])
		except:
			pass

data = open('Data/GVKEY_Data/raw_gvkey_data_2.csv')
gv_data = pd.read_csv(data)
gv_data.rename(columns = {'conm':'name'}, inplace = True)
gvkeys = []
years = []
tics = []
cusips = []
names = []
begyrs = []
endyrs = []

for row in gv_data.iterrows():
	gvkey = row[1]["gvkey"]
	fyear = row[1]["fyear"]
	tic = row[1]["tic"]
	cusip = row[1]['cusip']
	name = str(row[1]['name'])
	if gvkey not in gvkeys:
		gvkeys.append(gvkey)
		idx = len(gvkeys)-1
		years.append([])
		years[idx].append(fyear)
		cusips.append(cusip)
		tics.append(tic)
		names.append(name)
	else:
		idx = len(gvkeys)-1
		years[idx].append(fyear)

for i in range(len(gvkeys)):
	years[i] = [value for value in years[i] if pd.notnull(value)]
	if years[i]:
		begyrs.append(min(years[i]))
		endyrs.append(max(years[i]))
	else:
		begyrs.append(np.NAN)
		endyrs.append(np.NAN)
#remove_meaningless(names)

make_int(begyrs)
make_int(endyrs)
gvkeys = pd.Series(gvkeys)
names = pd.Series(names)
cusips = pd.Series(cusips)
tics = pd.Series(tics)
begyrs = pd.Series(begyrs, dtype = str)
endyrs = pd.Series(endyrs, dtype = str)
frame = [gvkeys, names, cusips, tics, begyrs, endyrs]
gvkey_data = pd.concat(frame, axis = 1)
gvkey_data = gvkey_data.rename(index=str, columns = {0: 'gvkey', 1: 'name', 2: 'cusip', 3:'tics', 4:'begyr', 5:'endyr'})
gvkey_data.to_csv("gvkey_data_cleaned.csv")

