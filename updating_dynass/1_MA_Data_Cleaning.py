'''
input: SDC Platinum M&A data from 2007 -2015
output: cleaned M&A data 
'''
import pandas as pd
import re


#PLC, co, group, '"', holdings

def quoted_csv_name_clean(input):
	assert type(input) == list
	for i in range(len(input)):
		assignee = str(input[i])
		if ',' in assignee:
			input[i] = assignee.split(',')[0]
		if '"' in assignee:
			input[i] = re.sub('"', "", assignee)
			

#remove whitespaces 
def strip_list(input):
	assert type(input) is list
	for i in range(len(input)):
		input[i] = str(input[i])
		input[i] = input[i].strip()

#convert "N.A." to "nan". 
def make_na_to_nan(input):
	assert type(input) is list
	for i in range(len(input)):
		if input[i] == "N.A.":
			input[i] = "nan"

def refine_date(input):
	for i in range(len(input)):
		try:
			input[i] = str(input[i].split('/')[2])
		except:
			print "not a data at index: ", i, " Value: ", input[i]
			input[i] = input[i-1]



MAdata = open("Data/MA_Data_SDC/MA_Data_SDC_07_15.csv")
MAdata = pd.read_csv(MAdata)

year = MAdata['Date Effective/Unconditional'].tolist()
refine_date(year)
acquirer_name = MAdata['Acquiror Name'].tolist()
acquirer_cusip = MAdata['Acquiror CUSIP'].tolist()
acquirer_ticker	= MAdata["Acquiror Primary Ticker Symbol"].tolist()
acquirer_country = MAdata["Acquiror Nation"].tolist()
target_name = MAdata['Target Name'].tolist()
target_cusip = MAdata['Target CUSIP'].tolist()
target_ticker = MAdata['Target Primary Ticker Symbol'].tolist()
target_country = MAdata["Target Nation"].tolist()
deal_attributes = MAdata["Form"].tolist()

strip_list(acquirer_cusip)
strip_list(acquirer_ticker)
make_na_to_nan(acquirer_cusip)
make_na_to_nan(acquirer_ticker)
strip_list(target_cusip)
strip_list(target_ticker)
make_na_to_nan(target_cusip)
make_na_to_nan(target_ticker)


year = pd.Series(year)
acquirer_name = pd.Series(acquirer_name)
acquirer_cusip = pd.Series(acquirer_cusip)
acquirer_ticker = pd.Series(acquirer_ticker)
acquirer_country = pd.Series(acquirer_country)
target_name = pd.Series(target_name)
target_cusip = pd.Series(target_cusip)
target_ticker = pd.Series(target_ticker)
target_country = pd.Series(target_country)
deal_attributes = pd.Series(deal_attributes)
frame = [year, target_name, target_cusip, target_ticker, target_country, acquirer_name, acquirer_cusip, acquirer_ticker, acquirer_country, deal_attributes]

MAdata = pd.concat(frame, axis = 1)
MAdata = MAdata.rename(index=str, columns = {0: 'effective_year', 1:"target_name", 2:"target_cusip", 3:"target_ticker", 4: "target_country", 5: "acquirer_name", 6:"acquirer_cusip", 7:"acquirer_ticker", 8: "acquiere_country", 9: "deal_form"})
MAdata.to_csv("MA_Data_cleaned.csv")



