'''
This will map M&A acquirees and acqurers to corresponding GVKEY using CUSIP and Ticker.
input: handpicked string match result file, M&A data, GVKEY data
output: M&A data with corresponding GVKEYS

'''

import pandas as pd
from collections import defaultdict
from collections import Counter
import collections
import sys
import numpy as np

reload(sys)
sys.setdefaultencoding('utf8')

file1 = open("Data/MA_Data_SDC/MA_Data_cleaned.csv")
file2 = open("Data/GVKEY_Data/gvkey_data_cleaned.csv")
MA_data = pd.read_csv(file1, index_col = 0)
gvkey_data = pd.read_csv(file2, dtype = str, index_col = 0)

# remove everything except MERGERS and ACQUISITIONS 
deal_form = MA_data['deal_form'].tolist()
for index in range(len(deal_form)):
	if deal_form[index] != "Merger" and deal_form[index] != "Acquisition":
		deal_form[index] = None

MA_data['deal_form'] = deal_form

MA_data = MA_data[pd.notnull(MA_data['deal_form'])]
new_index = range(len(MA_data))
MA_data['index'] = new_index
MA_data = MA_data.set_index('index')

'''reference data'''
gvkey_ref = gvkey_data["gvkey"].tolist()
name_ref = gvkey_data["name"].tolist()

cusip_ref_9digit = gvkey_data["cusip"].tolist()
cusip_ref = []
while cusip_ref_9digit:
	if pd.notnull(cusip_ref_9digit[0]) and len(cusip_ref_9digit[0]) > 6:
		cusip_ref_9digit[0] = cusip_ref_9digit[0][:6]
		cusip_ref.append(cusip_ref_9digit.pop(0))
	else:
		cusip_ref.append(cusip_ref_9digit.pop(0))

tics_ref = gvkey_data["tics"].tolist()
begyrs = gvkey_data["begyr"].tolist()
endyrs = gvkey_data["endyr"].tolist()

complete_year = MA_data['effective_year'].tolist()
gvkey_target = []
name_target = MA_data["target_name"].tolist()
cusip_target = MA_data["target_cusip"].tolist()
tics_target = MA_data["target_ticker"].tolist()
begyrs_target = [] 
endyrs_target = []

gvkey_acquirer = []
name_acquirer = MA_data["acquirer_name"].tolist()
cusip_acquirer = MA_data["acquirer_cusip"].tolist()
tics_acquirer = MA_data["acquirer_ticker"].tolist()
begyrs_acquirer = []
endyrs_acquirer = []


''' compare cusip'''
for i in range(len(cusip_acquirer)):
	if not pd.isnull(cusip_acquirer[i]) and cusip_acquirer[i] in cusip_ref:
		idx_ref = cusip_ref.index(cusip_acquirer[i])
		gvkey_acquirer.append(gvkey_ref[idx_ref])
		begyrs_acquirer.append(begyrs[idx_ref])
		endyrs_acquirer.append(endyrs[idx_ref])
	else:
		gvkey_acquirer.append(("nan"))
		begyrs_acquirer.append(("nan"))
		endyrs_acquirer.append(("nan"))

for i in range(len(cusip_target)):
	if not pd.isnull(cusip_target[i]) and cusip_target[i] in cusip_ref:
		idx_ref = cusip_ref.index(cusip_target[i])
		gvkey_target.append(gvkey_ref[idx_ref])
		begyrs_target.append(begyrs[idx_ref])
		endyrs_target.append(endyrs[idx_ref])
	else:
		gvkey_target.append(("nan"))
		begyrs_target.append(("nan"))
		endyrs_target.append(("nan"))

'''compare ticker'''
for i in range(len(tics_acquirer)):
	if gvkey_acquirer == "nan" and tics_acquirer[i] in tics_ref:
		idx_ref = tics_ref.index(tics_acquirer[i])
		gvkey_acquirer.append(gvkey_ref[idx_ref])
		begyrs_acquirer.append(begyrs[idx_ref])
		endyrs_acquirer.append(endyrs[idx_ref])
	else:
		pass

for i in range(len(tics_target)):
	if gvkey_target == "nan" and tics_target[i] in tics_ref:
		idx_ref = tics_ref.index(tics_target[i])
		gvkey_target.append(gvkey_ref[idx_ref])
		begyrs_target.append(begyrs[idx_ref])
		endyrs_target.append(endyrs[idx_ref])
	else:
		pass



'''						matching result: 							'''
def how_many_gvkey_matched(gvkey_list):
	count = 0
	total = len(gvkey_list)
	for gvkey in gvkey_list:
		if gvkey == 'nan':
			count += 1
	print "Found: {0} \n Not Found: {1} \n Total: {2}".format(total-count, count, total)

how_many_gvkey_matched(gvkey_target)
how_many_gvkey_matched(gvkey_acquirer)

matching_result_dict = defaultdict(list)
for i, name in enumerate(name_target):
	matching_result_dict[name].append(gvkey_target[i])

for i, name in enumerate(name_acquirer):
	matching_result_dict[name].append(gvkey_acquirer[i])

matching_result_dict = {key : set(value) for key, value in matching_result_dict.items()}

for key, value in matching_result_dict.items():
	if len(value) > 1:
		print key, value

nans = 0
for key, value in matching_result_dict.items():
	if 'nan' in value:
		nans += 1

print "Matched {0} companies out of {1}".format(len(matching_result_dict) - nans, len(matching_result_dict))

'''compare names'''


#compare string 1

for i in range(len(gvkey_acquirer)):
	if gvkey_acquirer[i] == "nan" and name_acquirer[i] in name_ref:
		idx = name_ref.index(name_acquirer[i])
		name_acquirer[i] = name_ref[idx]
		gvkey_acquirer[i] = gvkey_ref[idx]
		begyrs_acquirer[i] = begyrs[idx]
		endyrs_acquirer[i] = endyrs[idx]

for i in range(len(gvkey_target)):
	if gvkey_target[i] == "nan" and name_target[i] in name_ref:
		idx = name_ref.index(name_target[i])
		name_target[i] = name_ref[idx]
		gvkey_target[i] = gvkey_ref[idx]
		begyrs_target[i] = begyrs[idx]
		endyrs_target[i] = endyrs[idx]


'''assemble vectors '''
gvkey_target = pd.Series(gvkey_target)
name_target = pd.Series(name_target)
begyrs_target = pd.Series(begyrs_target)
endyrs_target = pd.Series(endyrs_target)

gvkey_acquirer = pd.Series(gvkey_acquirer)
name_acquirer = pd.Series(name_acquirer)
begyrs_acquirer = pd.Series(begyrs_acquirer)
endyrs_acquirer = pd.Series(endyrs_acquirer)


###part 2: use names do not have gvkey in updated dynass file and the names in the Compustat data. use Jaro-Winkler-Yongjip distance to match strings.  
# First, I need to clean names a little bit. 
# for comparision, we need cleaned names, raw names.
# Outcome file = index in the M&A data, raw_name, scores_i, names_i, gvkey_i
# for the matching, we should use the cleaned names, but need to keep the original name to merge data.
# Ultimately, we need name
# Once we found gvkey, we need to update begyr and endyr usint gvkeys

#compare string 2

def make_upper(input):
	for i in range(len(input)):
		input[i] = input[i].upper()

unmatched_names_MA = []
for i in range(len(name_target)):
	if gvkey_target[i] == 'nan':
		unmatched_names_MA.append(name_target[i])

for i in range(len(name_acquirer)):
	if gvkey_acquirer[i] == 'nan':
		unmatched_names_MA.append(name_acquirer[i])


raw_names = pd.unique(unmatched_names_MA)
raw_names = raw_names.tolist()
cleaned_names = list(raw_names)

count = 0
for name in raw_names:
	if name is 'nan':
		count += 1
		print 'nan'
	elif pd.isnull(name):
		count += 1
		print 'null'

if count == 0:
	print "There is no Null or nan names"
#No NAN or NULL name

#Make reference data

matched_gvkeys = pd.unique(gvkey_target.tolist()+gvkey_acquirer.tolist())
matched_gvkeys = matched_gvkeys.tolist()
matched_gvkeys.remove('nan')

matched_gvkey_dict = {key : 0 for key in matched_gvkeys}

unmatched_compustat_names = []
unmatched_compustat_gvkeys = []
begyear = []
endyear = []

for i in range(len(gvkey_ref)):
	if gvkey_ref[i] not in matched_gvkey_dict:				#check if gvkey in matched gvkey dictionary
		unmatched_compustat_names.append(name_ref[i])
		unmatched_compustat_gvkeys.append(gvkey_ref[i])
		begyear.append(begyrs[i])
		endyear.append(endyrs[i])

#Reference data are prepared

#Our varialbles: raw_names, cleaned_names, unmatched_compustat_names

make_upper(cleaned_names)
make_upper(unmatched_compustat_names)
#count_name_elements(cleaned_names, ' ')

#Now step 2: make a score dictionary

result_file = open("MA_string_matching_results/MAdata_string_match_handpicked2.csv")

result = pd.read_csv(result_file, dtype = str)
names = result.name.tolist()
gvkey1 = result.id1.tolist()
gvkey2 = result.id2.tolist()
gvkey3 = result.id3.tolist()


gvkeys = [gvkey3, gvkey2, gvkey1]


result_dict = defaultdict(list)

for gvkey in gvkeys:
	for index in range(len(gvkey)):
		if pd.notnull(gvkey[index]):
			result_dict[names[index]] = [gvkey[index]]

for name, item in result_dict.items():
	index = gvkey_ref.index(item[0])
	result_dict[name].append(begyrs[index])
	result_dict[name].append(endyrs[index])


#compare string 1
make_upper(name_acquirer)
make_upper(name_target)


for i in range(len(gvkey_acquirer)):
	if gvkey_acquirer[i] == "nan" and name_acquirer[i] in result_dict:
		gvkey_acquirer[i] = result_dict[name_acquirer[i]][0]
		begyrs_acquirer[i] = result_dict[name_acquirer[i]][1]
		endyrs_acquirer[i] = result_dict[name_acquirer[i]][2]

for i in range(len(gvkey_target)):
	if gvkey_target[i] == "nan" and name_target[i] in result_dict:
		gvkey_target[i] = result_dict[name_target[i]][0]
		begyrs_target[i] = result_dict[name_target[i]][1]
		endyrs_target[i] = result_dict[name_target[i]][2]

for key in result_dict:
	if key not in name_target and key not in name_acquirer:
		pass #print 'no matching'



how_many_gvkey_matched(gvkey_target)
how_many_gvkey_matched(gvkey_acquirer)

matching_result_dict = defaultdict(list)
for i, name in enumerate(name_target):
	matching_result_dict[name].append(gvkey_target[i])

for i, name in enumerate(name_acquirer):
	matching_result_dict[name].append(gvkey_acquirer[i])

matching_result_dict = {key : set(value) for key, value in matching_result_dict.items()}

for key, value in matching_result_dict.items():
	if len(value) > 1:
		print key, value

nans = 0
for key, value in matching_result_dict.items():
	if 'nan' in value:
		nans += 1

print "Matched {0} companies out of {1}(CUSIP, TICKER, STR MATCH)".format(len(matching_result_dict) - nans, len(matching_result_dict))


#string comparison

#make list of indice to drop meaningless rows
#for index, value in enumerate(name_target):
#	if value in meaningless_names and index not in meaningless_list:
#		meaningless_list.append(index)

#make begine and end year
target_dict = defaultdict(list)

for index, name in enumerate(name_target):
	target_dict[name].append(index)


gvkey_target = pd.Series(gvkey_target)
name_target = pd.Series(name_target)
begyrs_target = pd.Series(begyrs_target)
endyrs_target = pd.Series(endyrs_target)

gvkey_acquirer = pd.Series(gvkey_acquirer)
name_acquirer = pd.Series(name_acquirer)
begyrs_acquirer = pd.Series(begyrs_acquirer)
endyrs_acquirer = pd.Series(endyrs_acquirer)

frame = [pd.Series(complete_year), gvkey_acquirer, name_acquirer, begyrs_acquirer, endyrs_acquirer, gvkey_target, name_target, begyrs_target, endyrs_target]

output = pd.concat(frame, axis = 1)

new_index = range(len(output))
output['index'] = new_index
output = output.set_index('index')

output = output.rename(index = str, columns = {0: "complete_year", 1:"gvkey_acquirer", 2:"name_acquirer", 3:"begyr_acquirer", 4:"endyr_acquirer", 5:"gvkey_target", 6:"name_target", 7:"begyr_target", 8:"endyr_target"})

output.to_csv("updated_MA_Data.csv")

