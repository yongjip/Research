'''
This will map M&A acquirees and acqurers to corresponding GVKEY using CUSIP, Ticker, and company names.
input : M&A data, GVKEY data
output: string match result file
'''

import pandas as pd
import numpy as np
from collections import defaultdict
from collections import Counter
import jellyfish
import collections
import sys

reload(sys)
sys.setdefaultencoding('utf8')


def make_int(input):
	assert type(input) == list
	for i in range(len(input)):
		try:
			input[i] = int(input[i])
		except:
			pass

def comma_remover(input_list):
	for i in range(len(input_list)):
		if '.' in input_list[i]:
			splitted = input_list[i].split('.')
			input_list[i] = ''.join(splitted)

def remove_parenthesis(input):
	for i in range(len(input)):
		name = input[i]
		if '(' in name and ')' in name:
			start = name.find('(')
			end = name.find(')')
			if end != len(name):
				input[i] = name[:start]+' '+name[end+1:]
			else:
				input[i] = name[:start]

def lookup(target, input_list):
	output_dict = defaultdict(int)
	for name in input_list:
		if target in name:
			output_dict[name] += 1
	return output_dict

def remove_after_splitter(input, splitter = ','):
	for i in range(len(input)):
		if splitter in input[i]:
			name_list = input[i].split(splitter)
			if len(name_list) > 1:
				name = name_list[0].strip()
				input[i] = name

def count_suffixes(input, splitter=','):
	name_frequency = defaultdict(int) 
	for name in input:
		if pd.notnull(name):
			name_list = name.split(splitter)
			if len(name_list) > 1:
				name_list = name_list[-1]
				name_frequency[name_list] += 1
		else:
			name_frequency['nan'] += 1
	frequent_names = defaultdict(list)
	for key, value in name_frequency.items():
		frequent_names[value].append(key)
	return collections.OrderedDict(sorted(frequent_names.items()))

def count_name_elements(input, splitter=' '):
	name_frequency = defaultdict(int) 
	for name in input:
		if pd.notnull(name):
			name_elements = name.split(splitter)
			for element in name_elements:	
				name_frequency[element] += 1
		else:
			name_frequency['nan'] += 1
	frequent_names = defaultdict(list)
	for key, value in name_frequency.items():
		frequent_names[value].append(key)
	return collections.OrderedDict(sorted(frequent_names.items()))



file1 = open("Data/MA_Data_SDC/MA_Data_cleaned.csv")
file2 = open("Data/GVKEY_Data/gvkey_data_cleaned.csv")
MA_data = pd.read_csv(file1, index_col = 0)
gvkey_data = pd.read_csv(file2, dtype = str, index_col = 0)

#remove meaningless index columns


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

#reference data
gvkey_ref = gvkey_data["gvkey"].tolist()
name_ref = gvkey_data["name"].tolist()

#transform 9 digit CUSIPs in Compustat data into 6 digit CUSIPs
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



'''matching result: '''
def how_many_gvkey_matched(gvkey_list):
	count = 0
	total = len(gvkey_list)
	for gvkey in gvkey_list:
		if gvkey == 'nan':
			count += 1
	print "Found: {0} \n Not Found: {1} \n Total: {2}".format(total - count, count, total)

how_many_gvkey_matched(gvkey_target)
how_many_gvkey_matched(gvkey_acquirer)


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

how_many_gvkey_matched(gvkey_target)
how_many_gvkey_matched(gvkey_acquirer)


'''assemble vectors '''
gvkey_target = pd.Series(gvkey_target)
name_target = pd.Series(name_target)
begyrs_target = pd.Series(begyrs_target)
endyrs_target = pd.Series(endyrs_target)

gvkey_acquirer = pd.Series(gvkey_acquirer)
name_acquirer = pd.Series(name_acquirer)
begyrs_acquirer = pd.Series(begyrs_acquirer)
endyrs_acquirer = pd.Series(endyrs_acquirer)

frame = [gvkey_acquirer, name_acquirer, begyrs_acquirer, endyrs_acquirer, gvkey_target, name_target, begyrs_target, endyrs_target]

output = pd.concat(frame, axis = 1)

new_index = range(len(output))
output['index'] = new_index
output = output.set_index('index')

output = output.rename(index = str, columns = {0:"gvkey_acquirer", 1:"name_acquirer", 2:"begyr_acquirer", 3:"endyr_acquirer", 4:"gvkey_target", 5:"name_target", 6:"begyr_target", 7:"endyr_target"})

output.to_csv("MA_Data_updated_by_cusip.csv")


###part 2: use names do not have gvkey in updated dynass file and the names in the Compustat data. use Jaro-Winkler-Yongjip distance to match strings.  
# First, I need to clean names (minimally) for string comparision.
# Outcome file = index in the M&A data, raw_name, scores_i, names_i, gvkey_i
# for the matching, we should use the cleaned names, but need to keep the original name to merge data.
# Once we found gvkey, we need to update begyr and endyr usint gvkeys

#compare string 2

def make_upper(input):
	for i in range(len(input)):
		input[i] = input[i].upper()

#find unmatched names in M&A data
unmatched_names_MA = []
for i in range(len(name_target)):
	if gvkey_target[i] == 'nan':
		unmatched_names_MA.append(name_target[i])

for i in range(len(name_acquirer)):
	if gvkey_acquirer[i] == 'nan':
		unmatched_names_MA.append(name_acquirer[i])

#remove duplicate names
raw_names = pd.unique(unmatched_names_MA)
raw_names = raw_names.tolist() #transform them into list
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

#step1: make a dictionary containing matched gvkeys
matched_gvkeys = pd.unique(gvkey_target.tolist()+gvkey_acquirer.tolist())
matched_gvkeys = matched_gvkeys.tolist()
matched_gvkeys.remove('nan') #now it contains only matched gvkeys

matched_gvkey_dict = {key : 0 for key in matched_gvkeys}

unmatched_compustat_names = []
unmatched_compustat_gvkeys = []
begyear = []
endyear = []

for i in range(len(gvkey_ref)):
	if gvkey_ref[i] not in matched_gvkey_dict:
		unmatched_compustat_names.append(name_ref[i])
		unmatched_compustat_gvkeys.append(gvkey_ref[i])
		begyear.append(begyrs[i])
		endyear.append(endyrs[i])

#Reference data are prepared

#Clean data and use JWY distance.
#Our varialbles: raw_names, cleaned_names, unmatched_compustat_names

make_upper(cleaned_names)
make_upper(unmatched_compustat_names)
count_name_elements(cleaned_names, ' ')



############################################################need to fix this to use#########
dynass_names = cleaned_names


#let's make a dictionary to make a scoring system.
all_names = name_target.tolist() + name_acquirer.tolist()
all_names = pd.unique(all_names)
all_names = all_names.tolist()
make_upper(all_names)

scoring_system = defaultdict(int)
for name in all_names:
	elements = name.split(' ')
	for element in elements:
		scoring_system[element] += 1

'''Test whether there is any missing element in score dict '''
scoring_system_temp = defaultdict(int)
for dynass_name in dynass_names:
	elements = dynass_name.split(' ')
	for element in elements:
		scoring_system_temp[element] += 1


#check if there is a missing value from a dict
for key in scoring_system_temp:
	if key not in scoring_system:
		print key

#remove needless elements from scoring system dict
be_removed = []
for key in scoring_system:
	if key not in scoring_system_temp:
		be_removed.append(key)

for item in be_removed:
	del scoring_system[item]

len(scoring_system)-len(scoring_system_temp) == 0

#Now step 2: make a score dictionary


#dynass_name = cleaned_names, berkeley_name = unmatched_compustat_data
def scoring_function(dynass_name, berkeley_name, scoring_dict = scoring_system):
	total_score = 0
	highest = 0
	normalizer = 0
	dynass_elements = dynass_name.split(' ')
	berkeley_elements = berkeley_name.split(' ')
	for dynass_element in dynass_elements:
		for berkeley_element in berkeley_elements:
			new_score = jellyfish.jaro_winkler(unicode(dynass_element), unicode(berkeley_element))
			new_score = float(new_score)/scoring_dict[dynass_element]
			if new_score > highest:
				highest = new_score
		normalizer += 1.0/scoring_dict[dynass_element]
		total_score += highest
		highest = 0
	return total_score / normalizer



''' the score matching procedure does not yield precise matching results. I need to clean string names first. And the procedure is extremely slow '''
#start comparing work
matching_score_dict = defaultdict(list)

import time
time0 = time.clock()
count = 0
total = len(dynass_names)

dynass_names = cleaned_names
berkeley_names = unmatched_compustat_names
berkeley_ids = unmatched_compustat_gvkeys

for i in range(len(dynass_names)):
	for j in range(len(berkeley_names)):
		score = scoring_function(dynass_names[i], berkeley_names[j], scoring_system)
		if score > 0.0:
			if dynass_names[i] not in matching_score_dict:
				matching_score_dict[dynass_names[i]] = [[[score],[berkeley_names[j]],[berkeley_ids[j]]],[[float(0)],[],[]],[[float(0)],[],[]]]
			else:
				if score > matching_score_dict[dynass_names[i]][2][0][0]:
					new_score = [[score], [berkeley_names[j]], [berkeley_ids[j]]] #we can match ids later. let's compare names first.
					if score > matching_score_dict[dynass_names[i]][0][0][0]:
						matching_score_dict[dynass_names[i]].insert(0, new_score)
					elif score > matching_score_dict[dynass_names[i]][1][0][0]:
						matching_score_dict[dynass_names[i]].insert(1, new_score)
					else:
						matching_score_dict[dynass_names[i]].append(new_score)
					matching_score_dict[dynass_names[i]].remove(matching_score_dict[dynass_names[i]][2])
	count += 1
	print "{0} out of {1} have been done".format(count, total)


print 'It took {0} minutes to complete'.format((time.clock() - time0)/60.0)

final_outcome = pd.DataFrame.from_dict(matching_score_dict, orient = 'index')
final_outcome.to_csv('string_matching_result_MAData_ver3.csv')




