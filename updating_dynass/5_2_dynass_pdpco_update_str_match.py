'''
output: string matches b/w unmatched companies in dynass file and patent assignee names
'''


import pandas as pd
import jellyfish
from collections import defaultdict
import collections
import sys

def make_upper(input):
	for i in range(len(input)):
		input[i] = input[i].upper()

def element_remover(targets, input_list, splitter = ' ', joint = ' '):
	assert type(targets) is list
	if type(targets) is list:
		for target in targets:
			for i in range(len(input_list)):
				elements = input_list[i].split(splitter)
				if target in elements:
					elements.remove(target)
					for idx in range(len(elements)):
						elements[idx] = elements[idx].strip()
					input_list[i] = joint.join(elements)

def comma_space_remover(input_list):
	for i in range(len(input_list)):
		if '. ' in input_list[i]:
			splitted = input_list[i].split('. ')
			for idx in range(len(splitted)):
				splitted[idx] = splitted[idx].strip()
			input_list[i] = ' '.join(splitted)


def colon_remover(input_list):
	for i in range(len(input_list)):
		if ',' in input_list[i]:
			splitted = input_list[i].split(',')
			for idx in range(len(splitted)):
				splitted[idx] = splitted[idx].strip()
			input_list[i] = ' '.join(splitted)


def lookup(target, input_list): 
	output_dict = defaultdict(int)
	for name in input_list:
		if target in name:
			output_dict[name] += 1
	return output_dict

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

def scoring_function(dynass_name, berkeley_name, scoring_dict):
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


reload(sys)
sys.setdefaultencoding('utf8')

file1 = open('Data/Berkeley/grant_assignee_prob_fixed.tsv')
file2 = open('Data/dynass_pdpass_matching/unmatched_dynass_companies.csv')
file3 = open('Data/GVKEY_Data/gvkey_data_cleaned.csv')

#berkeley data columns: id, organization
berkeley = pd.read_csv(file1, dtype = str, sep = '\t')
berkeley = berkeley[['organization', 'id']]
berkeley = berkeley.dropna()
berkeley.reset_index(level = 0, inplace = True, drop = True)
#make list to start matching work
berk_names_orig = berkeley['organization'].tolist() #this is uncleaned names.
berk_names = list(berk_names_orig)

#dynass file columns: gvkey, Compustat company names 
dynass = pd.read_csv(file2, dtype = str, index_col = 0)
dynass_names_orig = dynass['name'].tolist()
dynass_names = list(dynass_names_orig)

#load names from compustat data
compustat_data = pd.read_csv(file3, dtype = str)
comp_names_orig = compustat_data.name
comp_names = list(comp_names_orig)

make_upper(comp_names)
make_upper(berk_names)
make_upper(dynass_names)
colon_remover(comp_names)
colon_remover(berk_names)
colon_remover(dynass_names)
comma_space_remover(comp_names)
comma_space_remover(berk_names)
comma_space_remover(dynass_names)


#let's make a dictionary to make a scoring system.

scoring_system = defaultdict(int)
for compustat_name in comp_names:
	elements = compustat_name.split(' ')
	for element in elements:
		scoring_system[element] += 1

for name in berk_names:
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

#dynass_names == the Compustat names of newly merged companies (cleaned)
#dynass_names_orig == the Compustat names of newly merged companies (original)
#berk_names_orig == the origianl berkeley names
#berk_names == the cleaned berkeley names.
berkeley_names = berk_names_orig
dynass_names = dynass_names_orig

matching_score_dict = defaultdict(list)


import time
time0 = time.clock()
count = 0
total = len(dynass_names)

for i in range(len(dynass_names)):
	for j in range(len(berkeley_names)):
		score = scoring_function(dynass_names[i], berk_names[j], scoring_system) #get score from cleaned name
		if score > 0.8:
			if dynass_names[i] not in matching_score_dict:
				matching_score_dict[dynass_names[i]] = [[[score],[berkeley_names[j]]],[[float(0)],[]],[[float(0)],[]],[[float(0)],[]]]
			else:
				if score > matching_score_dict[dynass_names[i]][3][0][0]:
					new_score = [[score], [berkeley_names[j]]] #we can match ids later. let's compare names first.
					if score > matching_score_dict[dynass_names[i]][0][0][0]:
						matching_score_dict[dynass_names[i]].insert(0, new_score)
					elif score > matching_score_dict[dynass_names[i]][1][0][0]:
						matching_score_dict[dynass_names[i]].insert(1, new_score)
					elif score > matching_score_dict[dynass_names[i]][2][0][0]:
						matching_score_dict[dynass_names[i]].insert(2, new_score)
					else:
						matching_score_dict[dynass_names[i]].append(new_score)
					matching_score_dict[dynass_names[i]].remove(matching_score_dict[dynass_names[i]][3])
	#count += 1
	#print "{0} out of {1} have been done".format(count, total)


print 'It took {0} minutes to complete'.format((time.clock() - time0)/60.0)

final_outcome = pd.DataFrame.from_dict(matching_score_dict, orient = 'index')
final_outcome.to_csv('str_match_newly_merged_and_berk.csv')
