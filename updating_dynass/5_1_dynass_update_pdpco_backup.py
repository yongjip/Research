#output = unmatched berkeley assignee ids and newly merged companies from dynass.

import pandas as pd
from collections import defaultdict
import collections
import time
import re

file1 = open("Data/Berkeley/grant_assignee_prob_fixed.tsv")
file2 = open('Data/GVKEY_Data/gvkey_data_cleaned.csv')
file3 = open('Data/BerkeleyAssignee_pdpass_76_06.csv')

#gvkey data. need to match assignee ids to this file.
#File 2 GVKEY data
gvkey_data = pd.read_csv(file2, index_col = 0)
gvkey_data['assignee_id'] = "nan"
gvkey_names = gvkey_data.name.tolist()
gvkey_assignee_ids = gvkey_data['assignee_id'].tolist()

#File 3. Variables: Pdpass, Berkeley_assignee_id
matching_result = pd.read_csv(file3, sep = '\t')
matched_assignee_ids = matching_result['Berkeley_assignee_ids']
raw_matched_assignee_ids = list(set(matched_assignee_ids)) #remove duplicated Berkeley assignee ids and make them into tuple

matched_assignee_ids = []
for i in range(len(raw_matched_assignee_ids)):
	temp = raw_matched_assignee_ids[i]
	temp = re.sub(r"[ \[ \] ' ]" , "", temp)
	if ',' in temp:
		temp = re.split(',', temp)
		matched_assignee_ids = matched_assignee_ids + temp
	else:
		matched_assignee_ids.append(str(temp))

#no need of these variables below
matched_nber_ids = matching_result['Pdpass']
matched_nber_ids = tuple(set(matched_nber_ids))

#File 1 Berkeley assignee data
assignee_data = pd.read_csv(file1, sep = '\t', index_col = 0)
assignee_data = assignee_data.dropna(subset = ['organization'], how = 'all') #remove NANs in the organization column
assignee_names = assignee_data.organization.tolist()
assignee_ids = assignee_data.id.tolist()

def remove_matched_ids(assignee_ids = assignee_ids, matched_assignees = matched_assignee_ids):
	before = len(assignee_ids)
	removed = 0
	for index in range(0, len(assignee_ids)):
		if assignee_ids[index] in matched_assignees:
			assignee_ids[index] = None
			removed += 1
	print "You removed {0} out of {1}. The remaining IDs are {2}".format(removed, before, before - removed)
	return assignee_ids

def make_upper(input=assignee_names):
	for i in range(len(input)):
		if pd.notnull(input[i]):
			input[i] = input[i].upper()

#make dict for matching
matched_assignee_dict = defaultdict(int)
for id in matched_assignee_ids:
	matched_assignee_dict[id] = 0

make_upper(assignee_names)
assignee_ids = remove_matched_ids(assignee_ids, matched_assignee_dict) #make matched ids into NANs.
assignee_data['id'] = assignee_ids
assignee_data = assignee_data[['id', 'organization']]
assignee_data = assignee_data.dropna()

#re-index assignee data
new_index = range(len(assignee_data))
assignee_data.index = new_index
assignee_data.to_csv("unmatched_berkeley_assignees.csv") #this file would not be used in the future. I have decided to use the entire Berkely assignee data.


'''		STEP 2		'''

###STEP 2: extract new rows in dynass file and their name from gvkey data

#File 4: dynass file
file4 = open('Data/dynass/updated_dynass_1.csv')
dynass = pd.read_csv(file4, dtype = str, index_col = 0)

#compustat gvkey data
file5 = open('Data/GVKEY_Data/gvkey_data_cleaned.csv')
gvkey_data = pd.read_csv(file5, dtype = str, index_col = 0)

dynass_pdpass = tuple(dynass.pdpass)
dynass_gvkey = tuple(dynass.gvkey1)
unmatched_gvkeys = []

for i in range(len(dynass_pdpass)):		#check newly added firms in the updated dynass file
	if dynass.loc[i, "pdpass"] == 'nan':
		unmatched_gvkeys.append(int(dynass.loc[i, "gvkey1"]))

#2-2 make compelete gvkey-name dictionay 
gvkey_name_dict = {}
for i in range(len(gvkey_data)):
	gvkey_name_dict[int(gvkey_data.loc[i, "gvkey"])] = gvkey_data.loc[i, "name"]


#2-3 map unmatched names and gvkeys
unmatched_names = []
for i in range(len(unmatched_gvkeys)):
	unmatched_names.append(gvkey_name_dict[int(unmatched_gvkeys[i])])

unmatched_dynass_companies = pd.DataFrame({'gvkey': unmatched_gvkeys, 'name': unmatched_names})
unmatched_dynass_companies = unmatched_dynass_companies.sort_values(by = 'gvkey', ascending = True, axis = 'index')
unmatched_dynass_companies = unmatched_dynass_companies.reset_index(level = 0, inplace = False, drop = True)
unmatched_dynass_companies = unmatched_dynass_companies.dropna()
unmatched_dynass_companies.to_csv("unmatched_dynass_companies.csv")