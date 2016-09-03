'''
update dynass file with new pdpasses
input: handpidcked string match result file, assignee data, gvkey data, dynass file
output: updated dynass file
'''


import pandas as pd
import numpy as np
import re
from collections import defaultdict
import sys
reload(sys)


def upper_strip(input_list):
	assert type(input_list) is list
	return [value.strip().upper() for value in input_list]

file1 = open("Data/dynass/updated_dynass_2.csv")
dynass = pd.read_csv(file1, dtype = str, index_col = 0)

file2 = open("Data/dynass_pdpass_matching/new_mergers_str_match_reduced_Jack.csv")
match_result = pd.read_csv(file2, dtype = str, index_col = 0)
match_result = match_result.reset_index(level = 0, drop = True)
match_result.drop(['score1', 'score2', 'score3', 'score4'], axis = 1, inplace = True)

file3 = open("Data/Berkeley/grant_assignee_prob_fixed.tsv")
berk_assignee = pd.read_csv(file3, dtype = str, index_col = 0, sep = '\t')
berk_assignee = berk_assignee.dropna(subset = ["organization"])
berk_assignee.reset_index(level = 0, drop = True, inplace = True)

berk_names = berk_assignee.organization.tolist()
berk_names = upper_strip(berk_names)
berk_assignee['organization'] = berk_names

assignee_dict = defaultdict(list)
for i in range(len(berk_assignee.organization)):
	name = berk_assignee.loc[i, "organization"]
	assignee_dict[name].append(berk_assignee.loc[i, 'id'])

file4 = open("Data/GVKEY_Data/gvkey_data_cleaned.csv")
complete_gvkey = pd.read_csv(file4, dtype = str, index_col = 0)
name_gvkey_dict = {name : gvkey for name, gvkey in zip(complete_gvkey.name, complete_gvkey.gvkey)}

match_gvkey = [name_gvkey_dict[name] for name in match_result.name]

match_berk_id = defaultdict(list)
not_in_data = []
updated = 0
for i in range(len(match_result)):
	series = match_result.loc[i]
	len_series = match_result.loc[i].count()
	count = 1
	for idx in range(1, len(series)):
		if pd.notnull(match_result.iloc[i, idx]):
			count += 1
			name = match_result.iloc[i, idx].upper().strip()
			if name in assignee_dict:
				berk_ids = assignee_dict[name]
				if berk_ids not in match_berk_id[match_gvkey[i]]:
					match_berk_id[match_gvkey[i]] += berk_ids
					updated += 1
			else:
				name = match_result.iloc[i, idx]
				not_in_data.append([name, i, idx])
				match_berk_id[match_gvkey[i]].append(name)
				updated += 1
			if count == len_series:
				break

print "Number of Pdpasses Updated: {0} \nCompanies Not in Berkeley Data(name, location in match_result): {1}".format(updated, not_in_data)

#pdpass dictionary for newly merged companies is ready

for i in range(len(dynass)):
	if pd.isnull(dynass.loc[i, "pdpass"]):
		gvkey = dynass.loc[i, "gvkey1"]
		if match_berk_id[gvkey]:
			dynass.loc[i, "pdpass"] = match_berk_id[gvkey]

dynass.to_csv("updated_dynass_with_new_pdpco.csv")



