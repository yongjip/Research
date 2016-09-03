'''
input: assignee data, gvkey data, assignee pdpass, uptated dynass file
outputs: unmatched berkeley assignee ids and newly merged companies from dynass.
to map GVKEYs to patent assignee IDs, I need these outputs
'''

import pandas as pd
from collections import defaultdict
import collections
import time
import re

file1 = open("Data/Berkeley/grant_assignee_prob_fixed.tsv")
file2 = open('Data/GVKEY_Data/gvkey_data_cleaned.csv')
file3 = open('Data/BerkeleyAssignee_pdpass_76_06.csv')

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
	if pd.isnull(dynass.loc[i, "pdpass"]):
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
