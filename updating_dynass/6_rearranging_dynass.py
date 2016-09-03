# Rearrange the dynass file as my supervisor wants
import pandas as pd
import numpy as np
from collections import defaultdict
import re

def make_real_list(input):
	out = []
	for value in input:
		if pd.notnull(value):
			splitted = value.split(",")
			for i in range(len(splitted)):
				splitted[i] = re.sub(r"[\[\]]", "", splitted[i]).strip()
				splitted[i] = re.sub(r"\A'", "", splitted[i])
				splitted[i] = re.sub(r"'\Z", "", splitted[i])
				splitted[i] = re.sub(r'\A"', "", splitted[i])
				splitted[i] = re.sub(r'\Z"', "", splitted[i])
			out.append(splitted)
		else:
			out.append(value)
	input = out
	return input

file1 = open("Data/dynass/dynass_76_15.tsv")
dynass = pd.read_csv(file1, dtype = str, sep = '\t')
pdpasses = dynass.pdpass.tolist()
pdpasses = make_real_list(pdpasses)                                     #convert the pdpasses (berkids) into real lists
dynass.pdpass = pdpasses

file2 = open("Data/BerkeleyAssignee_pdpass_76_06.csv")                  #Frank's matching result file
berk_pdpass_76_06 = pd.read_csv(file2, dtype = str, sep = '\t')
berk_assi_ids = berk_pdpass_76_06.Berkeley_assignee_ids.tolist()
berk_assi_ids = make_real_list(berk_assi_ids)                           #convert Berk ids into lists
berk_pdpass_76_06.Berkeley_assignee_ids = berk_assi_ids

file3 = open("Data/Berkeley/grant_assignee.tsv")
berk_data = pd.read_csv(file3, dtype = str, sep = '\t')
berk_id_name_dict = {key : value for key, value in zip(berk_data.id, berk_data.organization)}

#pdpass ==> Berk assignee ids
pdpass_berk_dict = {key : value for key, value in zip(berk_pdpass_76_06.Pdpass, berk_pdpass_76_06.Berkeley_assignee_ids)}



result_dict = defaultdict(list)

all_gvkeys = ["gvkey1", "gvkey2", "gvkey3", "gvkey4", "gvkey5"]
all_begyrs = ["begyr1", "begyr2", "begyr3", "begyr4", "begyr5"]
all_endyrs = ["endyr1", "endyr2", "endyr3", "endyr4", "endyr5"]

for index in range(len(dynass)):
    for gvkey_i in all_gvkeys:
        gvkey = dynass.loc[index, gvkey_i]
        if pd.notnull(gvkey) and np.any(pd.notnull(dynass.loc[index, "pdpass"])):
            for pdpass in dynass.loc[index, "pdpass"]:
                if pdpass in pdpass_berk_dict:
                    berk_ids = pdpass_berk_dict[pdpass]
                    begyr_i = all_begyrs[all_gvkeys.index(gvkey_i)]
                    endyr_i = all_endyrs[all_gvkeys.index(gvkey_i)]
                    begyr = dynass.loc[index, begyr_i]
                    endyr = dynass.loc[index, endyr_i]
                    for year_i in range(int(begyr), int(endyr)+1):
                        key = tuple([gvkey, year_i])
                        result_dict[key] += berk_ids
                elif pdpass in berk_id_name_dict:           #in case pdpass == berkeley assignee id
                    berk_id = [pdpass]                      #make list to append it to existing dictionary.
                    begyr_i = all_begyrs[all_gvkeys.index(gvkey_i)]
                    endyr_i = all_endyrs[all_gvkeys.index(gvkey_i)]
                    begyr = dynass.loc[index, begyr_i]
                    endyr = dynass.loc[index, endyr_i]
                    for year_i in range(int(begyr), int(endyr)+1):
                        key = tuple([gvkey, year_i])
                        result_dict[key] += berk_id


index = tuple(key for key in result_dict)
berk_ids = [result_dict[key] for key in result_dict]    #value list: list of berkeley IDs (b/w 76 to 15, Frank's and my work)
new_berk_ids = []
for pdpass in berk_ids:
	new_berk_ids.append(set(pdpass))                #remove duplicated Berk assignee IDs (set)
berk_ids = []
for pdpass in new_berk_ids:
	berk_ids.append(list(pdpass))                   #reassigne the Berk ids (list)

index = pd.Series(index)
berk_ids = pd.Series(berk_ids)


#berk_id_name_dict was creadted. use this to map Berk ids to their Berk names

berk_names = []

for berk_ids_i in berk_ids:                 #in berk_ids, we have list of berkeley ids.
	temp_names = []
	for i in range(len(berk_ids_i)):
		temp_names.append(berk_id_name_dict[berk_ids_i[i]])
	berk_names.append(temp_names)       #Berk company names.

berk_names = pd.Series(berk_names)          #make berk_names into Series to concatnate

outcome = pd.concat([index, berk_ids, berk_names], axis = 1)
outcome = outcome.rename(columns = {0:'index', 1:'Berkeley_assignee_id', 2:'Berkeley_assignee_names'})
outcome = outcome.set_index('index')

gvkey_year = outcome.index
gvkey_year = gvkey_year.tolist()
gvkey_year_list = [list(item) for item in gvkey_year]
gvkey_year = gvkey_year_list

gvkeys = []
years = []
for gvkey, year in gvkey_year:
	gvkeys.append(gvkey)
	years.append(year)

outcome.insert(0, 'gvkey', gvkeys)
outcome.insert(1, 'year', years)
outcome = outcome.sort_values(by=['gvkey', 'year'], ascending = [True, True])
outcome = outcome.reset_index(level = 0, drop = True)

outcome.to_csv("BerkeleyAssignee_gvkey_76_15.tsv", sep = '\t')

outcome[outcome['year'] < 2015].to_csv("BerkeleyAssignee_gvkey_76_14.tsv", sep = '\t')

nancount = 0
berkeley = 0

for value in berk_ids:
	if np.any(pd.isnull(value)):
		nancount += 1
	if np.any(pd.notnull(value)):
		if len(value) > 10:
			berkeley += 1

print "# of nan: ", nancount, "\n #of Berkeley IDs: ", berkeley

#this is for quality check: will include Compustat names next to GVKEYs

file5 = open("Data/GVKEY_data/gvkey_data_cleaned.csv")
comp_data = pd.read_csv(file5, dtype = str, index_col = 0)

gvkey_name_dict = {gvkey: name for gvkey, name in zip(comp_data.gvkey, comp_data.name)}

comp_names = []
for gvkey in outcome['gvkey']:
    if gvkey in gvkey_name_dict:
        comp_names.append(gvkey_name_dict[gvkey])
    else:
        comp_names.append(np.NAN)

outcome.insert(1, "Compustat_name", comp_names)
outcome.to_csv("BerkeleyAssignee_gvkey_76_15_FOR_QUALITY_CHECK.tsv", sep = '\t')
