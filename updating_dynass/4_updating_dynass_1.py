'''
input: M&A data with gvkeys, dynass (dynamic patent assignee data) from 1976 - 2006
output: dynass file from 1976 - 2015 (need to be updated more later)

'''

import pandas as pd
from collections import defaultdict

file1 = open("Data/MA_Data_SDC/updated_MA_Data.csv")
MAdata = pd.read_csv(file1, dtype = str, index_col = 0)

#MAdata.drop(MAdata.columns[0])
file2 = open("Data/dynass/dynass.dat")
dynass = pd.read_table(file2, dtype = str)

file3 = open("Data/GVKEY_Data/gvkey_data_cleaned.csv")
complete_gvkey = pd.read_csv(file3, dtype = str, index_col = 0)

#index, gvkey_acquirer, name_acquirer, begyr_acquirer, endyr_acquirer

MAdata = MAdata.dropna(subset = ['gvkey_target', 'gvkey_acquirer'])
MAdata.reset_index(level = 0, inplace = True, drop = True)

#add columns in dynass file
columns = ['pdpco6', 'begyr6', 'gvkey6', 'endyr6','pdpco7', 'begyr7', 'gvkey7', 'endyr7','pdpco8', 'begyr8', 'gvkey8', 'endyr8','pdpco9', 'begyr9', 'gvkey9', 'endyr9','pdpco10', 'begyr10', 'gvkey10', 'endyr10']
dynass_index = range(len(dynass))
additional_dynass = pd.DataFrame(index = dynass_index, columns = columns)

dynass = pd.concat([dynass, additional_dynass], axis = 1)


def duplicates(input_list, item):
	return [i for i, x in enumerate(input_list) if x == item]

gvkey_sequence = ['gvkey1','gvkey2','gvkey3','gvkey4','gvkey5','gvkey6','gvkey7','gvkey8','gvkey9', 'gvkey10']
gvkey_sequence.reverse() #to start from backward

gvkey1_indice_dict = defaultdict(list)
for index, gvkey in enumerate(dynass.gvkey1):
	gvkey1_indice_dict[gvkey].append(index)

for i in range(len(MAdata)):
	current_series = MAdata.loc[i]
	target_gvkey = MAdata.loc[i, "gvkey_target"]
	acquirer_gvkey = MAdata.loc[i, "gvkey_acquirer"]
	dynass_gvkey1_indice_list = gvkey1_indice_dict[target_gvkey]
	if dynass_gvkey1_indice_list:
		for index_dynass in dynass_gvkey1_indice_list:
			len_this_series = dynass.loc[index_dynass].count()
			last_acquirer_gvkey = dynass.iloc[index_dynass, len_this_series - 2]
			if last_acquirer_gvkey != acquirer_gvkey:
				dynass.iloc[index_dynass, len_this_series - 1] = MAdata.loc[i, "complete_year"]			#end year n-1
				dynass.iloc[index_dynass, len_this_series + 0] = acquirer_gvkey 						#new pdpco n (n th gvkey)
				dynass.iloc[index_dynass, len_this_series + 1] = MAdata.loc[i, "complete_year"]			#new begin year
				dynass.iloc[index_dynass, len_this_series + 2] = acquirer_gvkey							#new gvkey
				if MAdata.loc[i, "endyr_acquirer"] >= MAdata.loc[i, "complete_year"]:					#new end year
					dynass.iloc[index_dynass, len_this_series + 3] = MAdata.loc[i, "endyr_acquirer"]
				else:
					dynass.iloc[index_dynass, len_this_series + 3] = MAdata.loc[i, "complete_year"]
	else:
		new_row = pd.Series({"pdpass":'nan', 'pdpco1':target_gvkey, 'source':'SDC', 'begyr1':MAdata.loc[i, "begyr_target"], 
					'gvkey1': target_gvkey, 'endyr1': MAdata.loc[i, "complete_year"], 'pdpco2':acquirer_gvkey, "begyr2": MAdata.loc[i, "complete_year"],
					'gvkey2': acquirer_gvkey, 'endyr2': MAdata.loc[i, "endyr_acquirer"]})
		if new_row.loc["endyr2"] < MAdata.loc[i, "complete_year"]:
			new_row.loc["endyr2"] = MAdata.loc[i, "complete_year"]
		dynass = dynass.append(new_row, ignore_index = True)
		gvkey1_indice_dict[target_gvkey].append(len(dynass) - 1)


dynass.dropna(axis = 1, how = 'all', inplace = True)



dynass.to_csv('updated_dynass_1.csv')

#when there is no gvkey match in the dynass data
