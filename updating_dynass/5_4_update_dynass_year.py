'''
update last years in dynass file in case when the years are not correct
'''

import pandas as pd

file1 = open("Data/dynass/updated_dynass_1.csv")
file2 = open("Data/GVKEY_Data/gvkey_data_cleaned.csv")
dynass = pd.read_csv(file1, dtype = str, index_col = 0)
gvkey_data = pd.read_csv(file2, dtype = str, index_col = 0)

gvkey_endyr = gvkey_data[["gvkey", "endyr"]]
gvkey_endyr = gvkey_endyr.set_index('gvkey')
gvkey_endyr_dict = gvkey_endyr.to_dict()['endyr']

for i in range(len(dynass)):
	len_this_series = dynass.loc[i].count()
	if len_this_series % 2 != 0:
		last_gvkey = dynass.iloc[i, len_this_series - 1]
		endyr_index = len_this_series
	else:
		last_gvkey = dynass.iloc[i, len_this_series - 2]
		endyr_index = len_this_series - 1
	if last_gvkey in gvkey_endyr_dict:
		if dynass.iloc[i, endyr_index] < gvkey_endyr_dict[last_gvkey]:
			dynass.iloc[i, endyr_index] = gvkey_endyr_dict[last_gvkey]

dynass.to_csv('updated_dynass_2.csv')
