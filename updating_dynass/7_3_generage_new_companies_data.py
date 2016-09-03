import pandas as pd
from collections import defaultdict

file1 = open("Data/GVKEY_Data/gvkey_data_cleaned.csv")
file2 = open("Data/dynass/dynass_76_15.tsv")

gvkeyData = pd.read_csv(file1, dtype = str, index_col = 0)
new_gvkeys = gvkeyData[gvkeyData.begyr > "2006"]

new_gvkey_dict = {gvkey : name for gvkey, name in zip(new_gvkeys.gvkey, new_gvkeys.name)}

dynass = pd.read_csv(file2, dtype = str, sep = '\t', index_col = 0)
new_dynass = dynass.loc[13458:] 							#the first new added rows to the end
new_dynass = new_dynass[new_dynass.begyr1 > str(2006)]					#remove rows with begyr less than 2007
new_index = range(len(new_dynass))
new_dynass.index = new_index

gvkeys = new_dynass.gvkey1

names = []
for key in gvkeys:
	names.append(new_gvkey_dict[key])

names = pd.Series(names)
pdpasses = new_dynass.pdpass

output = pd.concat([names, gvkeys, pdpasses], axis = 1)
output = output.rename(columns = {0 : "name", 'gvkey1' : 'gvkey', 'pdpass' : 'Berkeley_assignee_id'})

output.to_csv("gvkey_berkeleyAssignee_new_firms.tsv", sep = '\t')
