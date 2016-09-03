#This program recursively track the last ownwers of GVKEY1 companies in dynass file
import pandas as pd
import numpy as np
from collections import defaultdict

file1 = open("Data/dynass/updated_dynass_with_new_pdpco.csv")
dynass = pd.read_csv(file1, index_col = 0, dtype = str)


def final_assignee(gvkey, dynass, cache = None):
    if cache is None:
        cache = {gvkey : 0 for gvkey in dynass.gvkey1}
    index = list(dynass.gvkey1).index(gvkey)
    series = dynass.loc[index]
    last_gvkey = series[series.count()-2]
    if dynass.loc[index].count() <= 6:						#There is only one gvkey. no gvkeys to track further. 
        return [series[1]] + list(series[3:6])				#we should just return the current last gvkey (which is gvkey1)
    elif last_gvkey not in cache:							#The same as above
        output = list(series.dropna()[:-5:-1])
        output = output.reverse()
        return output
    elif cache[last_gvkey] == 0:							#the last gvkey of current gvkey has not been visited. Can track further.
        cache[last_gvkey] = 1								#mark as True in cache[last_gvkey] (preventing infinite recursion).
        return final_assignee(last_gvkey, dynass, cache)	#recursively find the last gvkey of the current last gvkey
    else:
        output = list(series.dropna()[:-5:-1])				#current series > 6 (meaning more than 2 gvkeys are associated)
        output = output.reverse()							#AND the last gvkey has been visited. No need to recurse more.
        return output

pdpasses = []
sources = []
for pdpass in dynass.pdpass:
    if pd.notnull(pdpass):
        pdpasses.append(pdpass)
    else:
        pdpasses.append('NAN')

dynass.pdpass = pdpasses

updated = []
for idx, gvkey in enumerate(dynass.gvkey1):
    if dynass.loc[idx].count() > 6:										#gvkeys to track. recursively find the last gvkey of current gvkey
        last_assignee = final_assignee(gvkey, dynass)
        series = dynass.loc[idx]
        current_last_assignee = list(series.dropna()[:-5:-1])
        current_last_assignee.reverse()
        if np.any(pd.notnull(last_assignee)):
            if last_assignee[2] != current_last_assignee[2]:			#compare the last gvkey with the current last gvkey
                if current_last_assignee[1] > last_assignee[1]:			#compare begyears: (if current last gvkey begyr > last gvkey begyr)
                    last_assignee[1] = current_last_assignee[1]			#last gveky begyr = current last gvkey begyr
                if current_last_assignee[3] > last_assignee[3]:			#compare endyr (if endyr of the last gvkey is less than that of the current last)
                    last_assignee[3] = current_last_assignee[3]			#last gvkey endyr = current last gvkey enyr
                dynass.loc[idx, dynass.columns[dynass.loc[idx].count() : dynass.loc[idx].count() + 4]] = last_assignee
                updated.append(last_assignee)							#we cna check

pdpasses = []
sources = []
for pdpass in dynass.pdpass:
    if pdpass == "NAN":
        pdpasses.append(np.NAN)
    else:
        pdpasses.append(pdpass)

dynass.pdpass = pdpasses

dynass.to_csv("dynass_76_15.tsv", sep = '\t')

