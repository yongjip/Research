import pandas as pd
import numpy as np
from collections import defaultdict

file1 = open("updated_dynass_with_new_pdpco.csv")
dynass = pd.read_csv(file1, index_col = 0, dtype = str)



def final_assignee(gvkey, dynass, cache = None):
    if cache is None:
        cache = {gvkey:0 for gvkey in dynass.gvkey1}
    index = list(dynass.gvkey1).index(gvkey)
    series = dynass.loc[index]
    last_gvkey = series[series.count()-2]
    if last_gvkey not in cache and series.count() <= 6:
        return np.NAN
    elif last_gvkey not in cache and series.count() > 6:
        output = list(series.dropna()[:-5:-1])
        output = output.reverse()
        return output
    if cache[last_gvkey] == 0:
        cache[last_gvkey] = 1
        if series.count() <= 6:
            return [series[1]] + list(series[3:6])
        else:
            return final_assignee(last_gvkey, dynass, cache)
    else:
        if dynass.loc[index].count() <= 6:
            return [series[1]] + list(series[3:6])
        else:
            output = list(series.dropna()[:-5:-1])
            output = output.reverse()
            return output

pdpasses = []
sources = []
for pdpass in dynass.pdpass:
    if pd.notnull(pdpass):
        pdpasses.append(pdpass)
    else:
        pdpasses.append('NAN')

for source in dynass.source:
    if pd.notnull(source):
        sources.append(source)
    else:
        sources.append('NAN')

dynass.pdpass = pdpasses
dynass.source = sources

updated = []
for idx, gvkey in enumerate(dynass.gvkey1):
    if dynass.loc[idx].count() <= 6:
            pass
    else:
        last_assignee = final_assignee(gvkey, dynass)
	if dynass.loc[idx].count() <= 6:
            current_assignee = list(dynass.loc[idx, (['pdpco1','begyr1','gvkey1','endyr1'])])
	else:
            series = dynass.loc[idx]
            current_assignee = list(series.dropna()[:-5:-1])
            current_assignee.reverse()
        if np.any(pd.notnull(last_assignee)):
            if last_assignee[2] != current_assignee[2]:
                if current_assignee[1] > last_assignee[1]:
                    last_assignee[1] = current_assignee[1]
#            if current_assginee[3] < last_assignee[3]:
#                last_assignee[3] = current_assignee[3]
                dynass.loc[idx, dynass.columns[dynass.loc[idx].count() : dynass.loc[idx].count() + 4]] = last_assignee
                updated.append(last_assignee)

pdpasses = []
sources = []
for pdpass in dynass.pdpass:
    if pdpass == "NAN":
        pdpasses.append(np.NAN)
    else:
        pdpasses.append(pdpass)

for source in dynass.source:
    if source == "NAN":
        sources.append(np.NAN)
    else:
        sources.append(source)

dynass.pdpass = pdpasses
dynass.source = sources


dynass.to_csv("dynass_76_15.tsv", sep = '\t')

