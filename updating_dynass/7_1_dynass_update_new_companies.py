#update the new dynass file with new company.
import pandas as pd
import numpy as np
from collections import defaultdict

def upper_and_strip(input_list):
    assert type(input_list) is list
    for i in range(len(input_list)):
        input_list[i] = input_list[i].upper().strip()
    return input_list


file1 = open("str_match_result_122c_HANDPICKED.csv")
new_company_match = pd.read_csv(file1, dtype = str, index_col = 0)

file2 = open("Data/Berkeley/grant_assignee_prob_fixed.csv")
berk_data = pd.read_csv(file2, dtype = str, index_col = 0)
berk_data = berk_data.dropna(subset = ["organization"])
berk_data.reset_index(level = 0, drop = True, inplace = True)
berk_names = upper_and_strip(berk_data["organization"].tolist())
berk_data.organization = berk_names

berk_name_id_dict = defaultdict(list)
for i in range(len(berk_data)):                 #complete Berkeley name : assignee id dictionary
    berk_name_id_dict[berk_data.loc[i, "organization"]].append(berk_data.loc[i, "id"])


file3 = open("Data/GVKEY_Data/gvkey_data_cleaned.csv")
compustat_data = pd.read_csv(file3, dtype = str, index_col = 0)
compustat_names = upper_and_strip(compustat_data['name'].tolist())
compustat_data.name = compustat_names
compustat_name_index_dict = {compustat_data.loc[i, 'name'] : i for i in range(len(compustat_data)) }

wrong_comp_names = []
wrong_berk_names = []
gvkey_berk_id_dict = defaultdict(list)
for index in range(len(new_company_match)):
    company_name = new_company_match.loc[index, "name"].upper().strip()
    if company_name in compustat_name_index_dict:
        compustat_idx = compustat_name_index_dict[company_name]
        gvkey = compustat_data.loc[compustat_idx, 'gvkey']
        for i in range(1, 20):
            name_i = "name"+str(i)
            berk_name = new_company_match.loc[index, name_i]
            if pd.notnull(berk_name):
                berk_name = berk_name.upper().strip()
                if berk_name in berk_name_id_dict:
                    gvkey_berk_id_dict[gvkey] += berk_name_id_dict[berk_name]
                else:
                    wrong_berk_names.append(berk_name)
    else:
        wrong_comp_names.append(company_name) 

#gvkey => berkeley assignee ids (dict is ready)

print wrong_comp_names
print wrong_berk_names
#should be no wrong names

file4 = open("Data/dynass/dynass_76_15.tsv")
dynass = pd.read_csv(file4, dtype = str, sep = '\t', index_col = 0)

gvkey1s = {gvkey : 0 for gvkey in dynass.gvkey1}

for index in range(len(new_company_match)):
    compustat_name = new_company_match.loc[index, "name"]
    compustat_index = compustat_name_index_dict[compustat_name]
    gvkey = compustat_data.loc[compustat_index, "gvkey"]
    if gvkey not in gvkey1s:
        begyr = compustat_data.loc[compustat_index, "begyr"]
        endyr = compustat_data.loc[compustat_index, "endyr"]
        if gvkey in gvkey_berk_id_dict:
            pdpass = gvkey_berk_id_dict[gvkey]
        else:
            pdpass = np.NAN
        series = pd.Series({"pdpass" : pdpass, "pdpco1": gvkey, "source" : "Compustat", "begyr1": begyr, "gvkey1": gvkey, "endyr1": endyr})
        dynass = dynass.append(series, ignore_index = True)

dynass.to_csv("dynass_76_15_plus_new_firms.tsv", sep = '\t')
