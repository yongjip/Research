#strangely, berkeley assignee names are in the "residence" colum when "name_last" == "organization". So I need to fix this problem. 

import pandas as pd

file1 = open('Data/Berkeley/grant_assignee.tsv')
berkeley = pd.read_csv(file1, dtype = str, sep = '\t')
names = berkeley.organization.tolist()

count = 0
for i in range(len(berkeley)):
	if names[i] == berkeley.name_last[i]:
		count += 1
		names[i] = berkeley.residence[i]

berkeley['organization'] = names
berkeley.to_csv("grant_assignee_prob_fixed.tsv", sep = '\t')
berkeley.to_csv("grant_assignee_prob_fixed.csv")