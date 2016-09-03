'''
This program cleans the string match result file so that I can work easily in Excel
'''


import pandas as pd
import re


file1 = open("Data/dynass_pdpass_matching/str_match_newly_merged_and_berk.csv")
results = pd.read_csv(file1, dtype = str)


name = results[results.columns[0]]
col0 = results[results.columns[1]]
col1 = results[results.columns[2]]
col2 = results[results.columns[3]]
col3 = results[results.columns[4]]

score1 = []
name1 = []
score2 = []
name2 = []
score3 = []
name3 = []
score4 = []
name4 = []


def content_cleaner(col, score, name):
	for i in range(len(col)):
		col[i] = col[i].split("],")
		for index in range(0,2):
			col[i][index] = re.sub(r"[\]\[]", "", col[i][index]).strip()
			col[i][index] = re.sub(r"\A'", "", col[i][index])
			col[i][index] = re.sub(r"'\Z", "", col[i][index])
			col[i][index] = re.sub(r'\A"', "", col[i][index])
			col[i][index] = re.sub(r'"\Z', "", col[i][index])
		score.append(col[i][0])
		name.append(col[i][1])

content_cleaner(col0, score1, name1)
content_cleaner(col1, score2, name2)
content_cleaner(col2, score3, name3)
content_cleaner(col3, score4, name4)
score1, name1 = pd.Series(score1), pd.Series(name1)
score2, name2 = pd.Series(score2), pd.Series(name2)
score3, name3 = pd.Series(score3), pd.Series(name3)
score4, name4 = pd.Series(score4), pd.Series(name4)

frame = [name, score1, name1, score2, name2, score3, name3, score4, name4]

cleaned_result = pd.concat(frame, axis = 1, ignore_index = True)
cleaned_result = cleaned_result.rename(index = str, columns = {0: 'name', 1: 'score1', 2: 'name1',3: 'score2', 4: 'name2',5: 'score3', 6: 'name3', 7: 'score4', 8:'name4'})

cleaned_result.to_csv("str_match_newly_merged_and_berk_final.csv")
