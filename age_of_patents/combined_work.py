from __future__ import division, print_function
import cPickle
import numpy as np
import pandas as pd


def read_pickle(path):
    pkl_file = open(path, 'rb')
    out = cPickle.load(pkl_file)
    return out


# 1. Make patent -> keywords -> scores dictionary
patent_tfidf_dict = {}

for year in range(1963, 2015):
    year = str(year)
    relative_path = '63_14_abstract_tf/' + year + '_dict_patent_id_dict_word_tf.p'
    partial_patent_tfidf_dict = read_pickle(relative_path)
    patent_tfidf_dict.update(partial_patent_tfidf_dict)

patent_tfidf_dict = {str(key): value for key, value in patent_tfidf_dict.items()}
# OUTCOME 1: patent_tfidf_dict (patent_id -> keywords -> score)

# 2. 
fields = ['Patent_ID', 'Application_year', 'Grant_year']
patent_app_year = pd.read_csv('database/patent76_14_sorted.csv', usecols=fields, dtype=str)

fields = ['patent_id','application_year', 'grant_year']
patent_app_year_64_75 = pd.read_csv('database/data_63_75_all.csv', usecols=fields, dtype=str)
patent_app_year_64_75.rename(columns={'patent_id': 'Patent_ID', 'application_year': 'Application_year', 'grant_year': 'Grant_year'}, inplace=True)

patent_app_year = patent_app_year.append(patent_app_year_64_75, ignore_index=True)

# if no application year, grant year => application year
app_years = []
for i, app_year in enumerate(patent_app_year['Application_year']):
    if pd.isnull(app_year):
        app_years.append(patent_app_year['Grant_year'].iloc[i])
    else:
        app_years.append(app_year)

patent_app_year['Application_year'] = app_years
patent_app_year.set_index('Patent_ID', inplace=True)
patent_app_year_dict = patent_app_year.to_dict()['Application_year']
#OUTCOME 2: patent_app_year_dict (patent_id -> application year)

# 3. Want to make: words -> first year it appeared
keyword_year_dict = {}

for patent, keywords_tfidf in patent_tfidf_dict.items():
    patent = str(patent)
    application_year = patent_app_year_dict[patent]
    for keyword in keywords_tfidf.keys():
        if keyword not in keyword_year_dict:
            keyword_year_dict[keyword] = application_year
        else:
            if application_year < keyword_year_dict[keyword]:
                keyword_year_dict[keyword] = application_year
#OUTCOME 3: keyword_year_dict (keywords -> corresponding first year of keyword)

# 4. dictionaty of: patents -> keywords -> age of keyword
patent_age_dict = {}
patents = patent_tfidf_dict.keys()
for patent in patents:
    application_year = patent_app_year_dict[patent]
    keywords = patent_tfidf_dict[patent].keys()
    if keywords:
        for keyword in keywords:
            age_of_keyword = int(application_year) - int(keyword_year_dict[keyword])
            age_of_keyword = str(age_of_keyword)
            if patent not in patent_age_dict:
                patent_age_dict[patent] = {}
            patent_age_dict[patent][keyword] = age_of_keyword
    else:
        patent_age_dict[patent] = {}
#OUTCOME 4: keyword_year_dict (keyword -> first_year)

del patent_app_year
del patent_app_year_dict
del keyword_year_dict

#load patent_tfidf_dict
# patent_tfidf_dict: patent_id -> keywords -> tfidf_scores


denominator_adjustment = 1

output = []

for patent_id, keywords_age_dict in patent_age_dict.items():
    num_of_keywords = len(keywords_age_dict)
    if num_of_keywords == 0: #col 1, 2, 3 are 0
        total_earliness_score = 0
        avg_earliness_score = 0
        total_num_word = 0
    else:
        total_num_word = sum(patent_tfidf_dict[patent_id].values()) #col 3
        current_patent_tfidf_dict = patent_tfidf_dict[patent_id]
        total_earliness_score = 0
        for keyword, word_frequency in current_patent_tfidf_dict.items():
            age_of_keyword = int(keywords_age_dict[keyword])
            earliness_score_of_keyword = 1 / (age_of_keyword + denominator_adjustment) * word_frequency
            total_earliness_score += earliness_score_of_keyword # col 1
        avg_earliness_score = total_earliness_score / total_num_word #col 2
    output.append([patent_id, total_earliness_score, avg_earliness_score, total_num_word])

output = np.array(output)
output = pd.DataFrame(output, columns=['patent_id', 'total_earliness_scores', 'avg_earliness_score', 'total_num_word'])
output = output.sort_values(by='patent_id')
output.reset_index(drop=True, inplace=True)

output_file_name = 'earliness_score_dataframe.csv'
output.to_csv(output_file_name)


