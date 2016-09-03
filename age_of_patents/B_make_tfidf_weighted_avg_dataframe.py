'''
Input: patent_keyword_age_dicts, patent_keyword_tf_idf_score_dict
Output: patent-sum_of_earliness_score-avg_eariliness_score-tfidf_avg_score
'''
# zip -r tfidf_weighted_avg_score_dataframes.zip tfidf_weighted_avg_score_dataframes/*

# scp yongjip@hpcc.wharton.upenn.edu:/home/opim/yongjip/patentDB/yongjip/virtualenv/age_of_patents/tfidf_weighted_avg_score_dataframes.zip /home/yongjip/Documents/Research/age_of_patents

from __future__ import division, print_function
import cPickle
import numpy as np
import pandas as pd

def read_pickle(path):
    pkl_file = open(path, 'rb')
    out = cPickle.load(pkl_file)
    return out

def to_pickle(output, path):
    with open(path, 'wb') as handle:
        cPickle.dump(output, handle)

def calculate_earliness_score():

#load patent_tfidf_dict
file_path = 'patent_tfidf_dict.p'
patent_tfidf_dict = pd.read_pickle(file_path)
patent_tfidf_dict = {str(key): value for key, value in patent_tfidf_dict.items()}
# patent_tfidf_dict: patent_id -> keywords -> tfidf_scores

denominator_adjustment = 1

file_path = 'age_of_keywords_dict.p'
age_of_keywords_dict = pd.read_pickle(file_path)
age_of_keywords_dict = {str(key): value for key, value in age_of_keywords_dict.items()}

output = []



for patent_id, keywords_age_dict in age_of_keywords_dict.items():
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


