# Library importation
import sqlite3
import time
import numpy as np
import pandas as pd
import re
import json

# Definition of the functions used in the process
def create_count_pairs(all_sub):
    # Create the dictionary that stores the number of authors in common for each pair of subreddits
    # Size of the dictionary: N(N-1)/2 with N the number of subreddits
    nb_sub = len(all_sub)
    count_pairs = {}
    i = 0
    for sub in all_sub[:-1]:
        count_pairs[sub] = {}
        for k in range(i+1, nb_sub):
            count_pairs[sub][all_sub[k]] = 0
        i += 1
    return count_pairs
    
def increment_count_pairs(count_pairs, sub1, sub2):
    # Function that increments the number of authors in common for the selected pair of subreddits (sub1, sub2)
    # Due to the construction of the dictionary, the keys are a bit tricky and we need to make sure to reach the right pair in the dictionary
    if sub1 in count_pairs:
        if sub2 in count_pairs[sub1]:
            count_pairs[sub1][sub2] += 1
        else:
            count_pairs[sub2][sub1] += 1
    else:
        count_pairs[sub2][sub1] += 1
        
def update_count_pairs(count_pairs, all_sub, selected_sub):
    # From a list of subreddits all_sub, we will increment the number in the dictionary for each pair of that list
    # However, we only have interest in the subreddits that are present in the list selected_sub in order to reduce the computation time
    nb_sub = len(all_sub)
    i = 0
    for sub in all_sub[:-1]:
        for k in range(i+1, nb_sub):
            if sub in selected_sub and all_sub[k] in selected_sub:
                increment_count_pairs(count_pairs, sub, all_sub[k])
        i += 1
        
# Loading the list of subreddits with total comments
# The idea is to select only the subreddits that contain at least 1700 comments (that corresponds to 0,05% of the subreddits ~ 2400 subreddits)
with open('data/nbcomments_sub.json') as fp:
    nbcomments_sub = json.load(fp)

df = pd.DataFrame.from_dict(nbcomments_sub, orient = "index")

df_select = df.loc[df.iloc[:,0] >1700]
selected_sub = list(df_select.index)

selected_sub_set = set(selected_sub)

# Connection to the data base
conn = sqlite3.connect('data/reddit.db')

# Query: we concatenate all the distinct subreddit_id where the author has posted a comment
query = "select author_id, group_concat(distinct subreddit_id) \
         from comments \
         group by author_id"

start_time = time.time()
i = 0

count_pairs = create_count_pairs(selected_sub) # Create the dictionary result
for row in conn.execute(query):
    update_count_pairs(count_pairs, re.split(',',row[1]), selected_sub_set) # update the dictionary row by row
    if i % 5000 == 0:
        print("Number of Autors: {} ----- Time: {}".format(i, time.time()-start_time))
    i += 1

# Store the dictionary result in a file
fileName = 'results_2.json'
with open('data/' + fileName, 'w') as fp:
                json.dump(count_pairs,fp)

        
# Functions to find the maximums in a dictionary

def update_max(all_max, all_pairs, count, sub1, sub2):
    # This is a sub function that helps updating the list of maximums and the list of associated pairs of subreddits
    # The idea is to update both lists all_max and all_pairs, by inserting in the right place the new maximum (count, sub1, sub2)
    n = len(all_max)
    new_all_max = [0]*n
    new_all_pairs = [('','')]*n
    pos = 0
    for i in range(1,n):
        if count <= all_max[i]:
            new_all_max[i-1] = count
            new_all_pairs[i-1] = (sub1, sub2)
            for j in range(i,n):
                new_all_max[j] = all_max[j]
                new_all_pairs[j] = all_pairs[j]
            break
        elif i==n-1:
            new_all_max[i-1] = all_max[i]
            new_all_pairs[i-1] = all_pairs[i]
            new_all_max[i] = count
            new_all_pairs[i] = (sub1, sub2)
        else:
            new_all_max[i-1] = all_max[i]
            new_all_pairs[i-1] = all_pairs[i]
    return new_all_max, new_all_pairs


def retrieve_max_pairs(nb_max, count_pairs):
    # This is the main function to retrieve a certain number of maximums (nb_max) in the dictionary (count_pairs)
    # The idea is to iterate through all the dictionary value by value, and update the 2 lists all_max and all_pairs little by little
    # These 2 lists are updated only if a new value in a dictionary is higher than all the values in the list all_max
    # The main advantage of this function is that we go through the dictionary only once and retrieve the 10 higher maximums for example
    all_max = [0]*nb_max
    all_pairs = [('','')]*nb_max
    for key1, value in count_pairs.items():
        for key2, count in value.items():
            if count > all_max[0]:
                all_max, all_pairs = update_max(all_max, all_pairs, count, key1, key2)
                #print(all_max)
    result = {}
    for (m, pair) in zip(all_max, all_pairs):
        result[pair] = m
    return result

retrieve_max_pairs(10, count_pairs)

'''returns 
{('t5_2qh0u', 't5_2qh1e'): 67090,
 ('t5_2qh0u', 't5_2qh1i'): 138672,
 ('t5_2qh0u', 't5_2qh33'): 104903,
 ('t5_2qh0u', 't5_2qqjc'): 71557,
 ('t5_2qh1e', 't5_2qh1i'): 91975,
 ('t5_2qh1i', 't5_2qh33'): 141788,
 ('t5_2qh1i', 't5_2qh61'): 71247,
 ('t5_2qh1i', 't5_2qqjc'): 105138,
 ('t5_2qh1i', 't5_2s7tt'): 84457,
 ('t5_2qh33', 't5_2qqjc'): 71664}'''