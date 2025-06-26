# this code is used to check the trajectories

import pandas as pd
import os
import numpy as np
import csv

one_hop_df = pd.read_csv('one_hop_edges.csv', sep=',')
file_list = ['3_node.csv', '4_node.csv', '5_node,csv']

for i in range(1, len(file_list)):
    file_name = 'pre_'+file_list[i]

    if not os.path.exists(file_name):
        continue

    df = pd.read_csv(file_name, sep='\t')
    num_node = i+3

    merged_df = df.copy(deep=True)
    column_list = df.columns.tolist()
    for j in range(num_node-1):
        print('0')
        merged_df = pd.merge(merged_df, one_hop_df, how='left', left_on=['d'+str(j+1), 'd'+str(j+2)], right_on=['Source', 'Target'])
        merged_df = merged_df[~merged_df['Source'].isna()]
        merged_df = merged_df[column_list]

    merged_df.to_csv('pre_'+str(i+3)+'_node_filtered.csv', index=False)

for i in range(1, len(file_list)):
    file_name = 'post_' + file_list[i]

    if not os.path.exists(file_name):
        continue

    df = pd.read_csv(file_name, sep='\t')
    num_node = i + 3

    merged_df = df.copy(deep=True)
    column_list = df.columns.tolist()
    for j in range(num_node - 1):
        print('0')
        merged_df = pd.merge(merged_df, one_hop_df, how='left', left_on=['d' + str(j + 1), 'd' + str(j + 2)],
                             right_on=['Source', 'Target'])
        merged_df = merged_df[~merged_df['Source'].isna()]
        merged_df = merged_df[column_list]

    merged_df.to_csv('post_' + str(i + 3) + '_node_filtered.csv', index=False)



