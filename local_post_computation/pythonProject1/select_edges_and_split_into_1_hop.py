# this code is used to read the multi-hop edges and
# select them and further split them into 1_hop edge
# which can be further used as input to gephi

import csv
import pandas as pd
import numpy as np
import os

cancer_list = {'pancreatic_cancer': 469, 'sarcoma-retroperitoneum': 411, 'sarcoma-trunk_extremities': 383}
file_path = '../../result/post_processing_result/'
unique_nodes_df = pd.read_csv(file_path+'unique_nodes.csv')

# cancer = 'pancreatic_cancer'
# pre_node = {2: 'G89.2', 3: 'D64.9'}
# post_node = {2: 'D70.1', 3: 'D70.1'}

# cancer = 'sarcoma-retroperitoneum'
# pre_node = {2: 'E86.0', 3: 'E86.0'}
# post_node = {2: 'G89.3', 3: 'D70.1'}

cancer = 'sarcoma-trunk_extremities'
pre_node = {2: 'M79.6', 3: 'M79.8'}
post_node = {2: 'G89.3', 3: 'C78.0'}

for i in range(2, 4):
    file_name = file_path + cancer + '/' + str(i) + '_hop/' + '/pre_' + str(i) + '_hop_edge.csv'
    if not os.path.exists(file_name):
        continue
    original_df = pd.read_csv(file_name)
    df = original_df[original_df['d'+str(i)]==pre_node[i]]

    new_df = pd.DataFrame()
    hop_number = i
    for j in range(1, hop_number+1):
        cur_column = 'd' + str(j)
        next_column = 'd' + str(j+1)
        temp = df[[cur_column, next_column]]
        temp.rename(columns={cur_column: 'Source', next_column: 'Target'},
                  inplace=True)
        new_df = pd.concat([new_df, temp], axis=0, ignore_index=True)
        new_df.drop_duplicates(inplace=True)

    unique_node = set(new_df['Source'].unique().tolist())
    unique_node = unique_node.union(set(new_df['Target'].unique().tolist()))
    node_df = pd.DataFrame(unique_node, columns=['Id'])
    node_df = pd.merge(node_df, unique_nodes_df, how='left', on='Id')
    node_df.to_csv(file_path + cancer + '/' + str(i) + '_hop/' + '/pre_' + str(i) + '_node_gephi.csv', index=False)
    new_df.to_csv(file_path + cancer + '/' + str(i) + '_hop/' + '/pre_' + str(i) + '_edge_gephi.csv', index=False)

for i in range(2, 4):
    file_name = file_path + cancer + '/' + str(i) + '_hop/' + '/post_' + str(i) + '_hop_edge.csv'
    if not os.path.exists(file_name):
        continue
    original_df = pd.read_csv(file_name)
    df = original_df[original_df['d2']==post_node[i]]

    new_df = pd.DataFrame()
    hop_number = i
    for j in range(1, hop_number+1):
        cur_column = 'd' + str(j)
        next_column = 'd' + str(j+1)
        temp = df[[cur_column, next_column]]
        temp.rename(columns={cur_column: 'Source', next_column: 'Target'},
                  inplace=True)
        new_df = pd.concat([new_df, temp], axis=0, ignore_index=True)
        new_df.drop_duplicates(inplace=True)

    unique_node = set(new_df['Source'].unique().tolist())
    unique_node = unique_node.union(set(new_df['Target'].unique().tolist()))
    node_df = pd.DataFrame(unique_node, columns=['Id'])
    node_df = pd.merge(node_df, unique_nodes_df, how='left', on='Id')
    node_df.to_csv(file_path + cancer + '/' + str(i) + '_hop/' + '/post_' + str(i) + '_node_gephi.csv', index=False)
    new_df.to_csv(file_path + cancer + '/' + str(i) + '_hop/' + '/post_' + str(i) + '_edge_gephi.csv', index=False)



