# the input is one_hop_edges.csv
# this code is used to generate nodes.csv and edges.csv
# which can be further used to plot trajectory using gephi
# the nodes.csv will have Id, Label, Category

# the logic is to first get all the nodes for 3 cancers, and then get the name to be displayed.
# for each nodes.csv, use left join to get the label.
# set the node category based on edges.csv
# Group 1 in red represents the cancer of interest.
# Group 2 in blue represented nodes with a directed edge to the targeted cancer, indicating that the diagnosis shown by these nodes precedes the diagnosis of the targeted cancer.
# Group 3 in pink refers to nodes with a directed edge from the targeted cancer, indicating that the diagnosis shown by these nodes occurs after the cancer diagnosis.
# Group 4 in green shows nodes with an undirected edge to the targeted cancer, indicating that they are comorbidities of the cancer or the other diseases.
# Group 5 in yellow contains nodes that do not have any one-hop edge to the cancer.

import pandas as pd
import os
import numpy as np
from scipy.stats import binomtest
import csv

cancer_list = {'pancreatic_cancer': 469, 'sarcoma-retroperitoneum': 411, 'sarcoma-trunk_extremities': 383}
file_path = '../../result/post_processing_result/'

# # the following process get all the unique nodes and add their label by hand
# unique_nodes = set()
# for disease in cancer_list:
#     # read one_hop_edges.csv
#     df = pd.read_csv(file_path+disease+'/one_hop_edges.csv')
#     unique_nodes = unique_nodes.union(set(df['Source']))
#     unique_nodes = unique_nodes.union(set(df['Target']))
#
# unique_nodes_df = pd.DataFrame(list(unique_nodes), columns=['Id'])
# unique_nodes_df.to_csv(file_path+'/unique_nodes.csv', index=False, header=True)
# # add the label by hand

# read unique_nodes.csv
unique_nodes = pd.read_csv(file_path+'/unique_nodes.csv')
for disease in cancer_list:
    nodes = set()
    # read one_hop_edges.csv
    df = pd.read_csv(file_path+disease+'/one_hop_edges.csv')
    nodes = nodes.union(set(df['Source']))
    nodes = nodes.union(set(df['Target']))
    nodes_df = pd.DataFrame(list(nodes), columns=['Id'])
    # left join
    nodes_label_df = pd.merge(nodes_df, unique_nodes, how='left', on='Id')
    nodes_label_df['Category'] = None
    for idx, row in nodes_label_df.iterrows():
        if row['Id']==disease:
            nodes_label_df.loc[idx, 'Category'] = 1
        else:
            node = row['Id']
            filtered_df = df[(df['Source'] == node) & (df['Target'] == disease) & (df['Type'] == 'Directed')]
            if filtered_df.shape[0]>0:
                nodes_label_df.loc[idx, 'Category'] = 2
                continue

            filtered_df = df[(df['Source'] == disease) & (df['Target'] == node) & (df['Type'] == 'Directed')]
            if filtered_df.shape[0] > 0:
                nodes_label_df.loc[idx, 'Category'] = 3
                continue

            if node>disease:
                filtered_df = df[(df['Source'] == node) & (df['Target'] == disease) & (df['Type'] == 'Undirected')]
                if filtered_df.shape[0] > 0:
                    nodes_label_df.loc[idx, 'Category'] = 4
                    continue
            else:
                filtered_df = df[(df['Source'] == disease) & (df['Target'] == node) & (df['Type'] == 'Undirected')]
                if filtered_df.shape[0] > 0:
                    nodes_label_df.loc[idx, 'Category'] = 4
                    continue
    nodes_label_df['Category'] = nodes_label_df['Category'].fillna(5)
    nodes_label_df.to_csv(file_path + disease + '/nodes.csv', index=False, header=True)





