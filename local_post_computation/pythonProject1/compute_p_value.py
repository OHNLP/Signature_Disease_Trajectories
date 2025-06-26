# this code is used for get the one hop trajectories.
# it first checks if the pair is significant by comparing P_e(D2|D1) and P_c(D2).
# if we have P_e(D2|D1)>P_c(D2), then we regard the pair (D1, D2) is significant.
# then if both pairs (D1, D2) and (D2, D1) are significant, we further need to decide
# whether there is a direction or D1 and D2 are just comorbidities.

import pandas as pd
import os
import numpy as np
from scipy.stats import binomtest
import csv

disease_pairs = {'pancreatic_cancer': 469, 'sarcoma-retroperitoneum': 411, 'sarcoma-trunk_extremities': 383}
parent_path = '../../result/iqvia_result/'
save_path = '../../result/post_processing_result/'

for disease in disease_pairs:
    pair_threshold = 0.05/disease_pairs[disease]

    # read matched_cohort_P_D2.csv to get P_c(D2)
    P_c_d2_df = pd.read_csv(parent_path+disease+'/matched_cohort_P_D2.csv',
                            skiprows=1, header=None, names=['d1', 'd2', 'P_c_d2'])
    # read within_cohort_distribution.csv to get P_e(D2|D1)
    P_e_d2_d1_df = pd.read_csv(parent_path+disease+'/within_cohort_distribution.csv', sep='\t')

    # perform left join, make sure d1/d2 in P_c_d2_df matches d1/d2 in P_e_d2_d1_df
    df = pd.merge(P_e_d2_d1_df, P_c_d2_df, on=['d1', 'd2'], how='left')
    df = df[['d1', 'd2', 'num_d1', 'num_d1_d2', 'P_c_d2']]

    def apply_binomtest(row):
        result = binomtest(k=int(row['num_d1_d2']), n=int(row['num_d1']), p=float(row['P_c_d2']), alternative='greater')
        return result.pvalue
    df['p_value'] = df.apply(apply_binomtest, axis=1)
    significant_pair_df = df[df['p_value'] < pair_threshold]
    significant_pair_df['conditional_probability'] = 1.0*significant_pair_df['num_d1_d2']/significant_pair_df['num_d1']

    # save dataframe
    os.makedirs(save_path+disease, exist_ok=True)
    significant_pair_df.to_csv(save_path+disease+'/pair_with_conditional_prob_p_value.csv', index=False, header=True)

    # read 1_hop
    result_df = pd.read_csv(save_path+disease + '/1_hop.csv')
    result_df[['d1', 'd1_name']] = result_df['d1_ICD_name'].str.split(':', n=1, expand=True)
    result_df[['d2', 'd2_name']] = result_df['d2_ICD_name'].str.split(':', n=1, expand=True)
    result = result_df.merge(significant_pair_df, how='left', on=['d1', 'd2'])

    result = result[['d1_ICD_name','d2_ICD_name','num_patient','p_value','conditional_probability','Type','validated']]
    result.to_excel(save_path + disease + '/1_hop_conditional_probability_p_value.xlsx', index=False,
                               header=True)
    print('0')



    # print('*************************')
    # print('cancer {}'.format(disease))
    # print('total number of pairs after pre-filtering: {}'.format(df.shape[0]))
    # print('total number of significant pairs: {}'.format(significant_pair_df.shape[0]))
    #
    # # check whether the significant pair is directed or comorbidities
    # swapped_df = significant_pair_df[['d1', 'd2']].copy()
    # swapped_df.columns = ['d2', 'd1']
    # merged_df = pd.merge(significant_pair_df, swapped_df, on=['d1', 'd2'], how='left', indicator=True)
    # single_directed_df = merged_df[merged_df['_merge'] == 'left_only']
    # single_directed_df = single_directed_df.drop(columns=['_merge'])
    #
    # double_swapped_df = significant_pair_df[['d1', 'd2', 'num_d1_d2']].copy()
    # double_swapped_df.columns = ['d2', 'd1', 'num_d1_d2_reverse']  # Swap 'd1' and 'd2' and rename 'n1' to 'n1_reverse'
    # double_merged_df = pd.merge(significant_pair_df, double_swapped_df, on=['d1', 'd2'], how='inner')
    # double_directed_df = double_merged_df[['d1', 'd2', 'num_d1_d2', 'num_d1_d2_reverse']]
    #
    # print('total number of single directed pairs: {}'.format(single_directed_df.shape[0]))
    # print('total number of double directed pairs: {}'.format(double_directed_df.shape[0]))
    #
    # # for the double directed df, check whether they are comorbidities
    # double_directed_df['num'] = double_directed_df['num_d1_d2'] + double_directed_df['num_d1_d2_reverse']
    #
    # # this function is used to compute whether the pair is directed or comorbidity
    # def apply_comorbidity_binomtest(row):
    #     result = binomtest(k=int(row['num_d1_d2']), n=int(row['num']), p=0.5, alternative='greater')
    #     result_reverse = binomtest(k=int(row['num_d1_d2_reverse']), n=int(row['num']), p=0.5, alternative='greater')
    #     if result.pvalue<0.05:
    #         return 1
    #     elif result_reverse.pvalue<0.05:
    #         return 2
    #     else:
    #         return 3
    #
    # double_directed_df['category'] = double_directed_df.apply(apply_comorbidity_binomtest, axis=1)
    # from_double_new_single_directed_df = double_directed_df[double_directed_df['category'] == 1]
    # from_double_new_comorbidity_df = double_directed_df[double_directed_df['category'] == 3]
    #
    # # concatenate all single directed links
    # final_single_directed_df = from_double_new_single_directed_df[['d1', 'd2', 'num_d1_d2']]
    # temp = single_directed_df[['d1', 'd2', 'num_d1_d2']]
    # final_single_directed_df = pd.concat([final_single_directed_df, temp])
    # final_single_directed_df['Type'] = 'Directed'
    # final_single_directed_df = final_single_directed_df.rename(columns={'d1': 'Source', 'd2': 'Target', 'num_d1_d2': 'Weight'})
    #
    # # concatenate all comorbidities
    # final_comorbidity_df = from_double_new_comorbidity_df[['d1', 'd2', 'num']]
    # final_comorbidity_df['Type'] = 'Undirected'
    # final_comorbidity_df = final_comorbidity_df.rename(columns={'d1': 'Source', 'd2': 'Target', 'num': 'Weight'})
    # final_comorbidity_df = final_comorbidity_df[final_comorbidity_df['Source'] > final_comorbidity_df['Target']]
    #
    # print('total number of directed edges: {}'.format(final_single_directed_df.shape[0]))
    # print('total number of double comorbidity edges: {}'.format(final_comorbidity_df.shape[0]))
    #
    # # concatenate all links
    # all_link_df = pd.concat([final_single_directed_df, final_comorbidity_df])
    #
    # # save dataframe
    # os.makedirs(save_path+disease, exist_ok=True)
    # all_link_df.to_csv(save_path+disease+'/one_hop_edges.csv', index=False, header=True)
    #
    #




