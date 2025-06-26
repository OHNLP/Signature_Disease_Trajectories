import pandas as pd
import os
import numpy as np
from scipy.stats import norm
from scipy.stats import binomtest
import csv

disease_number = {'pancreatic_cancer': 13606, 'sarcoma-retroperitoneum': 1003, 'sarcoma-trunk_extremities': 4536}
parent_path = '../../result/post_processing_result/'

for hop in range(3, 4):
    for disease in disease_number:
        file = '/' + str(hop) + '_hop'
        # for section in ['/post_']:
        # for section in ['/post_', '/pre_']:
        for section in ['/pre_']:
            temp = section + str(hop) + '_hop_edge.csv'
            file_path = parent_path + disease + file + temp
            if not os.path.exists(file_path):
                continue

            # d1,d2,d3,num_patient
            edge_patient_df = pd.read_csv(file_path, index_col=False)
            # Source,Target,Weight,Type
            base_edge_patient_df = pd.read_csv(parent_path + disease + '/1_hop/one_hop_edges.csv', index_col=False)

            if hop==2:
                pre_edge_patient_df = pd.read_csv(parent_path + disease + '/1_hop/one_hop_edges.csv', index_col=False)
                pre_edge_patient_df = pre_edge_patient_df.drop(columns=['Type'])
                pre_edge_patient_df = pre_edge_patient_df.rename(columns={'Source': 'd1', 'Target': 'd2', 'Weight': 'num_patient'})

            elif section=='/pre_' and hop==3:
                pre_file = '/' + str(hop) + '_hop'
                pre_temp = '/oddsRatio.csv'
                pre_file_path = parent_path + disease + pre_file + pre_temp
                pre_edge_patient_df = pd.read_csv(pre_file_path, sep='\t', index_col=False)


            else:
                pre_file = '/' + str(hop-1) + '_hop'
                pre_temp = section + str(hop-1) + '_hop_edge.csv'
                pre_file_path = parent_path + disease + pre_file + pre_temp
                pre_edge_patient_df = pd.read_csv(pre_file_path, index_col=False)
                # d1,d2,d3,num_patient

            # find Y
            if hop==2:
                merged = (
                    pd.merge(
                        edge_patient_df,
                        base_edge_patient_df,
                        how="left",
                        left_on=["d2", "d3"],  # columns in df1
                        right_on=["Source", "Target"],  # matching columns in df2
                        suffixes=("", "_df2")  # optional: distinguish overlapping names
                    )
                )
                merged['Y'] = merged['Weight']
                new_merged = (
                    pd.merge(
                        merged,
                        pre_edge_patient_df,
                        how="left",
                        left_on=["d1", "d2"],  # columns in df1
                        right_on=["d1", "d2"],  # matching columns in df2
                        suffixes=("", "_pre")  # optional: distinguish overlapping names
                    )
                )
                new_merged['W'] = new_merged['num_patient_pre']
                new_merged['X'] = new_merged['num_patient']
                new_merged = new_merged[["d1", "d2", "d3", 'X', 'W', 'Y']]
                new_merged['B'] = new_merged['Y']-new_merged['X']
                new_merged['C'] = new_merged['W']-new_merged['X']
                new_merged['D'] = disease_number[disease]-new_merged['W']-new_merged['Y']+new_merged['X']

                new_merged['odds_ratio'] = 1.0*(1.0*new_merged['X']/new_merged['B'])/(1.0*new_merged['C']/new_merged['D'])
                new_merged['se_log_or'] = np.sqrt(1.0/new_merged['X']+1.0/new_merged['B']+1.0/new_merged['C']+1.0/new_merged['D'])
                new_merged['log_or'] = np.log(1.0*new_merged['odds_ratio'])

                z = norm.ppf(0.975)
                new_merged['lower_log'] = new_merged['log_or'] - z * new_merged['se_log_or']
                new_merged['upper_log'] = new_merged['log_or'] + z * new_merged['se_log_or']

                new_merged['CI_lower'] = np.exp(1.0*new_merged['lower_log'])
                new_merged['CI_upper'] = np.exp(1.0*new_merged['upper_log'])

                temp = section + str(hop) + '_hop_edge_odds_ratio.csv'
                new_file_path = parent_path + disease + file + temp
                new_merged.to_csv(new_file_path)

            if hop == 3:
                merged = (
                    pd.merge(
                        edge_patient_df,
                        base_edge_patient_df,
                        how="left",
                        left_on=["d3", "d4"],  # columns in df1
                        right_on=["Source", "Target"],  # matching columns in df2
                        suffixes=("", "_df2")  # optional: distinguish overlapping names
                    )
                )
                merged['Y'] = merged['Weight']
                new_merged = (
                    pd.merge(
                        merged,
                        pre_edge_patient_df,
                        how="left",
                        left_on=["d1", "d2", "d3"],  # columns in df1
                        right_on=["d1", "d2", "d3"],  # matching columns in df2
                        suffixes=("", "_pre")  # optional: distinguish overlapping names
                    )
                )
                new_merged['W'] = new_merged['num_patient_pre']
                new_merged['X'] = new_merged['num_patient']
                new_merged = new_merged[["d1", "d2", "d3", 'd4', 'X', 'W', 'Y']]
                new_merged['B'] = new_merged['Y'] - new_merged['X']
                new_merged['C'] = new_merged['W'] - new_merged['X']
                new_merged['D'] = disease_number[disease] - new_merged['W'] - new_merged['Y'] + new_merged['X']

                new_merged['odds_ratio'] = 1.0 * (1.0 * new_merged['X'] / new_merged['B']) / (
                            1.0 * new_merged['C'] / new_merged['D'])
                new_merged['se_log_or'] = np.sqrt(
                    1.0 / new_merged['X'] + 1.0 / new_merged['B'] + 1.0 / new_merged['C'] + 1.0 / new_merged['D'])
                new_merged['log_or'] = np.log(1.0 * new_merged['odds_ratio'])

                z = norm.ppf(0.975)
                new_merged['lower_log'] = new_merged['log_or'] - z * new_merged['se_log_or']
                new_merged['upper_log'] = new_merged['log_or'] + z * new_merged['se_log_or']

                new_merged['CI_lower'] = np.exp(1.0 * new_merged['lower_log'])
                new_merged['CI_upper'] = np.exp(1.0 * new_merged['upper_log'])

                temp = section + str(hop) + '_hop_edge_odds_ratio.csv'
                new_file_path = parent_path + disease + file + temp
                new_merged.to_csv(new_file_path)
