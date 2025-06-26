# this code is used to validate the multi-hop trajectories
import pandas as pd
import numpy as np
import os

patient_number = {'pancreatic_cancer': 2568, 'sarcoma-retroperitoneum': 179,
                  'sarcoma-trunk_extremities': 860}

for cancer in patient_number.keys():
    print('**************** {} *************'.format(cancer))

    for i in range(1, 4):
        print('-----------------')
        print('{} hop:'.format(i))
        file_path = '../data/'+cancer+'/'+str(i)+'_hop/'

        pre_file = file_path+'pre_'+str(i)+'_hop.csv' if i>1 else file_path+str(cancer)+'.csv'
        if os.path.exists(pre_file):
            if i==1:
                columns = ['d1', 'd2', 'ut_num']
                merge_columns = ['d1', 'd2']
                pre_file_ori = '../../result/post_processing_result/' + cancer + '/' + str(i) + '_hop/one_hop_edges.csv'

            elif i==2:
                columns = ['d1','d2','d3','ut_num']
                merge_columns = ['d1','d2','d3']
                pre_file_ori = '../../result/post_processing_result/' + cancer + '/' + str(i) + '_hop/pre_' + str(
                    i) + '_hop_edge.csv'
            else:
                columns = ['d1', 'd2', 'd3', 'd4', 'ut_num']
                merge_columns = ['d1', 'd2', 'd3', 'd4']
                pre_file_ori = '../../result/post_processing_result/' + cancer + '/' + str(i) + '_hop/pre_' + str(
                    i) + '_hop_edge.csv'

            df = pd.read_csv(pre_file, header=None)
            df.columns = columns

            ori_df = pd.read_csv(pre_file_ori)

            df_merge = pd.merge(ori_df, df, how='left',
                                on=merge_columns)
            non_nan_count = df_merge['ut_num'].notna().sum()

            # save merged result containing which trajectory is covered by data from UT
            df_merge['validated'] = ~df_merge['ut_num'].isna()
            df_merge = df_merge.drop(columns=['ut_num'])
            pre_file_new = '../../result/post_processing_result/' + cancer + '/' + str(i) + '_hop/new_pre_' + str(
                i) + '_hop_edge.csv'
            df_merge.to_csv(pre_file_new, index=False)
            print('pre edge count: {}/{}'.format(non_nan_count, ori_df.shape[0]))

        post_file = file_path + 'post_' + str(i) + '_hop.csv' if i>1 else file_path+str(cancer)+'.csv'
        if os.path.exists(post_file):
            if i==1:
                continue
            if i == 2:
                columns = ['d1', 'd2', 'd3', 'ut_num']
                merge_columns = ['d1', 'd2', 'd3']
                post_file_ori = '../../result/post_processing_result/' + cancer + '/' + str(i) + '_hop/post_' + str(
                    i) + '_hop_edge.csv'

            else:
                columns = ['d1', 'd2', 'd3', 'd4', 'ut_num']
                merge_columns = ['d1', 'd2', 'd3', 'd4']
                post_file_ori = '../../result/post_processing_result/' + cancer + '/' + str(i) + '_hop/post_' + str(
                    i) + '_hop_edge.csv'

            df = pd.read_csv(post_file, header=None)
            df.columns = columns

            if os.path.exists(post_file_ori):
                ori_df = pd.read_csv(post_file_ori)

                df_merge = pd.merge(ori_df, df, how='left',
                                    on=merge_columns)
                non_nan_count = df_merge['ut_num'].notna().sum()

                df_merge['validated'] = ~df_merge['ut_num'].isna()
                df_merge = df_merge.drop(columns=['ut_num'])
                post_file_new = '../../result/post_processing_result/' + cancer + '/' + str(i) + '_hop/new_post_' + str(
                    i) + '_hop_edge.csv'
                df_merge.to_csv(post_file_new, index=False)
                print('post edge count: {}/{}'.format(non_nan_count, ori_df.shape[0]))
