# -*- coding: utf-8 -*-
"""
Created on Thu Sep  1 14:33:44 2022

@author: cvskf
"""

from csvw_functions import csvw_functions2

action_fp=r'C:\Users\cvskf\OneDrive - Loughborough University\_Data\Energy_Performance_Certificates\2021\all-domestic-certificates\domestic-E06000001-Hartlepool\certificates.csv'

annotated_table_group_dict=\
    csvw_functions2.create_annotated_table_group(
            action_fp
            )