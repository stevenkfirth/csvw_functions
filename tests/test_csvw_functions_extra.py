# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 14:49:13 2023

@author: cvskf
"""
import unittest

import csvw_functions
from csvw_functions import csvw_functions_extra



class EXTRA(unittest.TestCase):
    ""
    
    def _test_download_table(self):
        ""
        
        fp=r'C:\Users\cvskf\OneDrive - Loughborough University\_Git\building-energy\ogp_functions\ogp_functions\Local_Authority_District_to_Region_(December_2022)_Lookup_in_England.csv-schema-metadata.json'
        
        csvw_functions_extra.download_table(
            fp
            )
        
        d=csvw_functions.create_annotated_table_group(
            '_data/Local_Authority_District_to_Region_December_2022.csv-metadata.json'   
            )
        print(len(d))
        
        
    def test_download_table_group(self):
        ""
        
        fp=r'C:\Users\cvskf\OneDrive - Loughborough University\_Git\building-energy\ogp_functions\ogp_functions\ogp_tables-metadata.json'
        
        csvw_functions_extra.download_table_group(
            fp
            )
        
        
    def test_import_table_group_to_sqlite(self):
        ""
        
        csvw_functions_extra.import_table_group_to_sqlite(
            '_data/ogp_tables-metadata.json',
            #_reload_all_database_tables=True
            )
        
        
        
if __name__=="__main__":
    
    unittest.main()

