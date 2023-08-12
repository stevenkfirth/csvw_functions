# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 14:49:13 2023

@author: cvskf
"""
import unittest

from csvw_functions import csvw_functions_extra



class EXTRA(unittest.TestCase):
    ""
    
    def test_download_table(self):
        ""
        
        fp=r'C:\Users\cvskf\OneDrive - Loughborough University\_Git\building-energy\ogp_functions\ogp_functions\Local_Authority_District_to_Region_(December_2022)_Lookup_in_England.csv-schema-metadata.json'
        
        csvw_functions_extra.download_table(
            fp
            )
        
        
        
if __name__=="__main__":
    
    unittest.main()

