# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 06:11:59 2022

@author: cvskf
"""

import csvw_functions

import unittest
import os
import json

class TestSection1(unittest.TestCase):
    ""
    
    def test_Section1_3(self):
        ""
        
        os.chdir('csvw_primer_example_files')
        
        embedded_metadata_dict=csvw_functions.get_embedded_metadata(
            'countries.csv',
            relative_path=True
            )
        
        print(embedded_metadata_dict)
        
        
        with open('embedded_metadata_dict.json','w') as f:
            json.dump(embedded_metadata_dict,
                      f,
                      indent=4
                      )
        
        
        
if __name__=='__main__':
    
    unittest.main()
        