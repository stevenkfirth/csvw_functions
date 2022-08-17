# -*- coding: utf-8 -*-
"""
Created on Mon Aug 15 06:48:49 2022

@author: cvskf
"""

from csvw_functions import csvw_functions2

import unittest
import os
import json
import datetime
from rdflib import Graph, Literal, URIRef, XSD
import csv

test_dir='_github_w3c_csvw_tests'


def compare_json(self,json1,json2):
    ""
    if isinstance(json1,dict):
        
        self.assertIsInstance(
            json2,
            dict,
            msg='not of type <dict>'
            )
        
        self.assertEqual(
            set(list(json1)),
            set(list(json2)),
            msg='dict keys are not the same'
            )
        
        for k in json1:
            
            compare_json(
                self,
                json1[k],
                json2[k]
                )
        
    elif isinstance(json1,list):
        
        self.assertIsInstance(
            json2,
            list,
            msg='not of type <list>'
            )
        
        self.assertEqual(
            len(json1),
            len(json2),
            msg='lists are not of the same length'
            )
        
        for i in range(len(json1)):
            
            compare_json(
                self,
                json1[i],
                json2[i]
                )
        
    else:
        
        self.assertEqual(json1,json2)



def rdf_test(
        self,
        test
        ):
    """
    """
    
    




class TestCSVWTestCases(unittest.TestCase):
    ""
    
    def test_json(self):
        ""
        
        with open(os.path.join(test_dir,'manifest-json.jsonld')) as f:
            
            manifest=json.load(f)
            
        # loop through json tests
        for entry in manifest['entries']:
            
            print('-manifest-entry',entry)
            
            action_fp=os.path.join(test_dir,entry['action'])
            
            result_fp=os.path.join(test_dir,entry['result'])
            
            annotated_table_group_dict=\
                csvw_functions2.create_annotated_table_group(
                        action_fp
                        )
                
            print(annotated_table_group_dict['tables'][0]['columns'][0]['aboutURL'])
            print(annotated_table_group_dict['tables'][0]['columns'][0]['cells'][0]['aboutURL'])
                
            #
            if entry['option'].get('miminal') is True:
                mode='minimal'
            else:
                mode='standard'
            
            #
            json_ld=\
                csvw_functions2.create_json_ld(
                        annotated_table_group_dict,
                        mode=mode,
                        _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{entry["action"]}'
                        )    
                        
            print('-json_ld',json_ld)
                  
            with open(result_fp) as f:
                json_ld_result=json.load(f)
            print('-json_ld_result',json_ld_result)
            
            self.maxDiff=None
            if not json_ld==json_ld_result:
                compare_json(
                    self,
                    json_ld,
                    json_ld_result
                    )
            
            self.assertEqual(
                json_ld,
                json_ld_result
                )
            
            print('---------------------------')
            #break
        
            
                
        
        
        
        
        









if __name__=='__main__':
    
    unittest.main()



