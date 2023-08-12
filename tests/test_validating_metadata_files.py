# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 10:49:10 2023

@author: cvskf
"""

# These tests are for a selection of different csvw metadata files
# with errors, to check the files can be successfully validated.

import csvw_functions

import unittest
import os
import json
import warnings

tg={
        '@context':'http://www.w3.org/ns/csvw',
        'tables':[
            {
                'url': 'temp.csv'
                }
            ]
    }

class Test_Validating_Table_Group(unittest.TestCase):
    ""
    
    
    def test_0_basic_valid_object(self):
        ""
        
        md=dict(tg)
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        csvw_functions.validate_table_group_metadata('temp.json')
        
    
        
    def test_1_no_context_property(self):
        ""
        
        md=dict(tg)
        md.pop('@context')
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_table_group_metadata('temp.json')
    
        #print(err.exception)  # Property "@context" is a required property.
        
        
    def test_2_invalid_context_property_string(self):
        ""
        
        md=dict(tg)
        md['@context']='None'
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_table_group_metadata('temp.json')
    
        #print(err.exception)   # @context property must be either the string "http://www.w3.org/ns/csvw" 
                                # or an array.
                                
    def test_3_context_property_valid_array(self):
        ""
        
        #
        md=dict(tg)
        md['@context']=['http://www.w3.org/ns/csvw',
                        {'@base':'temp.csv'}]
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        csvw_functions.validate_table_group_metadata('temp.json')
    
        #
        md=dict(tg)
        md['@context']=['http://www.w3.org/ns/csvw',
                        {'@language':'en'}]
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        csvw_functions.validate_table_group_metadata('temp.json')
        
        #
        md=dict(tg)
        md['@context']=['http://www.w3.org/ns/csvw',
                        {'@base':'temp.csv',
                         '@language':'en'}]
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        csvw_functions.validate_table_group_metadata('temp.json')
        
        
    def test_4_context_property_empty_array(self):
        ""
        
        md=dict(tg)
        md['@context']=[]
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_table_group_metadata('temp.json')
    
        #print(err.exception)   # If an array, property "@context" must of two items only. 
        
        
    def test_5_context_property_array_first_item_invalid(self):
        ""
        
        md=dict(tg)
        md['@context']=['abc',{}]
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_table_group_metadata('temp.json')
    
        # print(err.exception)    # If an array, the first item of property "@context" 
                                # must be the string "http://www.w3.org/ns/csvw".
        
        
    def test_5_context_property_array_second_item_invalid_type(self):
        ""
        
        md=dict(tg)
        md['@context']=['http://www.w3.org/ns/csvw','abc']
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_table_group_metadata('temp.json')
    
        #print(err.exception)    # If an array, the second item of property 
                                # "@context" must be an object.
                                
    def test_5_context_property_array_second_item_invalid_length(self):
        ""
        
        md=dict(tg)
        md['@context']=['http://www.w3.org/ns/csvw',{}]
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_table_group_metadata('temp.json')
    
        #print(err.exception)    # If an array, the second item of property 
                                # "@context" must be an object with either or 
                                # both of "@base" and "@language".
        
    def test_5_context_property_array_second_item_invalid_item(self):
        ""
        
        md=dict(tg)
        md['@context']=['http://www.w3.org/ns/csvw',{'abc':'def'}]
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_table_group_metadata('temp.json')
    
        #print(err.exception)    # If an array, the second item of property 
                                # "@context" can only contain properties "@base" and "@language". 
                                # Property "abc" is not valid.
        
    def test_5_context_property_array_base_item_invalid(self):
        ""
        
        md=dict(tg)
        md['@context']=['http://www.w3.org/ns/csvw',{'@base':0}]
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_table_group_metadata('temp.json')
    
        #print(err.exception)    # "@base" property must be a string (not <class 'int'>).
                            
        
        
        
        
        
    
class Test_Validating_Table(unittest.TestCase):
    ""
    
    def test_1_no_context_property(self):
        ""
        
        md={}
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_table_metadata('temp.json')
    
        #print(err.exception)  # Property "@context" is a required property.
        
        
    def test_2_invalid_context_property_string(self):
        ""
        
        md={
                '@context':'abc'
            }
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_table_metadata('temp.json')
    
        #print(err.exception)   # @context property must be either the string "http://www.w3.org/ns/csvw" 
                                # or an array.
        
    
class Test_Validating_Schema(unittest.TestCase):
    ""
    
    def test_1_no_context_property(self):
        ""
        
        md={}
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_schema_metadata('temp.json')
    
        #print(err.exception)  # Property "@context" is a required property.
        
        
    def test_2_invalid_context_property_string(self):
        ""
        
        md={
                '@context':'abc'
            }
    
        with open('temp.json','w') as f:
            json.dump(md,f,indent=4)
            
        with self.assertRaises(csvw_functions.CSVWError) as err:
            
            csvw_functions.validate_schema_metadata('temp.json')
    
        #print(err.exception)   # @context property must be either the string "http://www.w3.org/ns/csvw" 
                                # or an array.
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
        
    
    
if __name__=='__main__':
    
    warnings.filterwarnings("always",category=UserWarning)  # warnings always printed out
    
    def run_single_test(test_kls,test_name):
        ""
        suite = unittest.TestSuite()
        suite.addTest(test_kls(test_name))
        runner = unittest.TextTestRunner()
        runner.run(suite)
        
    unittest.main()

