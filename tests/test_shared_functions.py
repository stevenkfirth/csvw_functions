# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 09:59:59 2022

@author: cvskf
"""

import unittest
import os
import json

from csvw_functions import shared_functions

with open(os.path.join('csvw_primer_example_files','example_4.json')) as f:
     csvw_primer_example_4=json.load(f)     

with open(os.path.join('csvw_primer_example_files','example_5.json')) as f:
     csvw_primer_example_5=json.load(f)     
     
with open(os.path.join('metadata_vocabulary_example_files','example_41.json')) as f:
     metadata_vocabulary_example_41=json.load(f)     

#import os
#print(os.listdir('../csvw_functions/schema_files'))



class TestSharedFunctions(unittest.TestCase):
    ""
    
    def test_get_type_of_metadata_object(self):
        ""
        
        result=shared_functions.get_type_of_metadata_object(csvw_primer_example_4)
        self.assertEqual(result,
                         'Table')
        
        result=shared_functions.get_type_of_metadata_object(csvw_primer_example_5)
        self.assertEqual(result,
                         'TableGroup')
        
        
    def test_add_types_to_metadata(self):
        ""
        
        result=shared_functions.add_types_to_metadata(csvw_primer_example_4)
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 'url': 'countries.csv', '@type': 'Table'}
            )
        
        result=shared_functions.add_types_to_metadata(csvw_primer_example_5)
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             'tables': [{'url': 'countries.csv', '@type': 'Table'}, 
                        {'url': 'country-groups.csv', '@type': 'Table'}, 
                        {'url': 'unemployment.csv', '@type': 'Table'}], 
             '@type': 'TableGroup'}
            )
        
        
    def test_get_common_properties_of_metadata_object(self):
        ""
        
        result=shared_functions.get_common_properties_of_metadata_object(metadata_vocabulary_example_41)
        self.assertEqual(
            result,
            ['dc:title']
            )
        
        
    def test_get_embedded_metadata_from_csv_file(self):
        ""
        
        result=shared_functions.get_embedded_metadata_from_csv_file(
            r'model_for_tabular_data_and_metadata_example_files/example_14.csv'
            )
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             '@type': 'Table', 
             'url': '', 
             'tableSchema': {
                 'columns': [
                     {'titles': ['GID']}, 
                     {'titles': ['On Street']}, 
                     {'titles': ['Species']}, 
                     {'titles': ['Trim Cycle']}, 
                     {'titles': ['Inventory Date']}
                     ]
                 }
             }
            )
    
        
    def test_get_inherited_properties_from_type(self):
        ""
        
        result=shared_functions.get_inherited_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['aboutUrl', 'datatype', 'default', 'lang', 'null', 'ordered', 
             'propertyUrl', 'required', 'separator', 'textDirection', 'valueUrl']
            )
        
        
    def test_get_optional_properties_from_type(self):
        ""
        
        result=shared_functions.get_optional_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['dialect', 'notes', 'tableDirection', 'tableSchema', 
             'transformations', '@id', '@type']
            )
        
    def test_get_required_properties_from_type(self):
        ""
        
        result=shared_functions.get_required_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['tables']
            )
        
    
    def test_get_schema_from_schema_name(self):
        ""
        
        result=shared_functions.get_schema_from_schema_name(
            'table_description.schema.json'
            )
        self.assertIsInstance(
            result,
            dict)
        
        
    def test_get_schema_name_from_type(self):
        ""
        result=shared_functions.get_schema_name_from_type('TableGroup')
        self.assertEqual(
            result,
            'table_group_description.schema.json'
            )
        
        
    def test_get_top_level_properties_from_type(self):
        ""
        
        result=shared_functions.get_top_level_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['@context']
            )
        
        
if __name__=='__main__':
    
    unittest.main()