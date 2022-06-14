# -*- coding: utf-8 -*-
"""
Created on Sun Jun 12 06:59:51 2022

@author: cvskf
"""

import unittest
import os
import json

from csvw_functions import normalize_metadata_file


class TestNormalizeMetadata(unittest.TestCase):
    ""
    
    def test_normalize_metadata_example_41(self):
        ""
        metadata_file_url=r'metadata_vocabulary_example_files/example_41.json'
        result=normalize_metadata_file(metadata_file_url)
        
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             '@type': 'Table', 
             'url': 'http://example.com/table.csv', 
             'tableSchema': {}, 
             'dc:title': {'@value': 'The title of this Table', 
                          '@language': 'en'}}
            )
        
        
    def test_normalize_metadata_example_44(self):
        ""
        metadata_file_url=r'metadata_vocabulary_example_files/example_44.json'
        result=normalize_metadata_file(metadata_file_url)
        
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             '@type': 'Table', 
             'url': 'http://example.com/table.csv', 
             'tableSchema': {}, 
             'dc:title': [
                 {'@value': 'The title of this Table', '@language': 'en'}, 
                 {'@value': 'Der Titel dieser Tabelle', '@language': 'de'}
                 ]
             }
            )
        
        
    def test_normalize_metadata_example_46(self):
        ""
        metadata_file_url=r'metadata_vocabulary_example_files/example_46.json'
        result=normalize_metadata_file(metadata_file_url)
        
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             '@type': 'Table', 
             'url': 'http://example.com/table.csv', 
             'tableSchema': {}, 
             'schema:url': {'@id': 'http://example.com/table.csv'}
             }
            )
        
        
    def test_normalize_metadata_example_48(self):
        ""
        metadata_file_url=r'metadata_vocabulary_example_files/example_48.json'
        result=normalize_metadata_file(metadata_file_url)
        
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             '@type': 'Table', 
             'url': 'http://example.com/table.csv', 
             'tableSchema': {}, 
             'dc:publisher': [
                 {'schema:name': {'@value': 'Example Municipality'}, 
                  'schema:url': {'@id': 'http://example.org'}
                  }
                 ]
             }
            )
        
        
if __name__=='__main__':
    
    unittest.main()