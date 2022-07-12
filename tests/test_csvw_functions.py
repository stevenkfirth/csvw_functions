# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 09:59:59 2022

@author: cvskf
"""

import unittest
import os
import json

from csvw_functions import csvw_functions

with open(os.path.join('csvw_primer_example_files','example_4.json')) as f:
     csvw_primer_example_4=json.load(f)     

with open(os.path.join('csvw_primer_example_files','example_5.json')) as f:
     csvw_primer_example_5=json.load(f)     
     
with open(os.path.join('metadata_vocabulary_example_files','example_41.json')) as f:
     metadata_vocabulary_example_41=json.load(f)     

with open(os.path.join('model_for_tabular_data_and_metadata_example_files','example_14.csv')) as f:
     model_for_tabular_data_and_metadata_example_14=f.read()

#import os
#print(os.listdir('../csvw_functions/schema_files'))


class TestReadSchemas(unittest.TestCase):
    ""
    
class TestIdentifySchemaProperties(unittest.TestCase):
    ""
    
class TestSchemaPrefixes(unittest.TestCase):
    ""

class TestNormalizeMetadata(unittest.TestCase):
    ""
    
    def test_normalize_metadata_from_file_path_example_41(self):
        ""
        metadata_file_url=r'metadata_vocabulary_example_files/example_41.json'
        result=csvw_functions.normalize_metadata_from_file_path(metadata_file_url)
        
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             '@type': 'Table', 
             'url': 'http://example.com/table.csv', 
             'tableSchema': {}, 
             'dc:title': {'@value': 'The title of this Table', 
                          '@language': 'en'}}
            )
        
        
    def test_normalize_metadata_from_file_path_example_44(self):
        ""
        metadata_file_url=r'metadata_vocabulary_example_files/example_44.json'
        result=csvw_functions.normalize_metadata_from_file_path(metadata_file_url)
        
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
        
        
    def test_normalize_metadata_from_file_path_example_46(self):
        ""
        metadata_file_path=r'metadata_vocabulary_example_files/example_46.json'
        result=csvw_functions.normalize_metadata_from_file_path(metadata_file_path)
        
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             '@type': 'Table', 
             'url': 'http://example.com/table.csv', 
             'tableSchema': {}, 
             'schema:url': {'@id': 'http://example.com/table.csv'}
             }
            )
        
        
    def test_normalize_metadata_from_file_path_example_48(self):
        ""
        metadata_file_url=r'metadata_vocabulary_example_files/example_48.json'
        result=csvw_functions.normalize_metadata_from_file_path(metadata_file_url)
        
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
        
        
class TestLocatingMetadata(unittest.TestCase):
    ""
    
    def test_get_embedded_metadata_from_csv_file(self):
        ""
        
        
        result=csvw_functions.get_embedded_metadata_from_csv_file(
            'https://raw.githubusercontent.com/stevenkfirth/csvw_functions/main/tests/model_for_tabular_data_and_metadata_example_files/example_14.csv'
            )
        
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             '@type': 'Table', 
             'url': 'https://raw.githubusercontent.com/stevenkfirth/csvw_functions/main/tests/model_for_tabular_data_and_metadata_example_files/example_14.csv', 
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
        
#%% FUNCTIONS - Model for Tabular Data and Metadata

#%% Section 6.1 - Creating Annotated Tables

class TestSection6_1(unittest.TestCase):
    ""
    
    
    def test_create_annotated_tables_from_csv_file_path_or_url(self):
        ""
        # example 14
        fp=r'model_for_tabular_data_and_metadata_example_files/example_14.csv'
        result=csvw_functions.create_annotated_tables_from_csv_file_path_or_url(
            fp
            )
        #print(result)
    
    
    def xtest_create_annotated_tables_from_metadata_file_url(self):
        ""
        # example 15
        url='https://raw.githubusercontent.com/stevenkfirth/csvw_functions/main/tests/model_for_tabular_data_and_metadata_example_files/example_15.json'
        result=csvw_functions.create_annotated_tables_from_metadata_file_url(
            url
            )
        
        # check first column
        self.assertEqual(
            result['tables'][0]['columns'][0],
            {'table': result['tables'][0], 
             'number': 1, 
             'sourceNumber': 1, 
             'name': None, 
             'titles': {'und': ['GID']}, 
             'virtual': False, 
             'suppressOutput': False, 
             'datatype': 'string', 
             'default': '', 
             'lang': 'und', 
             'null': '', 
             'ordered': False, 
             'required': False, 
             'separator': None, 
             'textDirection': 'auto', 
             'aboutURL': None, 
             'propertyURL': None, 
             'valueURL': None, 
             'cells': [
                 {'table': result['tables'][0], 
                  'column': result['tables'][0]['columns'][0], 
                  'row': result['tables'][0]['rows'][0], 
                  'stringValue': '1', 
                  'value': '1', 
                  'errors': [], 
                  'textDirection': 'auto', 
                  'ordered': False, 
                  'aboutURL': None, 
                  'propertyURL': None, 
                  'valueURL': None}, 
                 {'table': result['tables'][0], 
                  'column': result['tables'][0]['columns'][0], 
                  'row': result['tables'][0]['rows'][1], 
                  'stringValue': '2', 
                  'value': '2', 
                  'errors': [], 
                  'textDirection': 'auto', 
                  'ordered': False, 
                  'aboutURL': None, 
                  'propertyURL': None, 
                  'valueURL': None}
                 ]
             }
            )
        
        # check first row
        self.assertEqual(
            result['tables'][0]['rows'][0],
            {'table': result['tables'][0], 
             'number': 1, 
             'sourceNumber': 2, 
             'primaryKey': [], 
             'referencedRows': [], 
             'cells': [
                 {'table': result['tables'][0], 
                  'column': result['tables'][0]['columns'][0], 
                  'row': result['tables'][0]['rows'][0], 
                  'stringValue': '1', 'value': '1', 'errors': [], 
                  'textDirection': 'auto', 'ordered': False, 'aboutURL': None, 
                  'propertyURL': None, 'valueURL': None}, 
                 {'table': result['tables'][0], 
                  'column': result['tables'][0]['columns'][1], 
                  'row': result['tables'][0]['rows'][0], 
                  'stringValue': 'ADDISON AV', 'value': 'ADDISON AV', 'errors': [], 
                  'textDirection': 'auto', 'ordered': False, 'aboutURL': None, 
                  'propertyURL': None, 'valueURL': None}, 
                 {'table': result['tables'][0], 
                  'column': result['tables'][0]['columns'][2], 
                  'row': result['tables'][0]['rows'][0], 
                  'stringValue': 'Celtis australis', 'value': 'Celtis australis', 
                  'errors': [], 
                  'textDirection': 'auto', 'ordered': False, 'aboutURL': None, 
                  'propertyURL': None, 'valueURL': None}, 
                 {'table': result['tables'][0], 
                  'column': result['tables'][0]['columns'][3], 
                  'row': result['tables'][0]['rows'][0], 
                  'stringValue': 'Large Tree Routine Prune', 
                  'value': 'Large Tree Routine Prune', 'errors': [], 
                  'textDirection': 'auto', 'ordered': False, 'aboutURL': None, 
                  'propertyURL': None, 'valueURL': None}, 
                 {'table': result['tables'][0], 
                  'column': result['tables'][0]['columns'][4], 
                  'row': result['tables'][0]['rows'][0], 
                  'stringValue': '10/18/2010', 'value': '10/18/2010', 'errors': [], 
                  'textDirection': 'auto', 'ordered': False, 'aboutURL': None, 
                  'propertyURL': None, 'valueURL': None}
                 ]
             }
            )
        
        # check first cell
        self.assertEqual(
            result['tables'][0]['columns'][0]['cells'][0],
            {'table': result['tables'][0], 
             'column': result['tables'][0]['columns'][0], 
             'row': result['tables'][0]['rows'][0], 
             'stringValue': '1', 
             'value': '1', 
             'errors': [], 
             'textDirection': 'auto', 
             'ordered': False, 
             'aboutURL': None, 
             'propertyURL': None, 
             'valueURL': None}
            )
        
        
        
        print(result['tables'][0]['id'])


#%% FUNCTIONS - General

class TestGeneralFunctions(unittest.TestCase):
    ""
    
    def test_get_type_of_metadata_object(self):
        ""
        
        result=csvw_functions.get_type_of_metadata_object(csvw_primer_example_4)
        self.assertEqual(result,
                         'Table')
        
        result=csvw_functions.get_type_of_metadata_object(csvw_primer_example_5)
        self.assertEqual(result,
                         'TableGroup')
        
        
    def test_add_types_to_metadata(self):
        ""
        
        result=csvw_functions.add_types_to_metadata(csvw_primer_example_4)
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 'url': 'countries.csv', '@type': 'Table'}
            )
        
        result=csvw_functions.add_types_to_metadata(csvw_primer_example_5)
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
        
        result=csvw_functions.get_common_properties_of_metadata_object(metadata_vocabulary_example_41)
        self.assertEqual(
            result,
            ['dc:title']
            )
        
        
    
    
        
    def test_get_inherited_properties_from_type(self):
        ""
        
        result=csvw_functions.get_inherited_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['aboutUrl', 'datatype', 'default', 'lang', 'null', 'ordered', 
             'propertyUrl', 'required', 'separator', 'textDirection', 'valueUrl']
            )
        
        
    def test_get_optional_properties_from_type(self):
        ""
        
        result=csvw_functions.get_optional_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['dialect', 'notes', 'tableDirection', 'tableSchema', 
             'transformations', '@id', '@type']
            )
        
    def test_get_required_properties_from_type(self):
        ""
        
        result=csvw_functions.get_required_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['tables']
            )
        
    
    def test_get_schema_from_schema_name(self):
        ""
        
        result=csvw_functions.get_schema_from_schema_name(
            'table_description.schema.json'
            )
        self.assertIsInstance(
            result,
            dict)
        
        
    def test_get_schema_name_from_type(self):
        ""
        result=csvw_functions.get_schema_name_from_type('TableGroup')
        self.assertEqual(
            result,
            'table_group_description.schema.json'
            )
        
    def test_get_text_and_headers_from_file_url(self):
        ""
        url='https://raw.githubusercontent.com/stevenkfirth/csvw_functions/main/tests/model_for_tabular_data_and_metadata_example_files/example_15.json'
        text,headers=csvw_functions.get_text_and_headers_from_file_url(url)
        #print(text); print(headers)
        
        # headers as recorded on a request made on 15 June 2022
            # {'Connection': 'keep-alive', 
            #  'Content-Length': '188', 
            #  'Cache-Control': 'max-age=300', 
            #  'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; sandbox", 
            #  'Content-Type': 'text/plain; charset=utf-8', 
            #  'ETag': 'W/"fcbe8a17772c280cc399fcaf8dbe8fa3ba658b66113125ec4e63528239fbafdb"', 
            #  'Strict-Transport-Security': 'max-age=31536000', 
            #  'X-Content-Type-Options': 'nosniff', 
            #  'X-Frame-Options': 'deny', 
            #  'X-XSS-Protection': '1; mode=block', 
            #  'X-GitHub-Request-Id': '0B9C:D78F:202FFE:22FD4A:62A998AF', 
            #  'Content-Encoding': 'gzip', 
            #  'Accept-Ranges': 'bytes', 
            #  'Date': 'Wed, 15 Jun 2022 08:36:05 GMT', 
            #  'Via': '1.1 varnish', 
            #  'X-Served-By': 'cache-lon4276-LON', 
            #  'X-Cache': 'MISS', 
            #  'X-Cache-Hits': '0', 
            #  'X-Timer': 'S1655282165.972155,VS0,VE151', 
            #  'Vary': 'Authorization,Accept-Encoding,Origin', 
            #  'Access-Control-Allow-Origin': '*', 
            #  'X-Fastly-Request-ID': 'edc6441601ccddd36b182976cfd64bb7bc05f52a', 
            #  'Expires': 'Wed, 15 Jun 2022 08:41:05 GMT', 
            #  'Source-Age': '0'}
        
        
    def test_get_top_level_properties_from_type(self):
        ""
        
        result=csvw_functions.get_top_level_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['@context']
            )
      
        

        
if __name__=='__main__':
    
    unittest.main()