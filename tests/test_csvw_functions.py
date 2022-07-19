# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 09:59:59 2022

@author: cvskf
"""

import unittest
import os
import json
import datetime

import logging
logging.basicConfig(
    filename='example.log', 
    encoding='utf-8', 
    level=logging.DEBUG,
    )

from csvw_functions import csvw_functions

with open(os.path.join('csvw_primer_example_files','example_4.json')) as f:
     csvw_primer_example_4=json.load(f)     

with open(os.path.join('csvw_primer_example_files','example_5.json')) as f:
     csvw_primer_example_5=json.load(f)     
     
with open(os.path.join('metadata_vocabulary_example_files','example_41.json')) as f:
     metadata_vocabulary_example_41=json.load(f)     

with open(os.path.join('model_for_tabular_data_and_metadata_example_files','example_14.csv')) as f:
     model_for_tabular_data_and_metadata_example_14=f.read()


    
    
#%% TESTS - Top Level Functions

class TestTopLevelFunctions(unittest.TestCase):
    ""
    
    def test_get_embedded_metadata_from_csv(self):
        ""
        logging.info('TEST: test_get_embedded_metadata_from_csv')
        
        # example 14
        fp=r'model_for_tabular_data_and_metadata_example_files/example_14.csv'
        result=csvw_functions.get_embedded_metadata_from_csv(
            fp
            )
        result['url']=os.path.basename(result['url'])
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             'tableSchema': {
                 'columns': [
                     {'titles': {'und': ['GID']}, 
                      '@type': 'Column'}, 
                     {'titles': {'und': ['On Street']}, 
                      '@type': 'Column'}, 
                     {'titles': {'und': ['Species']}, 
                      '@type': 'Column'}, 
                     {'titles': {'und': ['Trim Cycle']}, 
                      '@type': 'Column'}, 
                     {'titles': {'und': ['Inventory Date']}, 
                      '@type': 'Column'}], 
                 '@type': 'Table'}, 
             'url': 'example_14.csv'}
            )
        
        # example 21 - default dialect
        fp=r'model_for_tabular_data_and_metadata_example_files/example_21.csv'
        result=csvw_functions.get_embedded_metadata_from_csv(
            fp
            )
        result['url']=os.path.basename(result['url'])
        #print(result)
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             'tableSchema': {
                 'columns': [
                     {'titles': {'und': ['#\tpublisher\tCity of Palo Alto']}, 
                      '@type': 'Column'}], 
                 '@type': 'Table'}, 
             'url': 'example_21.csv'}
            )
    
        # example 21 - custom dialect with flags
        fp=r'model_for_tabular_data_and_metadata_example_files/example_21.csv'
        result=csvw_functions.get_embedded_metadata_from_csv(
            fp,
            delimiter='\t',
            skip_rows=4,
            skip_columns=1,
            comment_prefix='#'
            )
        result['url']=os.path.basename(result['url'])
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             'rdfs:comment': [
                 {'@value': '\tpublisher\tCity of Palo Alto'}, 
                 {'@value': '\tupdated\t12/31/2010'}, 
                 {'@value': 'name\tGID\ton_street\tspecies\ttrim_cycle\tinventory_date'}, 
                 {'@value': 'datatype\tstring\tstring\tstring\tstring\tdate:M/D/YYYY'}
                 ], 
             'tableSchema': {
                 'columns': [
                     {'titles': {'und': ['GID']}, '@type': 'Column'}, 
                     {'titles': {'und': ['On Street']}, '@type': 'Column'}, 
                     {'titles': {'und': ['Species']}, '@type': 'Column'}, 
                     {'titles': {'und': ['Trim Cycle']}, '@type': 'Column'}, 
                     {'titles': {'und': ['Inventory Date']}, '@type': 'Column'}
                     ], 
                 '@type': 'Table'
                 }, 
             'url': 'example_21.csv'}
            )
 
    
    def test_validate_metadata(self):
        ""
        logging.info('TEST: test_validate_metadata')
        
        json_fp=r'model_for_tabular_data_and_metadata_example_files/example_17.json'
        
        csvw_functions.validate_metadata(json_fp)
    
    
    
#%% TESTS - Model for Tabular Data and Metadata


#%% Section 6.4 - Parsing Cells

class TestSection6_4_1_Parsing_Examples(unittest.TestCase):
    ""
    
    def test_section_6_4_1(self):
        ""
        logging.info('TEST: test_section_6_4_1')
        
        kwargs=dict(
            default='',
            lang='und',
            null=None,
            required=False,
            separator=None
            )
    
        # string
        self.assertEqual(
            csvw_functions.parse_cell(
                string_value='99',
                datatype=None,
                #p=True,
                **kwargs
                ),
            ({'@value': '99', 
              '@type': 'http://www.w3.org/2001/XMLSchema#string', 
              '@language': 'und'},
             [])
            )
        
        # integer - example 9
        self.assertEqual(
            csvw_functions.parse_cell(
                string_value='99',
                datatype='integer',
                #p=True,
                **kwargs
                ),
            ({'@value': 99, 
              '@type': 'http://www.w3.org/2001/XMLSchema#integer'},
             [])
            )
        
        # integer - string value is 'one'
        self.assertEqual(
            csvw_functions.parse_cell(
                string_value='one',
                datatype='integer',
                #p=True,
                **kwargs
                ),
            ({'@value': 'one', 
              '@type': 'http://www.w3.org/2001/XMLSchema#string'},
             ['Value "one" is not a valid integer'])
            )
        
        # integer - string value is '1.0'
        self.assertEqual(
            csvw_functions.parse_cell(
                string_value='1.0',
                datatype='integer',
                #p=True,
                **kwargs
                ),
            ({'@value': '1.0', 
              '@type': 'http://www.w3.org/2001/XMLSchema#string'},
             ['Value "1.0" not valid as it contains the decimalChar character "."'])
            )
    
        # example 10 - integer with null - string value is '5'
        self.assertEqual(
            csvw_functions.parse_cell(
                string_value='5',
                datatype={
                  "base": "integer",
                  "minimum": 1,
                  "maximum": 10
                },
                default='',
                lang='und',
                null=99,
                required=False,
                separator=None
                ),
            ({'@value': 5, 
              '@type': 'http://www.w3.org/2001/XMLSchema#integer'},
             [])
            )
        
        # example 10 - integer with null - string value is '99'
        self.assertEqual(
            csvw_functions.parse_cell(
                string_value='99',
                datatype={
                  "base": "integer",
                  "minimum": 1,
                  "maximum": 10
                },
                default='',
                lang='und',
                null='99',
                required=False,
                separator=None
                ),
            (None,
             [])
            )
        
        # example 11 - integer with default - string value is ''
        self.assertEqual(
            csvw_functions.parse_cell(
                string_value='',
                datatype={
                  "base": "integer",
                  "minimum": 1,
                  "maximum": 10
                },
                default=5,
                lang='und',
                null=None,
                required=False,
                separator=None
                ),
            ({'@value': 5, 
              '@type': 'http://www.w3.org/2001/XMLSchema#integer'},
             [])
            )
        
        # example 11 - integer with default - string value is ' '
        self.assertEqual(
            csvw_functions.parse_cell(
                string_value=' ',
                datatype={
                  "base": "integer",
                  "minimum": 1,
                  "maximum": 10
                },
                default=5,
                lang='und',
                null=None,
                required=False,
                separator=None
                ),
            ({'@value': 5, 
              '@type': 'http://www.w3.org/2001/XMLSchema#integer'},
             [])
            )
        
        # example 12 - sequence of values - string value is '1 5 7.0'
        self.assertEqual(
            csvw_functions.parse_cell(
                string_value='1 5 7.0',
                datatype={
                  "base": "integer",
                  "minimum": 1,
                  "maximum": 10
                },
                default=5,
                lang='und',
                null=None,
                required=False,
                separator=' '
                ),
            ([{'@value': 1, 
               '@type': 'http://www.w3.org/2001/XMLSchema#integer'},
              {'@value': 5, 
                 '@type': 'http://www.w3.org/2001/XMLSchema#integer'},
              {'@value': '7.0', 
                 '@type': 'http://www.w3.org/2001/XMLSchema#string'}],
             ['Value "7.0" not valid as it contains the decimalChar character "."'])
            )
        
        # example 12 - sequence of values - string value is ''
        self.assertEqual(
            csvw_functions.parse_cell(
                string_value='',
                datatype={
                  "base": "integer",
                  "minimum": 1,
                  "maximum": 10
                },
                default='5',
                lang='und',
                null=None,
                required=False,
                separator=' '
                ),
            ([],
             [])
            )
        
        # example 13
        # Not done as largely a repeat of metadata vocab example 12.
        # Also looks like a possible error with the solution
        # shoudl be "1.0,5.0,7.0" rather than "?values=1.0,5.0,7.0" ??
        
        
        
class TestSection6_4_2_Formats_for_numeric_type(unittest.TestCase):
    ""
    
    def xtest_section_6_4_2(self):
        ""
        
        # test values taken from here: http://www.unicode.org/reports/tr35/tr35-numbers.html#Number_Format_Patterns
        
        decimal_char=','
        group_char=' '
        
        string_value='1 234,57'
        pattern='#,##0.##'
        
        result=csvw_functions.parse_number(
            string_value,
            'number',
            dict(
                decimalChar=decimal_char,
                groupChar=group_char,
                pattern=pattern
                ),
            errors=[]
            )
        
        self.assertEqual(
            result,
            (1234.57, [])
            )
        
        
        
    def test_parse_number_pattern(self):
        ""
        logging.info('TEST: test_parse_number_patters')
    
        result=csvw_functions.parse_number_pattern(
            pattern='#,##0.##'
            )
        
        #print(result)
        
        
        

#%% Section 8 - Parsing Tabular Data

class TestSection8(unittest.TestCase):
    ""
    
    def test_section_8_2_1_Simple_Example(self):
        ""
        
        logging.info('TEST: test_section_8_2_1_Simple_example')
        
        # Example 8.2.1.
        fp=r'model_for_tabular_data_and_metadata_example_files/example_14.csv'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #--- check annotated columns---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_columns_list],
            [1,2,3,4,5]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_columns_list],
            [1,2,3,4,5]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_columns_list],
            [2,2,2,2,2]
            )
        # titles
        self.assertEqual(
            [x['titles'] for x in annotated_columns_list],
            [[{'@language': 'und', '@value': 'GID'}],
             [{'@language': 'und', '@value': 'On Street'}],
             [{'@language': 'und', '@value': 'Species'}],
             [{'@language': 'und', '@value': 'Trim Cycle'}],
             [{'@language': 'und', '@value': 'Inventory Date'}]]
            )
        
        #---check embedded metadata---
        # url
        self.assertEqual(
            os.path.basename(annotated_table_dict['url']),
            'example_14.csv'
            )
        
        # column titles
        #... see above
        
        #--- check annotated rows---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_rows_list],
            [1,2]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_rows_list],
            [2,3]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_rows_list],
            [5,5]
            )
        
        #---check annotated cells---
        #---first row---
        annotated_cells_list=annotated_rows_list[0]['cells']
        # column number
        self.assertEqual(
            [x['column']['number'] for x in annotated_cells_list],
            [1,2,3,4,5]
            )
        # row number
        self.assertEqual(
            [x['row']['number'] for x in annotated_cells_list],
            [1,1,1,1,1]
            )
        # string value
        self.assertEqual(
            [x['stringValue'] for x in annotated_cells_list],
            ['1', 'ADDISON AV', 'Celtis australis', 'Large Tree Routine Prune', '10/18/2010']
            )
        # value
        #print([x['value'] for x in annotated_cells_list])
        self.assertEqual(
            [x['value'] for x in annotated_cells_list],
            [{'@value': '1', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'ADDISON AV', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Celtis australis', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '10/18/2010', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}
             ]
            )
        #---second row---
        annotated_cells_list=annotated_rows_list[1]['cells']
        # column number
        self.assertEqual(
            [x['column']['number'] for x in annotated_cells_list],
            [1,2,3,4,5]
            )
        # row number
        self.assertEqual(
            [x['row']['number'] for x in annotated_cells_list],
            [2,2,2,2,2]
            )
        # string value
        self.assertEqual(
            [x['stringValue'] for x in annotated_cells_list],
            ['2', 'EMERSON ST', 'Liquidambar styraciflua', 'Large Tree Routine Prune', '6/2/2010']
            )
        # value
        #print([x['value'] for x in annotated_cells_list])
        self.assertEqual(
            [x['value'] for x in annotated_cells_list],
            [{'@value': '2', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'EMERSON ST', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Liquidambar styraciflua', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '6/2/2010', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}
             ]
            )
        

    def test_section_8_2_1_1_Using_overriding_metadata(self):
        ""
        logging.info('TEST: test_section_8_2_1_1_Using_overriding_metadata')
        
        csv_fp=r'model_for_tabular_data_and_metadata_example_files/example_14.csv'
        json_fp=r'model_for_tabular_data_and_metadata_example_files/example_17.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                csv_file_path_or_url=csv_fp,
                overriding_metadata_file_path_or_url=json_fp
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #--check embedded metadata
        # titles
        self.assertEqual(
            [x['titles'] for x in annotated_columns_list],
            [[{'@language': 'und', '@value': 'GID'}],
             [{'@language': 'und', '@value': 'On Street'}],
             [{'@language': 'und', '@value': 'Species'}],
             [{'@language': 'und', '@value': 'Trim Cycle'}],
             [{'@language': 'und', '@value': 'Inventory Date'}]]
            )  
        # name
        self.assertEqual(
            [x['name'] for x in annotated_columns_list],
            ['GID', 'on_street', 'species', 'trim_cycle', 'inventory_date']
            )
        # datatype
        self.assertEqual(
            [x['datatype'] for x in annotated_columns_list],
            [{'base': 'string'}, 
             {'base': 'string'}, 
             {'base': 'string'}, 
             {'base': 'string'}, 
             {'base': 'date', 'format': 'M/d/yyyy'}]
            )
        
        #--- check annotated columns---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_columns_list],
            [1,2,3,4,5]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_columns_list],
            [1,2,3,4,5]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_columns_list],
            [2,2,2,2,2]
            )
        # name
        self.assertEqual(
            [x['name'] for x in annotated_columns_list],
            ['GID', 'on_street', 'species', 'trim_cycle', 'inventory_date']
            )
        # titles
        self.assertEqual(
            [x['titles'] for x in annotated_columns_list],
            [[{'@language': 'und', '@value': 'GID'}],
             [{'@language': 'und', '@value': 'On Street'}],
             [{'@language': 'und', '@value': 'Species'}],
             [{'@language': 'und', '@value': 'Trim Cycle'}],
             [{'@language': 'und', '@value': 'Inventory Date'}]]
            )
        # datatype
        self.assertEqual(
            [x['datatype'] for x in annotated_columns_list],
            [{'base': 'string'}, 
             {'base': 'string'}, 
             {'base': 'string'}, 
             {'base': 'string'}, 
             {'base': 'date', 'format': 'M/d/yyyy'}]
            )   


        #---check annotated cells---
        #---first row---
        annotated_cells_list=annotated_rows_list[0]['cells']
        # column number
        self.assertEqual(
            [x['column']['number'] for x in annotated_cells_list],
            [1,2,3,4,5]
            )
        # row number
        self.assertEqual(
            [x['row']['number'] for x in annotated_cells_list],
            [1,1,1,1,1]
            )
        # string value
        self.assertEqual(
            [x['stringValue'] for x in annotated_cells_list],
            ['1', 'ADDISON AV', 'Celtis australis', 'Large Tree Routine Prune', '10/18/2010']
            )
        # value
        #print([x['value'] for x in annotated_cells_list])
        self.assertEqual(
            [x['value'] for x in annotated_cells_list],
            [{'@value': '1', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'ADDISON AV', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Celtis australis', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '2010-10-18', '@type': 'http://www.w3.org/2001/XMLSchema#date'}
             ]
            )
        #---second row---
        annotated_cells_list=annotated_rows_list[1]['cells']
        # column number
        self.assertEqual(
            [x['column']['number'] for x in annotated_cells_list],
            [1,2,3,4,5]
            )
        # row number
        self.assertEqual(
            [x['row']['number'] for x in annotated_cells_list],
            [2,2,2,2,2]
            )
        # string value
        self.assertEqual(
            [x['stringValue'] for x in annotated_cells_list],
            ['2', 'EMERSON ST', 'Liquidambar styraciflua', 'Large Tree Routine Prune', '6/2/2010']
            )
        # value
        #print([x['value'] for x in annotated_cells_list])
        self.assertEqual(
            [x['value'] for x in annotated_cells_list],
            [{'@value': '2', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'EMERSON ST', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Liquidambar styraciflua', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '2010-06-02', '@type': 'http://www.w3.org/2001/XMLSchema#date'}
             ]
            )
        
    def test_section_8_2_1_2_Using_a_Metadata_File(self):
        ""
        logging.info('TEST: test_section_8_2_1_2_Using_a_Metadata_File')
        
        json_fp=r'model_for_tabular_data_and_metadata_example_files/example_19.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                metadata_file_path_or_url=json_fp
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']

        #---check annotated table---
        # dc:title
        self.assertEqual(
            annotated_table_dict['dc:title'],
            {'@value': 'Tree Operations', '@language': 'en'}
            )
        # dcat:keyword
        self.assertEqual(
            annotated_table_dict['dcat:keyword'],
            [{"@value": "tree", "@language": "en"}, 
             {"@value": "street", "@language": "en"}, 
             {"@value": "maintenance", "@language": "en"}]
            )
        # dc:publisher
        self.assertEqual(
            annotated_table_dict['dc:publisher'],
            {'schema:name': {'@value': 'Example Municipality', '@language': 'en'}, 
             'schema:url': {'@id': 'http://example.org'}}  # NOTE different to example solution
            )
        # dc:license
        self.assertEqual(
            annotated_table_dict['dc:license'],
            {"@id": "http://opendefinition.org/licenses/cc-by/"}
            )
        # dc:modified
        self.assertEqual(
            annotated_table_dict['dc:modified'],
            {"@value": "2010-12-31", "@type": "xsd:date"}  # NOTE different to example solution
            )

        #--- check annotated columns---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_columns_list],
            [1,2,3,4,5]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_columns_list],
            [1,2,3,4,5]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_columns_list],
            [2,2,2,2,2]
            )
        # name
        self.assertEqual(
            [x['name'] for x in annotated_columns_list],
            ['GID', 'on_street', 'species', 'trim_cycle', 'inventory_date']
            )
        # titles
        #print([x['titles'] for x in annotated_columns_list])
        self.assertEqual(
            [x['titles'] for x in annotated_columns_list],
            [[{'@language': 'en', '@value': 'GID'},
              {'@language': 'en', '@value': 'Generic Identifier'}],
             [{'@language': 'en', '@value': 'On Street'}],
             [{'@language': 'en', '@value': 'Species'}],
             [{'@language': 'en', '@value': 'Trim Cycle'}],
             [{'@language': 'en', '@value': 'Inventory Date'}]]
            )
        # datatype
        self.assertEqual(
            [x['datatype'] for x in annotated_columns_list],
            [{'base': 'string'}, 
             {'base': 'string'}, 
             {'base': 'string'}, 
             {'base': 'string'}, 
             {'base': 'date', 'format': 'M/d/yyyy'}]
            )   
        # dc:description
        #print([x['dc:description'] for x in annotated_columns_list])
        self.assertEqual(
            [x['dc:description'] for x in annotated_columns_list],
            [{'@value': 'An identifier for the operation on a tree.', '@language': 'en'}, 
             {'@value': 'The street that the tree is on.', '@language': 'en'}, 
             {'@value': 'The species of the tree.', '@language': 'en'}, 
             {'@value': 'The operation performed on the tree.', '@language': 'en'}, 
             {'@value': 'The date of the operation that was performed.', '@language': 'en'}]
            )
        
        #--- check annotated rows---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_rows_list],
            [1,2]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_rows_list],
            [2,3]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_rows_list],
            [5,5]
            )
        # primary key
        #print([x['primaryKey'][0]['column']['number'] for x in annotated_rows_list])
        #print([x['primaryKey'][0]['row']['number'] for x in annotated_rows_list])
        self.assertEqual(
            [x['primaryKey'] for x in annotated_rows_list],
            [[x['cells'][0]] for x in annotated_rows_list]
            )
        
        #---check annotated cells---
        #---first row---
        annotated_cells_list=annotated_rows_list[0]['cells']
        # column number
        self.assertEqual(
            [x['column']['number'] for x in annotated_cells_list],
            [1,2,3,4,5]
            )
        # row number
        self.assertEqual(
            [x['row']['number'] for x in annotated_cells_list],
            [1,1,1,1,1]
            )
        # string value
        self.assertEqual(
            [x['stringValue'] for x in annotated_cells_list],
            ['1', 'ADDISON AV', 'Celtis australis', 'Large Tree Routine Prune', '10/18/2010']
            )
        # value
        #print([x['value'] for x in annotated_cells_list])
        self.assertEqual(
            [x['value'] for x in annotated_cells_list],
            [{'@value': '1', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'ADDISON AV', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Celtis australis', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '2010-10-18', '@type': 'http://www.w3.org/2001/XMLSchema#date'}
             ]
            )
        # aboutURL
        #print([os.path.basename(x['aboutURL']) for x in annotated_cells_list])
        self.assertEqual(
            [os.path.basename(x['aboutURL']) for x in annotated_cells_list],  # hides the full path and checks on the filename only
            ['tree-ops.csv#gid-1', 
             'tree-ops.csv#gid-1', 
             'tree-ops.csv#gid-1', 
             'tree-ops.csv#gid-1', 
             'tree-ops.csv#gid-1']
            )
        
        #---second row---
        annotated_cells_list=annotated_rows_list[1]['cells']
        # column number
        self.assertEqual(
            [x['column']['number'] for x in annotated_cells_list],
            [1,2,3,4,5]
            )
        # row number
        self.assertEqual(
            [x['row']['number'] for x in annotated_cells_list],
            [2,2,2,2,2]
            )
        # string value
        self.assertEqual(
            [x['stringValue'] for x in annotated_cells_list],
            ['2', 'EMERSON ST', 'Liquidambar styraciflua', 'Large Tree Routine Prune', '6/2/2010']
            )
        # value
        #print([x['value'] for x in annotated_cells_list])
        self.assertEqual(
            [x['value'] for x in annotated_cells_list],
            [{'@value': '2', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'EMERSON ST', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Liquidambar styraciflua', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '2010-06-02', '@type': 'http://www.w3.org/2001/XMLSchema#date'}
             ]
            )
        # aboutURL
        #print([os.path.basename(x['aboutURL']) for x in annotated_cells_list])
        self.assertEqual(
            [os.path.basename(x['aboutURL']) for x in annotated_cells_list],  # hides the full path and checks on the filename only
            ['tree-ops.csv#gid-2', 
             'tree-ops.csv#gid-2', 
             'tree-ops.csv#gid-2', 
             'tree-ops.csv#gid-2', 
             'tree-ops.csv#gid-2']
            )
        
    def test_section_8_2_2_Empty_and_Quoted_Cells(self):
        ""
        logging.info('TEST: test_section_8_2_2_Empty_and_Quoted_Cells')
        
        fp=r'model_for_tabular_data_and_metadata_example_files/example_20.csv'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #---check annotated cells---
        #---first row---
        annotated_cells_list=annotated_rows_list[0]['cells']
        # column number
        self.assertEqual(
            [x['column']['number'] for x in annotated_cells_list],
            [1,2,3,4,5]
            )
        # row number
        self.assertEqual(
            [x['row']['number'] for x in annotated_cells_list],
            [1,1,1,1,1]
            )
        # string value
        self.assertEqual(
            [x['stringValue'] for x in annotated_cells_list],
            ['1', 'ADDISON AV', 'Celtis australis', 'Large Tree Routine Prune', '10/18/2010']
            )
        # value
        #print([x['value'] for x in annotated_cells_list])
        self.assertEqual(
            [x['value'] for x in annotated_cells_list],
            [{'@value': '1', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'ADDISON AV', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Celtis australis', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '10/18/2010', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}
             ]
            )
        #---second row---
        annotated_cells_list=annotated_rows_list[1]['cells']
        # column number
        self.assertEqual(
            [x['column']['number'] for x in annotated_cells_list],
            [1,2,3,4,5]
            )
        # row number
        self.assertEqual(
            [x['row']['number'] for x in annotated_cells_list],
            [2,2,2,2,2]
            )
        # string value
        self.assertEqual(
            [x['stringValue'] for x in annotated_cells_list],
            ['2', '', 'Liquidambar styraciflua', 'Large Tree Routine Prune', '']
            )
        # value
        #print([x['value'] for x in annotated_cells_list])
        self.assertEqual(
            [x['value'] for x in annotated_cells_list],
            [{'@value': '2', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             None, 
             {'@value': 'Liquidambar styraciflua', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             None]
            )

    def test_section_8_2_3_1_Naive_Parsing(self):
        ""
        logging.info('TEST: test_section_8_2_3_1_Naive_Parsing')
        
        fp=r'model_for_tabular_data_and_metadata_example_files/example_21.csv'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']

        #--- check annotated columns---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_columns_list],
            [1]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_columns_list],
            [1]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_columns_list],
            [6]
            )
        # titles
        self.assertEqual(
            [x['titles'] for x in annotated_columns_list],
            [
                [{'@language': 'und', '@value': '#\tpublisher\tCity of Palo Alto'}]
            ]
            )
        
        #--- check annotated rows---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_rows_list],
            [1,2,3,4,5,6]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_rows_list],
            [2,3,4,5,6,7]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_rows_list],
            [1,1,1,1,1,1]
            )
        
        #---check annotated cells---
        #---first row---
        annotated_cells_list=annotated_columns_list[0]['cells']
        # column number
        self.assertEqual(
            [x['column']['number'] for x in annotated_cells_list],
            [1,1,1,1,1,1]
            )
        # row number
        self.assertEqual(
            [x['row']['number'] for x in annotated_cells_list],
            [1,2,3,4,5,6]
            )
        # string value
        #print([x['stringValue'] for x in annotated_cells_list])
        self.assertEqual(
            [x['stringValue'] for x in annotated_cells_list],
            ['#\tupdated\t12/31/2010', 
             '#name\tGID\ton_street\tspecies\ttrim_cycle\tinventory_date', 
             '#datatype\tstring\tstring\tstring\tstring\tdate:M/D/YYYY', 
             'GID\tOn Street\tSpecies\tTrim Cycle\tInventory Date', 
             '1\tADDISON AV\tCeltis australis\tLarge Tree Routine Prune\t10/18/2010', 
             '2\tEMERSON ST\tLiquidambar styraciflua\tLarge Tree Routine Prune\t6/2/2010']
            )  # NOTE, example in standard seems to have tabs replaced with spaces??
        # value
        #print([x['value'] for x in annotated_cells_list])
        self.assertEqual(
            [x['value'] for x in annotated_cells_list],
            [{'@value': '#\tupdated\t12/31/2010', 
              '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '#name\tGID\ton_street\tspecies\ttrim_cycle\tinventory_date', 
              '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '#datatype\tstring\tstring\tstring\tstring\tdate:M/D/YYYY', 
              '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'GID\tOn Street\tSpecies\tTrim Cycle\tInventory Date', 
              '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '1\tADDISON AV\tCeltis australis\tLarge Tree Routine Prune\t10/18/2010', 
              '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '2\tEMERSON ST\tLiquidambar styraciflua\tLarge Tree Routine Prune\t6/2/2010', 
              '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}
             ]
            )  # NOTE, example in standard seems to have tabs replaced with spaces??
        
    
    def test_section_8_2_3_2_Parsing_with_Flags(self):
        ""
        logging.info('TEST: test_section_8_2_3_2_Parsing_with_Flags')
        
        fp=r'model_for_tabular_data_and_metadata_example_files/example_21.csv'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp,
                delimiter='\t',
                skip_rows=4,
                skip_columns=1,
                comment_prefix='#'
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
    
        #---check annotated table---
        # rdfs:comment
        #print(annotated_table_dict['rdfs:comment'])
        self.assertEqual(
            annotated_table_dict['rdfs:comment'],
            [{'@value': '\tpublisher\tCity of Palo Alto'}, 
             {'@value': '\tupdated\t12/31/2010'}, 
             {'@value': 'name\tGID\ton_street\tspecies\ttrim_cycle\tinventory_date'}, 
             {'@value': 'datatype\tstring\tstring\tstring\tstring\tdate:M/D/YYYY'}
             ]  # NOTE these values still contain the tabs but tabs are not 
                # shown in the example solution
            )
    
        #--- check annotated columns---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_columns_list],
            [1,2,3,4,5]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_columns_list],
            [2,3,4,5,6]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_columns_list],
            [2,2,2,2,2]
            )
        # titles
        self.assertEqual(
            [x['titles'] for x in annotated_columns_list],
            [[{'@language': 'und', '@value': 'GID'}],
             [{'@language': 'und', '@value': 'On Street'}],
             [{'@language': 'und', '@value': 'Species'}],
             [{'@language': 'und', '@value': 'Trim Cycle'}],
             [{'@language': 'und', '@value': 'Inventory Date'}]]
            )
        
        #--- check annotated rows---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_rows_list],
            [1,2]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_rows_list],
            [6,7]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_rows_list],
            [5,5]
            )
        
    def test_section_8_2_3_3_Recognizing_Tabular_Data_Formats(self):
        ""
        logging.info('TEST: test_section_8_2_3_3_Recognizing_Tabular_Data_Formats')
        
        fp_csv=r'model_for_tabular_data_and_metadata_example_files/example_21.csv'
        fp_json=r'model_for_tabular_data_and_metadata_example_files/example_23.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                csv_file_path_or_url=fp_csv,
                overriding_metadata_file_path_or_url=fp_json,
                delimiter='\t',
                skip_rows=4,
                skip_columns=1,
                comment_prefix='#'
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #---check annotated table---
        # dc:publisher
        self.assertEqual(
            annotated_table_dict['dc:publisher'],
            {'@value': 'City of Palo Alto'}
            )
        # dc@updated
        self.assertEqual(
            annotated_table_dict['dc:updated'],
            {'@value': '12/31/2010'}
            )
        
        #--- check annotated columns---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_columns_list],
            [1,2,3,4,5]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_columns_list],
            [2,3,4,5,6]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_columns_list],
            [2,2,2,2,2]
            )
        # name
        self.assertEqual(
            [x['name'] for x in annotated_columns_list],
            ['GID', 'on_street', 'species', 'trim_cycle', 'inventory_date']
            )
        # titles
        self.assertEqual(
            [x['titles'] for x in annotated_columns_list],
            [[{'@language': 'und', '@value': 'GID'}],
             [{'@language': 'und', '@value': 'On Street'}],
             [{'@language': 'und', '@value': 'Species'}],
             [{'@language': 'und', '@value': 'Trim Cycle'}],
             [{'@language': 'und', '@value': 'Inventory Date'}]]
            )
        
        #--- check annotated rows---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_rows_list],
            [1,2]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_rows_list],
            [6,7]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_rows_list],
            [5,5]
            )
        
        #---check annotated cells---
        #---first row---
        annotated_cells_list=annotated_rows_list[0]['cells']
        # column number
        self.assertEqual(
            [x['column']['number'] for x in annotated_cells_list],
            [1,2,3,4,5]
            )
        # row number
        self.assertEqual(
            [x['row']['number'] for x in annotated_cells_list],
            [1,1,1,1,1]
            )
        # string value
        self.assertEqual(
            [x['stringValue'] for x in annotated_cells_list],
            ['1', 'ADDISON AV', 'Celtis australis', 'Large Tree Routine Prune', '10/18/2010']
            )
        # value
        #print([x['value'] for x in annotated_cells_list])
        self.assertEqual(
            [x['value'] for x in annotated_cells_list],
            [{'@value': '1', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'ADDISON AV', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Celtis australis', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '2010-10-18', '@type': 'http://www.w3.org/2001/XMLSchema#date'}]
            )
        #---second row---
        annotated_cells_list=annotated_rows_list[1]['cells']
        # column number
        self.assertEqual(
            [x['column']['number'] for x in annotated_cells_list],
            [1,2,3,4,5]
            )
        # row number
        self.assertEqual(
            [x['row']['number'] for x in annotated_cells_list],
            [2,2,2,2,2]
            )
        # string value
        self.assertEqual(
            [x['stringValue'] for x in annotated_cells_list],
            ['2', 'EMERSON ST', 'Liquidambar styraciflua', 'Large Tree Routine Prune', '6/2/2010']
            )
        # value
        #print([x['value'] for x in annotated_cells_list])
        self.assertEqual(
            [x['value'] for x in annotated_cells_list],
            [{'@value': '2', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'EMERSON ST', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Liquidambar styraciflua', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '2010-06-02', '@type': 'http://www.w3.org/2001/XMLSchema#date'}
             ]
            )
        
        
    def test_section_8_2_4_Parsing_Multiple_Header_Lines(self):
        ""
        logging.info('TEST: test_section_8_2_4_Parsing_Multiple_Header_Lines')
        
        fp_csv=r'model_for_tabular_data_and_metadata_example_files/example_24.csv'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                csv_file_path_or_url=fp_csv,
                skip_rows=1,
                header_row_count=2,
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #--- check annotated columns---
        # number
        self.assertEqual(
            [x['number'] for x in annotated_columns_list],
            [1,2,3,4,5]
            )
        # source number
        self.assertEqual(
            [x['sourceNumber'] for x in annotated_columns_list],
            [1,2,3,4,5]
            )
        # number of cells
        self.assertEqual(
            [len(x['cells']) for x in annotated_columns_list],
            [2,2,2,2,2]
            )
        # titles
        #print([x['titles'] for x in annotated_columns_list])
        self.assertEqual(
            [x['titles'] for x in annotated_columns_list],
            [[{'@value': 'Organization', '@language': 'und'}, 
              {'@value': '#org', '@language': 'und'}], 
             [{'@value': 'Sector', '@language': 'und'}, 
              {'@value': '#sector', '@language': 'und'}], 
             [{'@value': 'Subsector', '@language': 'und'}, 
              {'@value': '#subsector', '@language': 'und'}], 
             [{'@value': 'Department', '@language': 'und'}, 
              {'@value': '#adm1', '@language': 'und'}], 
             [{'@value': 'Municipality', '@language': 'und'}, 
              {'@value': '#adm2', '@language': 'und'}]]
            )
        
        # embedded metadata
        embedded_metadata=csvw_functions.get_embedded_metadata_from_csv(
            fp_csv,
            skip_rows=1,
            header_row_count=2,
            )
        embedded_metadata['url']=os.path.basename(embedded_metadata['url'])
        
        self.assertEqual(
            embedded_metadata,
            {'@context': 'http://www.w3.org/ns/csvw', 
             'rdfs:comment': [{'@value': 'Who,What,,Where,'}], 
             'tableSchema': {
                 'columns': [
                     {'titles': {'und': ['Organization', '#org']}, '@type': 'Column'}, 
                     {'titles': {'und': ['Sector', '#sector']}, '@type': 'Column'}, 
                     {'titles': {'und': ['Subsector', '#subsector']}, '@type': 'Column'}, 
                     {'titles': {'und': ['Department', '#adm1']}, '@type': 'Column'}, 
                     {'titles': {'und': ['Municipality', '#adm2']}, '@type': 'Column'}
                     ],
                 '@type': 'Table'
                 },
             'url': 'example_24.csv'}
            )        
    
    
#%% TESTS - Metadata Vocabulary for Tabular Data
    
#%% Section 5.1.3 - URI Template Properties

class TestSection5_3_1(unittest.TestCase):
    ""
    
    def test_section_5_3_1_example_8(self):
        ""
        logging.info('TEST: test_section_5_3_1_example_8')
        
        json_fp=r'metadata_vocabulary_example_files/example_8.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                metadata_file_path_or_url=json_fp
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #---check annotated cells---
        #---first column---
        annotated_cells_list=annotated_columns_list[0]['cells']
        # aboutURL
        #print([x['aboutURL'] for x in annotated_cells_list])
        self.assertEqual(
            [x['aboutURL'] for x in annotated_cells_list],
            ['http://example.org/example.csv#row.1', 
             'http://example.org/example.csv#row.2']
            )
        
    def test_section_5_3_1_example_9(self):
        ""
        logging.info('TEST: test_section_5_3_1_example_9')
        
        json_fp=r'metadata_vocabulary_example_files/example_9.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                metadata_file_path_or_url=json_fp
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #---check annotated cells---
        #---first column---
        annotated_cells_list=annotated_columns_list[0]['cells']
        # aboutURL
        #print([x['aboutURL'] for x in annotated_cells_list])
        self.assertEqual(
            [x['aboutURL'] for x in annotated_cells_list],
            ['http://example.org/tree/ADDISON%20AV/1',
             'http://example.org/tree/EMERSON%20ST/2']
            )
        
        
    def test_section_5_3_1_example_10(self):
        ""
        logging.info('TEST: test_section_5_3_1_example_10')
        
        x=r'https://raw.githubusercontent.com/stevenkfirth/csvw_functions/main/tests/metadata_vocabulary_example_files/'
        json_url=x+'example_10.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                metadata_file_path_or_url=json_url
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #---check annotated cells---
        #---first column---
        annotated_cells_list=annotated_columns_list[0]['cells']
        # aboutURL
        #print([x['aboutURL'] for x in annotated_cells_list])
        self.assertEqual(
            [x['aboutURL'] for x in annotated_cells_list],
            [x+'tree-ops.csv#row.1',
             x+'tree-ops.csv#row.2']
            )
        
    def test_section_5_3_1_example_11(self):
        ""
        logging.info('TEST: test_section_5_3_1_example_11')
        
        json_fp=r'metadata_vocabulary_example_files/example_11.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                metadata_file_path_or_url=json_fp
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #---check annotated cells---
        #---first column---
        annotated_cells_list=annotated_columns_list[0]['cells']
        # aboutURL
        #print([os.path.basename(x['propertyURL']) for x in annotated_cells_list])
        self.assertEqual(
            [os.path.basename(x['propertyURL']) for x in annotated_cells_list],
            ['tree-ops.csv#GID', 'tree-ops.csv#GID']
            )
        
    def test_section_5_3_1_example_12(self):
        ""
        logging.info('TEST: test_section_5_3_1_example_12')
        
        json_fp=r'metadata_vocabulary_example_files/example_12.csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                metadata_file_path_or_url=json_fp
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #---check annotated cells---
        #---first row---
        annotated_cells_list=annotated_rows_list[0]['cells']
        # aboutURL
        self.assertEqual(
            annotated_cells_list[1]['valueURL'],
            'http://xmlns.com/foaf/0.1/Project'
            )
        self.assertEqual(
            annotated_cells_list[2]['valueURL'],
            'https://duckduckgo.com/?q=table,data,conversion'
            )
        
    def test_section_5_3_1_example_12b(self):
        ""
        logging.info('TEST: test_section_5_3_1_example_12b')
        
        json_fp=r'metadata_vocabulary_example_files/example_12b.csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                metadata_file_path_or_url=json_fp
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #---check annotated cells---
        #---first row---
        annotated_cells_list=annotated_rows_list[0]['cells']
        # aboutURL
        self.assertEqual(
            annotated_cells_list[1]['valueURL'],
            'http://xmlns.com/foaf/0.1/Project'
            )
        self.assertEqual(
            annotated_cells_list[2]['valueURL'],
            'https://duckduckgo.com/?q='
            )
    
        
    def test_section_5_3_1_example_13(self):
        ""
        logging.info('TEST: test_section_5_3_1_example_13')
        
        json_fp=r'metadata_vocabulary_example_files/example_13.csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                metadata_file_path_or_url=json_fp
                )
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        
        #---check annotated cells---
        #---first row---
        annotated_cells_list=annotated_rows_list[0]['cells']
        # aboutURL
        self.assertEqual(
            annotated_cells_list[0]['aboutURL'],
            'http://example.org/event/2010-10-18'
            )
        


#%% 6. Normalization

class TestSection6(unittest.TestCase):
    
    def test_section_6_1_example_41(self):
        ""
        logging.info('TEST: test_section_6_1_example_41')
        
        metadata_file_url=r'metadata_vocabulary_example_files/example_41.json'
        result=csvw_functions.normalize_metadata_from_file_path_or_url(metadata_file_url)[0]
        
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             '@type': 'Table', 
             'url': 'http://example.com/table.csv', 
             'tableSchema': {}, 
             'dc:title': {'@value': 'The title of this Table', 
                          '@language': 'en'}}
            )
        
        
    def test_section_6_1_example_44(self):
        ""
        logging.info('TEST: test_section_6_1_example_44')
        
        metadata_file_url=r'metadata_vocabulary_example_files/example_44.json'
        result=csvw_functions.normalize_metadata_from_file_path_or_url(metadata_file_url)[0]
        
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
        
        
    def test_section_6_1_example_46(self):
        ""
        logging.info('TEST: test_section_6_1_example_46')
        
        metadata_file_path=r'metadata_vocabulary_example_files/example_46.json'
        result=csvw_functions.normalize_metadata_from_file_path_or_url(metadata_file_path)[0]
        
        self.assertEqual(
            result,
            {'@context': 'http://www.w3.org/ns/csvw', 
             '@type': 'Table', 
             'url': 'http://example.com/table.csv', 
             'tableSchema': {}, 
             'schema:url': {'@id': 'http://example.com/table.csv'}
             }
            )
        
        
    def test_section_6_1_example_48(self):
        ""
        logging.info('TEST: test_section_6_1_example_48')
        
        metadata_file_url=r'metadata_vocabulary_example_files/example_48.json'
        result=csvw_functions.normalize_metadata_from_file_path_or_url(metadata_file_url)[0]
        
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
        

#%% TESTS - Generating JSON from Tabular Data on the Web


class TestSection6(unittest.TestCase):
    ""
    
    def test_section_6_1_simple_example(self):
        ""
        
        logging.info('TEST: test_section_6_1_simple_example')
        
        
        fp=r'generating_json_from_tabular_data_example_files/countries.csv'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(fp)
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        annotated_cells_list=[cell 
                              for row in annotated_rows_list 
                              for cell in row['cells']]
        
        #---check annotated table---
        # url
        self.assertEqual(
            os.path.basename(annotated_table_dict['url']),
            'countries.csv'
            )
        # number of columns
        self.assertEqual(
            len(annotated_table_dict['columns']),
            4
            )
        # number of rows
        self.assertEqual(
            len(annotated_table_dict['rows']),
            3
            )
        
        #---check annotated columns---
        # number
        self.assertEqual(
            [column['number'] for column in annotated_columns_list],
            [1,2,3,4]
            )
        # source number
        self.assertEqual(
            [column['sourceNumber'] for column in annotated_columns_list],
            [1,2,3,4]
            )
        # number of cells
        self.assertEqual(
            [len(column['cells']) for column in annotated_columns_list],
            [3,3,3,3]
            )
        # name
        self.assertEqual(
            [column['name'] for column in annotated_columns_list],
            ['countryCode', 'latitude', 'longitude', 'name']
            )
        # title
        self.assertEqual(
            [column['titles'] for column in annotated_columns_list],
            [[{'@value': 'countryCode', '@language': 'und'}], 
             [{'@value': 'latitude', '@language': 'und'}], 
             [{'@value': 'longitude', '@language': 'und'}], 
             [{'@value': 'name', '@language': 'und'}]]
            )
        
        #---check annotated rows---
        # number
        self.assertEqual(
            [row['number'] for row in annotated_rows_list],
            [1,2,3]
            )
        # source number
        self.assertEqual(
            [row['sourceNumber'] for row in annotated_rows_list],
            [2,3,4]
            )
        # number of cells
        self.assertEqual(
            [len(row['cells']) for row in annotated_rows_list],
            [4,4,4]
            )
        
        #---check annotated cells---
        # column number
        self.assertEqual(
            [cell['column']['number'] for cell in annotated_cells_list],
            [1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4]
            )
        # row number
        self.assertEqual(
            [cell['row']['number'] for cell in annotated_cells_list],
            [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]
            )
        # string value
        self.assertEqual(
            [cell['stringValue'] for cell in annotated_cells_list],
            ['AD', 
             '42.5', 
             '1.6', 
             'Andorra', 
             'AE', 
             '23.4', 
             '53.8', 
             'United Arab Emirates', 
             'AF', 
             '33.9', 
             '67.7', 
             'Afghanistan'
             ]
            )
        # value
        self.assertEqual(
            [cell['value'] for cell in annotated_cells_list],
            [{'@value': 'AD', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '42.5', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '1.6', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Andorra', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'AE', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '23.4', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '53.8', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'United Arab Emirates', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'AF', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '33.9', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '67.7', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Afghanistan', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}
             ]
            )
        # property url
        self.assertEqual(
            [cell['propertyURL'] for cell in annotated_cells_list],
            [None, None, None, None, None, None, None, None, None, None, None, None]
            )
        
        
        # minimal mode
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='minimal'
                    )
            
        #print(json_ld)
        
        self.assertEqual(
            json_ld,
            [{ 
              "countryCode": "AD", 
              "latitude": "42.5", 
              "longitude": "1.6", 
              "name": "Andorra" 
            }, { 
              "countryCode": "AE", 
              "latitude": "23.4", 
              "longitude": "53.8", 
              "name": "United Arab Emirates" 
            }, { 
              "countryCode": "AF", 
              "latitude": "33.9", 
              "longitude": "67.7", 
              "name": "Afghanistan" 
            }])
                
        # standard mode
        
        # TO DO
        
        
        
    def test_section_6_2_Example_with_single_table_and_rich_annotations(self):
        ""
        logging.info('TEST: test_section_6_2_Example_with_single_table_and_rich_annotations')
        
        fp=r'generating_json_from_tabular_data_example_files/tree-ops-ext.csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(fp)
        
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        annotated_cells_list=[cell 
                              for row in annotated_rows_list 
                              for cell in row['cells']]
        
        #---check annotated table---
        # id
        self.assertEqual(
            annotated_table_dict['id'],
            'http://example.org/tree-ops-ext'
            )
        # url
        self.assertEqual(
            os.path.basename(annotated_table_dict['url']),
            'tree-ops-ext.csv'
            )
        self.assertEqual(
            len(annotated_table_dict['columns']),
            9
            )
        self.assertEqual(
            len(annotated_table_dict['rows']),
            3
            )
        self.assertEqual(
            annotated_table_dict['notes'],
            [
                {'@type': 'oa:Annotation', 
                 'oa:hasTarget': {'@id': 'http://example.org/tree-ops-ext'}, 
                 'oa:hasBody': {'@type': 'oa:EmbeddedContent', 
                                'rdf:value': {
                                    '@value': "This is a very interesting comment about the table; it's a table!", 
                                    '@language': 'en'}, 
                                'dc:format': {'@value': 'text/plain'}
                                }
                 }
                ]
            )
        self.assertEqual(
            annotated_table_dict['dc:title'],
            {'@value': 'Tree Operations', '@language': 'en'}
            )
        self.assertEqual(
            annotated_table_dict['dcat:keyword'],
            [{'@value': 'tree', '@language': 'en'}, 
             {'@value': 'street', '@language': 'en'}, 
             {'@value': 'maintenance', '@language': 'en'}]
            )
        self.assertEqual(
            annotated_table_dict['dc:publisher'],
            [{'schema:name': {'@value': 'Example Municipality', '@language': 'en'}, 
              'schema:url': {'@id': 'http://example.org'}}]
            )
        self.assertEqual(
            annotated_table_dict['dc:license'],
            {'@id': 'http://opendefinition.org/licenses/cc-by/'}
            )
        self.assertEqual(
            annotated_table_dict['dc:modified'],
            {'@value': '2010-12-31', '@type': 'xsd:date'}
            )
        
        #---check annotated columns---
        # number
        self.assertEqual(
            [column['number'] for column in annotated_columns_list],
            [1,2,3,4,5,6,7,8,9]
            )
        # source number
        self.assertEqual(
            [column['sourceNumber'] for column in annotated_columns_list],
            [1,2,3,4,5,6,7,8,9]
            )
        # number of cells
        self.assertEqual(
            [len(column['cells']) for column in annotated_columns_list],
            [3,3,3,3,3,3,3,3,3]
            )
        # name
        self.assertEqual(
            [column['name'] for column in annotated_columns_list],
            ['GID', 
             'on_street', 
             'species', 
             'trim_cycle', 
             'dbh', 
             'inventory_date', 
             'comments', 
             'protected', 
             'kml']
            )
        # title
        self.assertEqual(
            [column['titles'] for column in annotated_columns_list],
            [[{'@value': 'GID', '@language': 'en'}, 
              {'@value': 'Generic Identifier', '@language': 'en'}], 
             [{'@value': 'On Street', '@language': 'en'}], 
             [{'@value': 'Species', '@language': 'en'}], 
             [{'@value': 'Trim Cycle', '@language': 'en'}], 
             [{'@value': 'Diameter at Breast Ht', '@language': 'en'}], 
             [{'@value': 'Inventory Date', '@language': 'en'}], 
             [{'@value': 'Comments', '@language': 'en'}], 
             [{'@value': 'Protected', '@language': 'en'}], 
             [{'@value': 'KML', '@language': 'en'}]
             ]
            )
        # required
        self.assertEqual(
            [column['required'] for column in annotated_columns_list],
            [True, False, False, False, False, False, False, False, False]
            )
        # suppress output
        self.assertEqual(
            [column['suppressOutput'] for column in annotated_columns_list],
            [True, False, False, False, False, False, False, False, False]
            )
        # dc:description
        self.assertEqual(
            [column['dc:description'] for column in annotated_columns_list],
            [{'@value': 'An identifier for the operation on a tree.', '@language': 'en'}, 
             {'@value': 'The street that the tree is on.', '@language': 'en'}, 
             {'@value': 'The species of the tree.', '@language': 'en'}, 
             {'@value': 'The operation performed on the tree.', '@language': 'en'}, 
             {'@value': 'Diameter at Breast Height (DBH) of the tree (in feet), measured 4.5ft above ground.', '@language': 'en'}, 
             {'@value': 'The date of the operation that was performed.', '@language': 'en'}, 
             {'@value': 'Supplementary comments relating to the operation or tree.', '@language': 'en'}, 
             {'@value': 'Indication (YES / NO) whether the tree is subject to a protection order.', '@language': 'en'}, 
             {'@value': 'KML-encoded description of tree location.', '@language': 'en'}
             ]
            )
        
        #---check annotated rows---
        # number
        self.assertEqual(
            [row['number'] for row in annotated_rows_list],
            [1,2,3]
            )
        # source number
        self.assertEqual(
            [row['sourceNumber'] for row in annotated_rows_list],
            [2,3,4]
            )
        # number of cells
        self.assertEqual(
            [len(row['cells']) for row in annotated_rows_list],
            [9,9,9]
            )
        # number of primary keys
        self.assertEqual(
            [len(row['primaryKey']) for row in annotated_rows_list],
            [1,1,1]
            )
        # row number of first primary key cell
        self.assertEqual(
            [row['primaryKey'][0]['row']['number'] for row in annotated_rows_list],
            [1,2,3]
            )
        # column number of first primary key cell
        self.assertEqual(
            [row['primaryKey'][0]['column']['number'] for row in annotated_rows_list],
            [1,1,1]
            )
        
        #---check annotated cells---
        # column number
        self.assertEqual(
            [cell['column']['number'] for cell in annotated_cells_list],
            [1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9]
            )
        # row number
        self.assertEqual(
            [cell['row']['number'] for cell in annotated_cells_list],
            [1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3]
            )
        # string value
        self.assertEqual(
            [cell['stringValue'] for cell in annotated_cells_list],
            ['1', 
             'ADDISON AV', 
             'Celtis australis', 
             'Large Tree Routine Prune', 
             '11', 
             '10/18/2010', 
             '', 
             '', 
             '<Point><coordinates>-122.156485,37.440963</coordinates></Point>', 
             '2', 
             'EMERSON ST', 
             'Liquidambar styraciflua', 
             'Large Tree Routine Prune', 
             '11', 
             '6/2/2010', 
             '', 
             '', 
             '<Point><coordinates>-122.156749,37.440958</coordinates></Point>', 
             '6', 
             'ADDISON AV', 
             'Robinia pseudoacacia', 
             'Large Tree Routine Prune', 
             '29', 
             '6/1/2010', 
             'cavity or decay; trunk decay; codominant leaders; included bark; large leader or limb decay; previous failure root damage; root decay;  beware of BEES', 
             'YES', 
             '<Point><coordinates>-122.156299,37.441151</coordinates></Point>'
             ]
            )
        # value
        self.assertEqual(
            [cell['value'] for cell in annotated_cells_list],
            [{'@value': '1', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'ADDISON AV', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Celtis australis', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'en'}, 
             {'@value': 11, '@type': 'http://www.w3.org/2001/XMLSchema#integer'}, 
             {'@value': '2010-10-18', '@type': 'http://www.w3.org/2001/XMLSchema#date'}, 
             [], 
             {'@value': False, '@type': 'http://www.w3.org/2001/XMLSchema#boolean'}, 
             {'@value': '<Point><coordinates>-122.156485,37.440963</coordinates></Point>', '@type': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral', '@language': 'und'}, 
             {'@value': '2', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'EMERSON ST', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Liquidambar styraciflua', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'en'}, 
             {'@value': 11, '@type': 'http://www.w3.org/2001/XMLSchema#integer'}, 
             {'@value': '2010-06-02', '@type': 'http://www.w3.org/2001/XMLSchema#date'}, 
             [], 
             {'@value': False, '@type': 'http://www.w3.org/2001/XMLSchema#boolean'}, 
             {'@value': '<Point><coordinates>-122.156749,37.440958</coordinates></Point>', '@type': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral', '@language': 'und'}, 
             {'@value': '6', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'ADDISON AV', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Robinia pseudoacacia', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'en'}, 
             {'@value': 29, '@type': 'http://www.w3.org/2001/XMLSchema#integer'}, 
             {'@value': '2010-06-01', '@type': 'http://www.w3.org/2001/XMLSchema#date'}, 
             [{'@value': 'cavity or decay', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
              {'@value': 'trunk decay', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
              {'@value': 'codominant leaders', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
              {'@value': 'included bark', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
              {'@value': 'large leader or limb decay', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
              {'@value': 'previous failure root damage', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
              {'@value': 'root decay', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
              {'@value': 'beware of BEES', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}
              ], 
             {'@value': True, '@type': 'http://www.w3.org/2001/XMLSchema#boolean'}, 
             {'@value': '<Point><coordinates>-122.156299,37.441151</coordinates></Point>', '@type': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral', '@language': 'und'}
             ]
            )
        # property url
        self.assertEqual(
            [cell['aboutURL'] for cell in annotated_cells_list],
            ['http://example.org/tree-ops-ext#gid-1', 
             'http://example.org/tree-ops-ext#gid-1', 
             'http://example.org/tree-ops-ext#gid-1', 
             'http://example.org/tree-ops-ext#gid-1', 
             'http://example.org/tree-ops-ext#gid-1', 
             'http://example.org/tree-ops-ext#gid-1', 
             'http://example.org/tree-ops-ext#gid-1', 
             'http://example.org/tree-ops-ext#gid-1', 
             'http://example.org/tree-ops-ext#gid-1', 
             'http://example.org/tree-ops-ext#gid-2', 
             'http://example.org/tree-ops-ext#gid-2', 
             'http://example.org/tree-ops-ext#gid-2', 
             'http://example.org/tree-ops-ext#gid-2', 
             'http://example.org/tree-ops-ext#gid-2', 
             'http://example.org/tree-ops-ext#gid-2', 
             'http://example.org/tree-ops-ext#gid-2', 
             'http://example.org/tree-ops-ext#gid-2', 
             'http://example.org/tree-ops-ext#gid-2', 
             'http://example.org/tree-ops-ext#gid-6', 
             'http://example.org/tree-ops-ext#gid-6', 
             'http://example.org/tree-ops-ext#gid-6', 
             'http://example.org/tree-ops-ext#gid-6', 
             'http://example.org/tree-ops-ext#gid-6', 
             'http://example.org/tree-ops-ext#gid-6', 
             'http://example.org/tree-ops-ext#gid-6', 
             'http://example.org/tree-ops-ext#gid-6', 
             'http://example.org/tree-ops-ext#gid-6']
            )
        
        
        # minimal mode
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='minimal'
                    )
            
        print(json_ld)
        

#%% TESTS - General Functions

class TestGeneralFunctions(unittest.TestCase):
    ""
    
    def test_get_type_of_metadata_object(self):
        ""
        logging.info('TEST: test_get_type_of_metadata_object')
        
        
        result=csvw_functions.get_type_of_metadata_object(csvw_primer_example_4)
        self.assertEqual(result,
                         'Table')
        
        result=csvw_functions.get_type_of_metadata_object(csvw_primer_example_5)
        self.assertEqual(result,
                         'TableGroup')
        
        
    
        
    def test_get_common_properties_of_metadata_object(self):
        ""
        logging.info('TEST: test_get_common_properties_of_metadata_object')
        
        result=csvw_functions.get_common_properties_of_metadata_object(metadata_vocabulary_example_41)
        self.assertEqual(
            result,
            ['dc:title']
            )
        
        
    def test_get_inherited_properties_from_type(self):
        ""
        logging.info('TEST: test_get_inherited_properties_from_type')
        
        result=csvw_functions.get_inherited_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['aboutUrl', 'datatype', 'default', 'lang', 'null', 'ordered', 
             'propertyUrl', 'required', 'separator', 'textDirection', 'valueUrl']
            )
        
        
    def test_get_optional_properties_from_type(self):
        ""
        logging.info('TEST: test_get_optional_properties_from_type')
        
        result=csvw_functions.get_optional_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['dialect', 'notes', 'tableDirection', 'tableSchema', 
             'transformations', '@id', '@type']
            )
        
    def test_get_required_properties_from_type(self):
        ""
        logging.info('TEST: test_get_required_properties_from_type')
        
        result=csvw_functions.get_required_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['tables']
            )
        
    
    def test_get_schema_from_schema_name(self):
        ""
        logging.info('TEST: test_get_schema_from_schema_name')
        
        result=csvw_functions.get_schema_from_schema_name(
            'table_description.schema.json'
            )
        self.assertIsInstance(
            result,
            dict)
        
        
    def test_get_schema_name_from_type(self):
        ""
        logging.info('TEST: test_get_schema_name_from_type')
        
        result=csvw_functions.get_schema_name_from_type('TableGroup')
        self.assertEqual(
            result,
            'table_group_description.schema.json'
            )
        
        
    def test_get_text_and_headers_from_file_url(self):
        ""
        logging.info('TEST: test_get_text_and_headers_from_file_url')
        
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
        logging.info('TEST: test_get_top_level_properties_from_type')
        
        result=csvw_functions.get_top_level_properties_from_type('TableGroup')
        self.assertEqual(
            result,
            ['@context']
            )
      
        

        
if __name__=='__main__':
    
    unittest.main()