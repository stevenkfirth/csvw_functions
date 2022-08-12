# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 09:59:59 2022

@author: cvskf
"""

import unittest
import os
import json
import datetime
from rdflib import Graph, Literal, URIRef, XSD

import logging
logging.basicConfig(
    filename='example2.log', 
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



def compare_json(self,json1,json2):
    ""
    if isinstance(json1,dict):
        
        self.assertIsInstance(
            json2,
            dict
            )
        
        self.assertEqual(
            set(list(json1)),
            set(list(json2))
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
            list
            )
        
        self.assertEqual(
            len(json1),
            len(json2)
            )
        
        for i in range(len(json1)):
            
            compare_json(
                self,
                json1[i],
                json2[i]
                )
        
    else:
        
        self.assertEqual(json1,json2)


    
    
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
                 },
             '@type': 'Table', 
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
                      '@type': 'Column'}]
                 }, 
             '@type': 'Table', 
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
                     ]
                 }, 
             '@type': 'Table', 
             'url': 'example_21.csv'}
            )
 
    
    def test_validate_metadata(self):
        ""
        logging.info('TEST: test_validate_metadata')
        
        json_fp=r'model_for_tabular_data_and_metadata_example_files/example_17.json'
        
        csvw_functions.validate_metadata(json_fp)
    
    
    
#%% TESTS - Model for Tabular Data and Metadata

#%% 4- Annotating Tables

class TestSection4(unittest.TestCase):
    ""
    
    def test_validate_metadata_obj_dict(self):
        ""
        
        metadata_obj_dict={}
        schema_name='table_group_description.schema.json'
        errors=\
            csvw_functions.validate_metadata_obj_dict(
                metadata_obj_dict,
                schema_name
                )
        
        # for error in errors:
            
        #     #print(error)
        #     print(error.message)
        #     print(error.validator)
        #     print(error.validator_value)
        #     print(error.schema)
        #     print(error.relative_schema_path)
        #     print(error.absolute_schema_path)
        #     print(error.schema_path)
        #     print(error.relative_path)
        #     print(error.absolute_path)
        #     #print(error.json_path)
        #     print(error.path)
        #     print(error.instance)
        #     print(error.context)
        #     print(error.cause)
        #     print(error.parent)
        #     #print(dir(error))

        
        metadata_obj_dict={'tables':[{'url':''}]}
        schema_name='table_group_description.schema.json'
        csvw_functions.validate_metadata_obj_dict(
                metadata_obj_dict,
                schema_name
                )
        
        metadata_obj_dict={'null':False}
        schema_name='table_group_description.schema.json'
        errors=\
            csvw_functions.validate_metadata_obj_dict(
                metadata_obj_dict,
                schema_name
                )
        # for error in errors:
            
        #     #print(error)
        #     print(error.message)
        #     print(error.validator)
        #     print(error.validator_value)
        #     print(error.schema)
        #     print(error.relative_schema_path)
        #     print(error.absolute_schema_path)
        #     print(error.schema_path)
        #     print(error.relative_path)
        #     print(error.absolute_path)
        #     #print(error.json_path)
        #     print(error.path)
        #     print(error.instance)
        #     print(error.context)
        #     print(error.cause)
        #     print(error.parent)
        #     #print(dir(error))
        
        metadata_obj_dict={'textDirection':'forwards'}
        schema_name='table_group_description.schema.json'
        errors=\
            csvw_functions.validate_metadata_obj_dict(
                metadata_obj_dict,
                schema_name
                )
        
        #print(list(errors))
        
        metadata_obj_dict={'datatype': {'base': 'string', 'format': 'YYYY-mm-dd'}}
        schema_name='table_group_description.schema.json'
        errors=\
            csvw_functions.validate_metadata_obj_dict(
                metadata_obj_dict,
                schema_name
                )
        
        #print(list(errors))
        
        
     
        

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
        
    
#%% Section 6.4.4. Formats for dates and times

class TestSection6_4_4_Formats_for_dates_and_times(unittest.TestCase):
    ""

    def test_get_timezone_format(self):
        ""
        
        self.assertEqual(
            csvw_functions.get_timezone_format('yyyy-MM-ddXX'),
            ('XX', '')
            )
        
        self.assertEqual(
            csvw_functions.get_timezone_format('yyyy-MM-dd x'),
            ('x', ' ')
            )
        
        
    def test_parse_date(self):
        ""
        
        self.assertEqual(
            csvw_functions.parse_date(
                string_value='2022-07-20',
                datatype_base='date',
                datatype_format='yyyy-MM-dd',
                errors=[]),
            ('2022-07-20', 'date', [])
            )
    
        self.assertEqual(
            csvw_functions.parse_date(
                string_value='20-07-2022',
                datatype_base='date',
                datatype_format='dd-MM-yyyy',
                errors=[]),
            ('2022-07-20', 'date', [])
            )
        
        self.assertEqual(
            csvw_functions.parse_date(
                string_value='2022-07-20Z',
                datatype_base='date',
                datatype_format='yyyy-MM-ddX',
                errors=[]),
            ('2022-07-20+00:00', 'date', [])
            )
    
        self.assertEqual(
            csvw_functions.parse_date(
                string_value='2022-07-20+0100',
                datatype_base='date',
                datatype_format='yyyy-MM-ddXX',
                errors=[]),
            ('2022-07-20+01:00', 'date', [])
            )
    
    
    def test_parse_time(self):
        ""
        
        self.assertEqual(
            csvw_functions.parse_time(
                string_value='12:13:14',
                datatype_base='time',
                datatype_format='HH:mm:ss',
                errors=[]),
            ('12:13:14', 'time', [])
            )
        
        self.assertEqual(
            csvw_functions.parse_time(
                string_value='121314',
                datatype_base='time',
                datatype_format='HHmmss',
                errors=[]),
            ('12:13:14', 'time', [])
            )
        
        self.assertEqual(
            csvw_functions.parse_time(
                string_value='12:13:14.5',
                datatype_base='time',
                datatype_format='HH:mm:ss.S',
                errors=[]),
            ('12:13:14.5', 'time', [])
            )
        
        self.assertEqual(
            csvw_functions.parse_time(
                string_value='12:13:14.55',
                datatype_base='time',
                datatype_format='HH:mm:ss.S',
                errors=[]),
            ('12:13:14.6', 'time', [])
            )
        
        self.assertEqual(
            csvw_functions.parse_time(
                string_value='12:13:14Z',
                datatype_base='time',
                datatype_format='HH:mm:ssX',
                errors=[]),
            ('12:13:14+00:00', 'time', [])
            )
    
        self.assertEqual(
            csvw_functions.parse_time(
                string_value='12:13:14.5Z',
                datatype_base='time',
                datatype_format='HH:mm:ss.SX',
                errors=[]),
            ('12:13:14.5+00:00', 'time', [])
            )
        
        
    def test_parse_datetime(self):
        ""
        self.assertEqual(
            csvw_functions.parse_datetime(
                string_value='2022-07-20T01:02:03',
                datatype_base='datetime',
                datatype_format='yyyy-MM-ddTHH:mm:ss',
                errors=[]),
            ('2022-07-20T01:02:03', 'datetime', [])
            )
        self.assertEqual(
            csvw_functions.parse_datetime(
                string_value='2022-07-20 01:02:03',
                datatype_base='datetime',
                datatype_format='yyyy-MM-dd HH:mm:ss',
                errors=[]),
            ('2022-07-20T01:02:03', 'datetime', [])
            )
        self.assertEqual(
            csvw_functions.parse_datetime(
                string_value='2022-07-20T01:02:03Z',
                datatype_base='datetime',
                datatype_format='yyyy-MM-ddTHH:mm:ssX',
                errors=[]),
            ('2022-07-20T01:02:03+00:00', 'datetime', [])
            )
        

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
                     ]
                 },
             '@type': 'Table',
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
        
#%% Section 5.5.2. (Foreign Keys) Examples

class TestSection5_5_2(unittest.TestCase):
    ""
    
    def test_section_5_5_2_example_27(self):
        ""
        # 5.5.2.1 Foreign Key Reference Between Tables
        
        logging.info('TEST: test_section_5_5_2_example_27')
        
        json_fp=r'metadata_vocabulary_example_files/example_27.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                metadata_file_path_or_url=json_fp
                )
        annotated_table_dicts=annotated_table_group_dict['tables']
        
        #---check first table---
        # url
        #print(os.path.basename(annotated_table_dicts[0]['url']))
        self.assertEqual(
            os.path.basename(annotated_table_dicts[0]['url']),
            'countries.csv'
            )
        
        #---check first row of first table---
        # number of primary keys
        #print(len(annotated_table_dicts[0]['rows'][0]['primaryKey']))
        self.assertEqual(
            len(annotated_table_dicts[0]['rows'][0]['primaryKey']),
            1
            )
        # string value of cell of first primary key
        #print(annotated_table_dicts[0]['rows'][0]['primaryKey'][0]['stringValue'])
        self.assertEqual(
            annotated_table_dicts[0]['rows'][0]['primaryKey'][0]['stringValue'],
            'AD'
            )
        
        #---check second table---
        # url
        #print(os.path.basename(annotated_table_dicts[1]['url']))
        self.assertEqual(
            os.path.basename(annotated_table_dicts[1]['url']),
            'country_slice.csv'
            )
        # number of items in first foreign key
        #print(len(annotated_table_dicts[1]['foreignKeys'][0]))
        self.assertEqual(
            len(annotated_table_dicts[1]['foreignKeys'][0]),
            2
            )
        # table urls of first item in first foreign key
        #print([os.path.basename(x['table']['url']) for x in annotated_table_dicts[1]['foreignKeys'][0][0]])
        self.assertEqual(
            [os.path.basename(x['table']['url']) for x in annotated_table_dicts[1]['foreignKeys'][0][0]],
            ['country_slice.csv']
            )
        # column names of first item in first foreign key
        #print([os.path.basename(x['name']) for x in annotated_table_dicts[1]['foreignKeys'][0][0]])
        self.assertEqual(
            [os.path.basename(x['name']) for x in annotated_table_dicts[1]['foreignKeys'][0][0]],
            ['countryRef']
            )
        # table urls of second item in first foreign key
        #print([os.path.basename(x['table']['url']) for x in annotated_table_dicts[1]['foreignKeys'][0][1]])
        self.assertEqual(
            [os.path.basename(x['table']['url']) for x in annotated_table_dicts[1]['foreignKeys'][0][1]],
            ['countries.csv']
            )
        # column names of second item in first foreign key
        #print([os.path.basename(x['name']) for x in annotated_table_dicts[1]['foreignKeys'][0][1]])
        self.assertEqual(
            [os.path.basename(x['name']) for x in annotated_table_dicts[1]['foreignKeys'][0][1]],
            ['countryCode']
            )
        # foreignKeys annotation
        self.assertEqual(
            annotated_table_dicts[1]['foreignKeys'],
            [
                [
                    [annotated_table_dicts[1]['columns'][0]],
                    [annotated_table_dicts[0]['columns'][0]]
                ]
            ]
            )
        
        
        #---check first row of second table---
        # number of referencedRow annotations
        #print(len(annotated_table_dicts[1]['rows'][0]['referencedRows']))
        self.assertEqual(
            len(annotated_table_dicts[1]['rows'][0]['referencedRows']),
            1
            )
        # referencedRow annotation
        self.assertEqual(
            annotated_table_dicts[1]['rows'][0]['referencedRows'],
            [
                [
                    annotated_table_dicts[1]['foreignKeys'][0],
                    annotated_table_dicts[0]['rows'][2]
                 ]
            ]
            )
        
        
        
    def test_section_5_5_2_example_30(self):
        ""
        # 5.5.2.2. Foreign Key Reference Between Schemas
        
        logging.info('TEST: test_section_5_5_2_2_example_30')
        
        json_fp=r'metadata_vocabulary_example_files/example_30.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                metadata_file_path_or_url=json_fp
                )
        annotated_table_dicts=annotated_table_group_dict['tables']
        
        # check referencedRows in second table are referrring to the correct
        # row numbers from the first table.
        self.assertEqual(
            [x['referencedRows'][0][1]['number'] for x in annotated_table_dicts[1]['rows']],
            [3, 3, 1, 1]
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
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://example.org/countries.csv'
                    )
        #print(json_ld)
        
        self.assertEqual(
            json_ld,
            {'tables': [
                {'url': 'http://example.org/countries.csv', 
                 'row': [
                     {'rownum': 1, 
                      'url': 'http://example.org/countries.csv#row=2', 
                      'describes': [
                          {'countryCode': 'AD', 
                           'latitude': '42.5', 
                           'longitude': '1.6', 
                           'name': 'Andorra'
                           }
                          ]
                      }, 
                     {'rownum': 2, 
                      'url': 'http://example.org/countries.csv#row=3', 
                      'describes': [
                            {'countryCode': 'AE', 
                             'latitude': '23.4', 
                             'longitude': '53.8', 
                             'name': 'United Arab Emirates'
                             }
                            ]
                        }, 
                    {'rownum': 3, 
                     'url': 'http://example.org/countries.csv#row=4', 
                     'describes': [
                         {'countryCode': 'AF', 
                          'latitude': '33.9', 
                          'longitude': '67.7', 
                          'name': 'Afghanistan'}
                         ]
                     }
                    ]
                 }
                ]
                }
            
            )
        
        
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
        # about url
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
            
        #print(json_ld)

        self.assertEqual(
            json_ld,
            [{'@id': 'http://example.org/tree-ops-ext#gid-1', 
              'on_street': 'ADDISON AV', 
              'species': 'Celtis australis', 
              'trim_cycle': 'Large Tree Routine Prune', 
              'dbh': 11, 
              'inventory_date': '2010-10-18', 
              'protected': False, 
              'kml': '<Point><coordinates>-122.156485,37.440963</coordinates></Point>'}, 
             {'@id': 'http://example.org/tree-ops-ext#gid-2', 
              'on_street': 'EMERSON ST', 
              'species': 'Liquidambar styraciflua', 
              'trim_cycle': 'Large Tree Routine Prune', 
              'dbh': 11, 
              'inventory_date': '2010-06-02', 
              'protected': False, 
              'kml': '<Point><coordinates>-122.156749,37.440958</coordinates></Point>'}, 
             {'@id': 'http://example.org/tree-ops-ext#gid-6', 
              'on_street': 'ADDISON AV', 
              'species': 'Robinia pseudoacacia', 
              'trim_cycle': 'Large Tree Routine Prune', 
              'dbh': 29, 
              'inventory_date': '2010-06-01', 
              'comments': [
                  'cavity or decay', 
                  'trunk decay', 
                  'codominant leaders', 
                  'included bark', 
                  'large leader or limb decay', 
                  'previous failure root damage', 
                  'root decay', 
                  'beware of BEES'
                  ], 
              'protected': True, 
              'kml': '<Point><coordinates>-122.156299,37.441151</coordinates></Point>'}
             ]
            )

        # standard mode
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://example.org/tree-ops-ext.csv'
                    )
        #print(json_ld)
        
        # table properties
        # @id
        self.assertEqual(
            json_ld['tables'][0]['@id'],
            'http://example.org/tree-ops-ext'
            )
        # url
        self.assertEqual(
            json_ld['tables'][0]['url'],
            'http://example.org/tree-ops-ext.csv'
            )
        # dc:title
        self.assertEqual(
            json_ld['tables'][0]['dc:title'],
            'Tree Operations'
            )
        # dcat:keyword
        self.assertEqual(
            json_ld['tables'][0]['dcat:keyword'],
            [ "tree", "street", "maintenance" ]
            )
        # dc:publisher
        self.assertEqual(
            json_ld['tables'][0]['dc:publisher'],
            [
                {"schema:name": "Example Municipality",
                 "schema:url": "http://example.org"}
            ]
            )
        # dc:license
        self.assertEqual(
            json_ld['tables'][0]['dc:license'],
            'http://opendefinition.org/licenses/cc-by/'
            )
        # dc:modified
        self.assertEqual(
            json_ld['tables'][0]['dc:modified'],
            '2010-12-31'
            )
        # notes
        self.assertEqual(
            json_ld['tables'][0]['notes'],
            [{
              "@type": "oa:Annotation",
              "oa:hasTarget": "http://example.org/tree-ops-ext",
              "oa:hasBody": {
                "@type": "oa:EmbeddedContent",
                "rdf:value": "This is a very interesting comment about the table; it's a table!",
                "dc:format": "text/plain"
                }
            }]
            )
        
        # first row
        self.assertEqual(
            json_ld['tables'][0]['row'][0],
            {
              "url": "http://example.org/tree-ops-ext.csv#row=2",
              "rownum": 1,
              "describes": [{ 
                "@id": "http://example.org/tree-ops-ext#gid-1",
                "on_street": "ADDISON AV",
                "species": "Celtis australis",
                "trim_cycle": "Large Tree Routine Prune",
                "dbh": 11,
                "inventory_date": "2010-10-18",
                "protected": False,
                "kml": "<Point><coordinates>-122.156485,37.440963</coordinates></Point>"
              }]
            }
            )
        
        
        # second row
        self.assertEqual(
            json_ld['tables'][0]['row'][1],
            {
              "url": "http://example.org/tree-ops-ext.csv#row=3",
              "rownum": 2,
              "describes": [{ 
                "@id": "http://example.org/tree-ops-ext#gid-2",
                "on_street": "EMERSON ST",
                "species": "Liquidambar styraciflua",
                "trim_cycle": "Large Tree Routine Prune",
                "dbh": 11,
                "inventory_date": "2010-06-02",
                "protected": False,
                "kml": "<Point><coordinates>-122.156749,37.440958</coordinates></Point>"
              }]
            }
            )
        
        # third row
        self.assertEqual(
            json_ld['tables'][0]['row'][2],
            {
              "url": "http://example.org/tree-ops-ext.csv#row=4",
              "rownum": 3,
              "describes": [{  
                "@id": "http://example.org/tree-ops-ext#gid-6",
                "on_street": "ADDISON AV",
                "species": "Robinia pseudoacacia",
                "trim_cycle": "Large Tree Routine Prune",
                "dbh": 29,
                "inventory_date": "2010-06-01",
                "comments": [ "cavity or decay", 
                  "trunk decay", 
                  "codominant leaders", 
                  "included bark",
                  "large leader or limb decay", 
                  "previous failure root damage", 
                  "root decay", 
                  "beware of BEES" ],
                "protected": True,
                "kml": "<Point><coordinates>-122.156299,37.441151</coordinates></Point>"
              }]
            }
            )
        

    def test_section_6_3_Example_with_single_table_and_using_virtual_columns_to_produce_multiple_subjects_per_row(self):
        ""
        logging.info('TEST: test_section_6_3_Example_with_single_table_and_using_virtual_columns_to_produce_multiple_subjects_per_row')
        
        fp=r'generating_json_from_tabular_data_example_files/events-listing.csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(fp)
        
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
            'events-listing.csv'
            )
        self.assertEqual(
            len(annotated_table_dict['columns']),
            10
            )
        self.assertEqual(
            len(annotated_table_dict['rows']),
            2
            )
        
        #---check annotated columns---
        # number
        self.assertEqual(
            [column['number'] for column in annotated_columns_list],
            [1,2,3,4,5,6,7,8,9,10]
            )
        # source number
        self.assertEqual(
            [column['sourceNumber'] for column in annotated_columns_list],
            [1,2,3,4,5,None,None,None,None,None]  # source numbers for virtual columns set to None -- different from example
            )
        # number of cells
        self.assertEqual(
            [len(column['cells']) for column in annotated_columns_list],
            [2,2,2,2,2,2,2,2,2,2]
            )
        # name
        #print([column['name'] for column in annotated_columns_list])
        self.assertEqual(
            [column['name'] for column in annotated_columns_list],
            ['name', 
             'start_date', 
             'location_name', 
             'location_address', 
             'ticket_url', 
             'type_event', 
             'type_place', 
             'type_offer', 
             'location', 
             'offers']
            )
        # title
        #print([column['titles'] for column in annotated_columns_list])
        self.assertEqual(
            [column['titles'] for column in annotated_columns_list],
            [[{'@value': 'Name', '@language': 'en'}], 
             [{'@value': 'Start Date', '@language': 'en'}], 
             [{'@value': 'Location Name', '@language': 'en'}], 
             [{'@value': 'Location Address', '@language': 'en'}], 
             [{'@value': 'Ticket Url', '@language': 'en'}], 
             [], 
             [], 
             [], 
             [], 
             []]
            )
        # virtual
        #print([column['virtual'] for column in annotated_columns_list])
        self.assertEqual(
            [column['virtual'] for column in annotated_columns_list],
            [False, False, False, False, False, True, True, True, True, True]
            )
        
        #---check annotated rows---
        # number
        self.assertEqual(
            [row['number'] for row in annotated_rows_list],
            [1,2]
            )
        # source number
        self.assertEqual(
            [row['sourceNumber'] for row in annotated_rows_list],
            [2,3]
            )
        # number of cells
        self.assertEqual(
            [len(row['cells']) for row in annotated_rows_list],
            [10,10]
            )
        
        #---check annotated cells---
        # column number
        #print([cell['column']['number'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['column']['number'] for cell in annotated_cells_list],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            )
        # row number
        #print([cell['row']['number'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['row']['number'] for cell in annotated_cells_list],
            [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
            )
        # string value
        #print([cell['stringValue'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['stringValue'] for cell in annotated_cells_list],
            ['B.B. King', 
             '2014-04-12T19:30', 
             'Lupoâ€™s Heartbreak Hotel', 
             '79 Washington St., Providence, RI', 
             'https://www.etix.com/ticket/1771656', 
             '', 
             '', 
             '', 
             '', 
             '', 
             'B.B. King', 
             '2014-04-13T20:00', 
             'Lynn Auditorium', 
             'Lynn, MA, 01901', 
             'http://frontgatetickets.com/venue.php?id=11766', 
             '', 
             '', 
             '', 
             '', 
             '']
            )
        # value
        #print([cell['value'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['value'] for cell in annotated_cells_list],
            [{'@value': 'B.B. King', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '2014-04-12T19:30:00', '@type': 'http://www.w3.org/2001/XMLSchema#dateTime'}, 
             {'@value': 'Lupoâ€™s Heartbreak Hotel', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '79 Washington St., Providence, RI', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'https://www.etix.com/ticket/1771656', '@type': 'http://www.w3.org/2001/XMLSchema#anyURI', '@language': 'und'}, 
             None, 
             None, 
             None, 
             None, 
             None, 
             {'@value': 'B.B. King', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '2014-04-13T20:00:00', '@type': 'http://www.w3.org/2001/XMLSchema#dateTime'}, 
             {'@value': 'Lynn Auditorium', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Lynn, MA, 01901', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'http://frontgatetickets.com/venue.php?id=11766', '@type': 'http://www.w3.org/2001/XMLSchema#anyURI', '@language': 'und'}, 
             None, 
             None, 
             None, 
             None, 
             None]
            )
        # about url
        #print([os.path.basename(cell['aboutURL']) for cell in annotated_cells_list])
        self.assertEqual(
            [os.path.basename(cell['aboutURL']) for cell in annotated_cells_list],
            ['events-listing.csv#event-1', 
             'events-listing.csv#event-1', 
             'events-listing.csv#place-1', 
             'events-listing.csv#place-1', 
             'events-listing.csv#offer-1', 
             'events-listing.csv#event-1', 
             'events-listing.csv#place-1', 
             'events-listing.csv#offer-1', 
             'events-listing.csv#event-1', 
             'events-listing.csv#event-1', 
             'events-listing.csv#event-2', 
             'events-listing.csv#event-2', 
             'events-listing.csv#place-2', 
             'events-listing.csv#place-2', 
             'events-listing.csv#offer-2', 
             'events-listing.csv#event-2', 
             'events-listing.csv#place-2', 
             'events-listing.csv#offer-2', 
             'events-listing.csv#event-2', 
             'events-listing.csv#event-2']
            )
        # property url
        #print([cell['propertyURL'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['propertyURL'] for cell in annotated_cells_list],
            ['http://schema.org/name', 
             'http://schema.org/startDate', 
             'http://schema.org/name', 
             'http://schema.org/address', 
             'http://schema.org/url', 
             'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 
             'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 
             'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 
             'http://schema.org/location', 
             'http://schema.org/offers', 
             'http://schema.org/name', 
             'http://schema.org/startDate', 
             'http://schema.org/name', 
             'http://schema.org/address', 
             'http://schema.org/url', 
             'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 
             'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 
             'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 
             'http://schema.org/location', 
             'http://schema.org/offers']
            )
        # value url
        # print([os.path.basename(cell['valueURL']) 
        #        if (not cell['valueURL'] is None and cell['valueURL'].startswith('C:'))
        #        else cell['valueURL']
        #        for cell in annotated_cells_list])
        self.assertEqual(
            [os.path.basename(cell['valueURL']) 
                   if (not cell['valueURL'] is None and cell['valueURL'].startswith('C:'))
                   else cell['valueURL']
                   for cell in annotated_cells_list],
            [None, 
             None, 
             None, 
             None, 
             None, 
             'http://schema.org/MusicEvent', 
             'http://schema.org/Place', 
             'http://schema.org/Offer', 
             'events-listing.csv#place-1',  # basename
             'events-listing.csv#offer-1',  # basename 
             None, 
             None, 
             None, 
             None, 
             None, 
             'http://schema.org/MusicEvent', 
             'http://schema.org/Place', 
             'http://schema.org/Offer', 
             'events-listing.csv#place-2',  # basename 
             'events-listing.csv#offer-2'  # basename
             ]
            )
        
        # minimal mode
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='minimal'
                    )
        #print(json_ld)
        
        #--check first item--
        # keys of object
        #print(list(json_ld[0].keys()))
        self.assertEqual(
            list(json_ld[0].keys()),
            ['@id', 
             'schema:name', 
             'schema:startDate', 
             '@type', 
             'schema:location', 
             'schema:offers']
            )
        # @id
        #print(os.path.basename(json_ld[0]['@id']))
        self.assertEqual(
            os.path.basename(json_ld[0]['@id']),
            'events-listing.csv#event-1'
            )
        # @type
        #print(json_ld[0]['@type'])
        self.assertEqual(
            json_ld[0]['@type'],
            'schema:MusicEvent'
            )
        # schema:name
        #print(json_ld[0]['schema:name'])
        self.assertEqual(
            json_ld[0]['schema:name'],
            'B.B. King'
            )
        # schema:startDate
        #print(json_ld[0]['schema:startDate'])
        self.assertEqual(
            json_ld[0]['schema:startDate'],
            '2014-04-12T19:30:00'
            )
        
        # schema:location[@id]
        #print(os.path.basename(json_ld[0]['schema:location']['@id']))
        self.assertEqual(
            os.path.basename(json_ld[0]['schema:location']['@id']),
            'events-listing.csv#place-1'
            )
        # schema:location[schema:name]
        #print(json_ld[0]['schema:location']['schema:name'])
        self.assertEqual(
            json_ld[0]['schema:location']['schema:name'],
            'Lupoâ€™s Heartbreak Hotel'
            )
        # schema:location[schema:address]
        #print(json_ld[0]['schema:location']['schema:address'])
        self.assertEqual(
            json_ld[0]['schema:location']['schema:address'],
            '79 Washington St., Providence, RI'
            )
        # schema:location[@type]
        #print(json_ld[0]['schema:location']['@type'])
        self.assertEqual(
            json_ld[0]['schema:location']['@type'],
            'schema:Place'
            )
        # schema:offers[@id]
        #print(os.path.basename(json_ld[0]['schema:offers']['@id']))
        self.assertEqual(
            os.path.basename(json_ld[0]['schema:offers']['@id']),
            'events-listing.csv#offer-1'
            )
        # schema:offers[schema:url]   # NOTE: DIFFERENT FROM EXAMPLE SOLUTION
        #print(json_ld[0]['schema:offers']['schema:url'])
        self.assertEqual(
            json_ld[0]['schema:offers']['schema:url'],
            'https://www.etix.com/ticket/1771656'
            )
        # schema:offers[@type]
        #print(json_ld[0]['schema:offers']['@type'])
        self.assertEqual(
            json_ld[0]['schema:offers']['@type'],
            'schema:Offer'
            )
        
        #--check second item--
        # keys of object
        #print(list(json_ld[1].keys()))
        self.assertEqual(
            list(json_ld[1].keys()),
            ['@id', 
             'schema:name', 
             'schema:startDate', 
             '@type', 
             'schema:location', 
             'schema:offers']
            )
        # @id
        #print(os.path.basename(json_ld[1]['@id']))
        self.assertEqual(
            os.path.basename(json_ld[1]['@id']),
            'events-listing.csv#event-2'
            )
        # @type
        #print(json_ld[1]['@type'])
        self.assertEqual(
            json_ld[1]['@type'],
            'schema:MusicEvent'
            )
        # schema:name
        #print(json_ld[1]['schema:name'])
        self.assertEqual(
            json_ld[1]['schema:name'],
            'B.B. King'
            )
        # schema:startDate
        #print(json_ld[1]['schema:startDate'])
        self.assertEqual(
            json_ld[1]['schema:startDate'],
            '2014-04-13T20:00:00'
            )
        
        # schema:location[@id]
        #print(os.path.basename(json_ld[1]['schema:location']['@id']))
        self.assertEqual(
            os.path.basename(json_ld[1]['schema:location']['@id']),
            'events-listing.csv#place-2'
            )
        # schema:location[schema:name]
        #print(json_ld[1]['schema:location']['schema:name'])
        self.assertEqual(
            json_ld[1]['schema:location']['schema:name'],
            'Lynn Auditorium'
            )
        # schema:location[schema:address]
        #print(json_ld[1]['schema:location']['schema:address'])
        self.assertEqual(
            json_ld[1]['schema:location']['schema:address'],
            'Lynn, MA, 01901'
            )
        # schema:location[@type]
        #print(json_ld[1]['schema:location']['@type'])
        self.assertEqual(
            json_ld[1]['schema:location']['@type'],
            'schema:Place'
            )
        # schema:offers[@id]
        #print(os.path.basename(json_ld[1]['schema:offers']['@id']))
        self.assertEqual(
            os.path.basename(json_ld[1]['schema:offers']['@id']),
            'events-listing.csv#offer-2'
            )
        # schema:offers[schema:url]   # NOTE: DIFFERENT FROM EXAMPLE SOLUTION
        #print(json_ld[1]['schema:offers']['schema:url'])
        self.assertEqual(
            json_ld[1]['schema:offers']['schema:url'],
            'http://frontgatetickets.com/venue.php?id=11766'
            )
        # schema:offers[@type]
        #print(json_ld[1]['schema:offers']['@type'])
        self.assertEqual(
            json_ld[1]['schema:offers']['@type'],
            'schema:Offer'
            )
        
        
    def test_section_6_4_Example_with_table_group_comprising_four_interrelated_tables(self):
        ""
        logging.info('TEST: test_section_6_4_Example_with_table_group_comprising_four_interrelated_tables')
        
        fp=r'generating_json_from_tabular_data_example_files/csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(fp)
        
        annotated_table_dicts=annotated_table_group_dict['tables']
        annotated_foreign_keys=[annotated_foreign_key
                                for annotated_table_dict in annotated_table_dicts
                                for annotated_foreign_key in annotated_table_dict['foreignKeys']]
        annotated_columns_list=[annotated_column_dict 
                                for annotated_table_dict in annotated_table_dicts
                                for annotated_column_dict in annotated_table_dict['columns']]
        annotated_rows_list=[annotated_row_dict 
                             for annotated_table_dict in annotated_table_dicts
                             for annotated_row_dict in annotated_table_dict['rows']]
        annotated_cells_list=[cell 
                              for row in annotated_rows_list 
                              for cell in row['cells']]
        
        #---check annotated table group
        # number of tables
        self.assertEqual(
            len(annotated_table_group_dict['tables']),
            4
            )
        
        #---check annotated tables---
        # url
        #print([os.path.basename(x['url']) for x in annotated_table_dicts])
        self.assertEqual(
            [os.path.basename(x['url']) for x in annotated_table_dicts],
            ['organizations.csv', 
             'professions.csv', 
             'senior-roles.csv', 
             'junior-roles.csv']
            )
        # number of columns
        #print([len(x['columns']) for x in annotated_table_dicts])
        self.assertEqual(
            [len(x['columns']) for x in annotated_table_dicts],
            [3, 1, 8, 8]  # NOTE DIFFERENT FROM EXAMPLE SOLUTION
            )
        # number of rows
        #print([len(x['rows']) for x in annotated_table_dicts])
        self.assertEqual(
            [len(x['rows']) for x in annotated_table_dicts],
            [2, 4, 2, 2]
            )
        # suppress Output
        #print([x['suppressOutput'] for x in annotated_table_dicts])
        self.assertEqual(
            [x['suppressOutput'] for x in annotated_table_dicts],
            [True, True, False, False]
            )
        # foreign keys
        #print([len(x['foreignKeys']) for x in annotated_table_dicts])
        self.assertEqual(
            [len(x['foreignKeys']) for x in annotated_table_dicts],
            [1, 0, 3, 3]
            )
        
        #---check foreign keys---
        # column numbers in table
        #print([y['number'] for x in annotated_foreign_keys for y in x[0]])
        self.assertEqual(
            [y['number'] for x in annotated_foreign_keys for y in x[0]],
            [3, 5, 6, 7, 1, 7, 8]
            )
        # column numbers in referenced table
        #print([y['number'] for x in annotated_foreign_keys for y in x[1]])
        self.assertEqual(
            [y['number'] for x in annotated_foreign_keys for y in x[1]],
            [1, 1, 1, 1, 1, 1, 1]
            )
        # table index of referenced table
        #print([annotated_table_dicts.index(y['table']) for x in annotated_foreign_keys for y in x[1]])
        self.assertEqual(
            [annotated_table_dicts.index(y['table']) for x in annotated_foreign_keys for y in x[1]],
            [0, 2, 1, 0, 2, 1, 0]
            )
        
        #---check annotated columns---
        # number
        self.assertEqual(
            [column['number'] for column in annotated_columns_list],
            [1, 2, 3, 1, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8]  # second and third value differ from example
            )
        # source number
        self.assertEqual(
            [column['sourceNumber'] for column in annotated_columns_list],
            [1, 2, 3, 1, 1, 2, 3, 4, 5, 6, 7, None, 1, 2, 3, 4, 5, 6, 7, 8]  # second and third value differ from example; virtual column differs from example (None rather than 8)
            )
        # number of cells
        self.assertEqual(
            [len(column['cells']) for column in annotated_columns_list],
            [2, 2, 2, 4, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
            )
        # name
        #print([column['name'] for column in annotated_columns_list])
        self.assertEqual(
            [column['name'] for column in annotated_columns_list],
            ['ref', 
             'name', 
             'department', 
             'name', 
             'ref', 
             'name', 
             'grade', 
             'job', 
             'reportsTo', 
             'profession', 
             'organizationRef', 
             'post_holder', 
             'reportsToSenior', 
             'grade', 
             'min_pay', 
             'max_pay', 
             'job', 
             'number', 
             'profession', 
             'organizationRef']
            )
        # title
        #print([column['titles'] for column in annotated_columns_list])
        self.assertEqual(
            [column['titles'] for column in annotated_columns_list],
            [[{'@value': 'Organization Unique Reference', '@language': 'und'}], 
             [{'@value': 'Organization Name', '@language': 'und'}], 
             [{'@value': 'Department Reference', '@language': 'und'}], 
             [{'@value': 'Profession', '@language': 'und'}], 
             [{'@value': 'Post Unique Reference', '@language': 'und'}], 
             [{'@value': 'Name', '@language': 'und'}], 
             [{'@value': 'Grade', '@language': 'und'}], 
             [{'@value': 'Job Title', '@language': 'und'}], 
             [{'@value': 'Reports to Senior Post', '@language': 'und'}], 
             [{'@value': 'Profession', '@language': 'und'}], 
             [{'@value': 'Organization Reference', '@language': 'und'}], 
             [], 
             [{'@value': 'Reporting Senior Post', '@language': 'und'}], 
             [{'@value': 'Grade', '@language': 'und'}], 
             [{'@value': 'Payscale Minimum (Â£)', '@language': 'und'}], 
             [{'@value': 'Payscale Maximum (Â£)', '@language': 'und'}], 
             [{'@value': 'Generic Job Title', '@language': 'und'}], 
             [{'@value': 'Number of Posts (FTE)', '@language': 'und'}], 
             [{'@value': 'Profession', '@language': 'und'}], 
             [{'@value': 'Organization Reference', '@language': 'und'}]
             ]
            )
        # required
        #print([column['required'] for column in annotated_columns_list])
        self.assertEqual(
            [column['required'] for column in annotated_columns_list],
            [True, 
             False, 
             False, 
             True, 
             True, 
             False, 
             False, 
             False, 
             False, 
             False, 
             True, 
             False, 
             True, 
             False, 
             False, 
             False, 
             False, 
             False, 
             False, 
             True]
            )
        # virtual
        #print([column['virtual'] for column in annotated_columns_list])
        self.assertEqual(
            [column['virtual'] for column in annotated_columns_list],
            [False, False, False, False, False, False, False, False, False, 
             False, False, True, False, False, False, False, False, 
             False, False, False]
            )
        
        #---check annotated rows---
        # number
        self.assertEqual(
            [row['number'] for row in annotated_rows_list],
            [1, 2, 1, 2, 3, 4, 1, 2, 1, 2]
            )
        # source number
        self.assertEqual(
            [row['sourceNumber'] for row in annotated_rows_list],
            [2, 3, 2, 3, 4, 5, 2, 3, 2, 3]
            )
        # number of cells
        self.assertEqual(
            [len(row['cells']) for row in annotated_rows_list],
            [3, 3, 1, 1, 1, 1, 8, 8, 8, 8]
            )
        
        #---check annotated cells---
        # column number
        #print([cell['column']['number'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['column']['number'] for cell in annotated_cells_list],
            [1, 2, 3,
             1, 2, 3, 
             1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8,
             1, 2, 3, 4, 5, 6, 7, 8, 
             1, 2, 3, 4, 5, 6, 7, 8, 
             1, 2, 3, 4, 5, 6, 7, 8]
            )
        # row number
        #print([cell['row']['number'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['row']['number'] for cell in annotated_cells_list],
            [1, 1, 1, 
             2, 2, 2, 
             1, 2, 3, 4, 
             1, 1, 1, 1, 1, 1, 1, 1, 
             2, 2, 2, 2, 2, 2, 2, 2, 
             1, 1, 1, 1, 1, 1, 1, 1, 
             2, 2, 2, 2, 2, 2, 2, 2]
            )
        # string value
        #print([cell['stringValue'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['stringValue'] for cell in annotated_cells_list],
            ['hefce.ac.uk', 
             'Higher Education Funding Council for England', 
             'bis.gov.uk', 
             'bis.gov.uk', 
             'Department for Business, Innovation and Skills', 
             'xx', 
             'Finance', 
             'Information Technology', 
             'Operational Delivery', 
             'Policy', 
             '90115', 
             'Steve Egan', 
             'SCS1A', 
             'Deputy Chief Executive', 
             '90334', 
             'Finance', 
             'hefce.ac.uk', 
             '', 
             '90334', 
             'Sir Alan Langlands', 
             'SCS4', 
             'Chief Executive', 
             'xx', 
             'Policy', 
             'hefce.ac.uk', 
             '', 
             '90115', 
             '4', 
             '17426', 
             '20002', 
             'Administrator', 
             '8.67', 
             'Operational Delivery', 
             'hefce.ac.uk', 
             '90115', 
             '5', 
             '19546', 
             '22478', 
             'Administrator', 
             '0.5', 
             'Operational Delivery', 
             'hefce.ac.uk']
            )
        # value
        #print([cell['value'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['value'] for cell in annotated_cells_list],
            [{'@value': 'hefce.ac.uk', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Higher Education Funding Council for England', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'bis.gov.uk', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'bis.gov.uk', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Department for Business, Innovation and Skills', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             None, 
             {'@value': 'Finance', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Information Technology', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Operational Delivery', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Policy', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '90115', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Steve Egan', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'SCS1A', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Deputy Chief Executive', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '90334', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Finance', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'hefce.ac.uk', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             None, 
             {'@value': '90334', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Sir Alan Langlands', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'SCS4', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Chief Executive', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             None, 
             {'@value': 'Policy', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'hefce.ac.uk', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             None, 
             {'@value': '90115', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '4', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 17426, '@type': 'http://www.w3.org/2001/XMLSchema#integer'}, 
             {'@value': 20002, '@type': 'http://www.w3.org/2001/XMLSchema#integer'}, 
             {'@value': 'Administrator', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 8.67, '@type': 'http://www.w3.org/2001/XMLSchema#double'}, 
             {'@value': 'Operational Delivery', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'hefce.ac.uk', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '90115', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '5', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'},
             {'@value': 19546, '@type': 'http://www.w3.org/2001/XMLSchema#integer'}, 
             {'@value': 22478, '@type': 'http://www.w3.org/2001/XMLSchema#integer'}, 
             {'@value': 'Administrator', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 0.5, '@type': 'http://www.w3.org/2001/XMLSchema#double'}, 
             {'@value': 'Operational Delivery', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'hefce.ac.uk', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}
             ]
            )
        # about url
        #print([cell['aboutURL'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['aboutURL'] for cell in annotated_cells_list],
            ['http://example.org/organization/hefce.ac.uk', 
             'http://example.org/organization/hefce.ac.uk', 
             'http://example.org/organization/hefce.ac.uk', 
             'http://example.org/organization/bis.gov.uk', 
             'http://example.org/organization/bis.gov.uk', 
             'http://example.org/organization/bis.gov.uk', 
             None, 
             None, 
             None, 
             None, 
             'http://example.org/organization/hefce.ac.uk/post/90115', 
             'http://example.org/organization/hefce.ac.uk/person/1', 
             'http://example.org/organization/hefce.ac.uk/post/90115', 
             'http://example.org/organization/hefce.ac.uk/post/90115', 
             'http://example.org/organization/hefce.ac.uk/post/90115', 
             'http://example.org/organization/hefce.ac.uk/post/90115', 
             'http://example.org/organization/hefce.ac.uk/post/90115', 
             'http://example.org/organization/hefce.ac.uk/post/90115', 
             'http://example.org/organization/hefce.ac.uk/post/90334', 
             'http://example.org/organization/hefce.ac.uk/person/2', 
             'http://example.org/organization/hefce.ac.uk/post/90334', 
             'http://example.org/organization/hefce.ac.uk/post/90334', 
             'http://example.org/organization/hefce.ac.uk/post/90334', 
             'http://example.org/organization/hefce.ac.uk/post/90334', 
             'http://example.org/organization/hefce.ac.uk/post/90334', 
             'http://example.org/organization/hefce.ac.uk/post/90334', 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None
             ]
            )
        # property url
        #print([cell['propertyURL'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['propertyURL'] for cell in annotated_cells_list],
            ['http://purl.org/dc/terms/identifier', 
             'http://xmlns.com/foaf/0.1/name', 
             'http://www.w3.org/ns/org#subOrganizationOf', 
             'http://purl.org/dc/terms/identifier', 
             'http://xmlns.com/foaf/0.1/name', 
             'http://www.w3.org/ns/org#subOrganizationOf', 
             None, 
             None, 
             None, 
             None, 
             'http://purl.org/dc/terms/identifier', 
             'http://xmlns.com/foaf/0.1/name', 
             'http://example.org/gov.uk/def/grade', 
             'http://example.org/gov.uk/def/job', 
             'http://www.w3.org/ns/org#reportsTo', 
             'http://example.org/gov.uk/def/profession', 
             'http://www.w3.org/ns/org#postIn', 
             'http://www.w3.org/ns/org#heldBy', 
             'http://purl.org/dc/terms/identifier', 
             'http://xmlns.com/foaf/0.1/name', 
             'http://example.org/gov.uk/def/grade', 
             'http://example.org/gov.uk/def/job', 
             'http://www.w3.org/ns/org#reportsTo', 
             'http://example.org/gov.uk/def/profession', 
             'http://www.w3.org/ns/org#postIn', 
             'http://www.w3.org/ns/org#heldBy', 
             'http://www.w3.org/ns/org#reportsTo', 
             'http://example.org/gov.uk/def/grade', 
             'http://example.org/gov.uk/def/min_pay', 
             'http://example.org/gov.uk/def/max_pay', 
             'http://example.org/gov.uk/def/job', 
             'http://example.org/gov.uk/def/number_of_posts', 
             'http://example.org/gov.uk/def/profession', 
             'http://www.w3.org/ns/org#postIn', 
             'http://www.w3.org/ns/org#reportsTo', 
             'http://example.org/gov.uk/def/grade', 
             'http://example.org/gov.uk/def/min_pay', 
             'http://example.org/gov.uk/def/max_pay', 
             'http://example.org/gov.uk/def/job', 
             'http://example.org/gov.uk/def/number_of_posts', 
             'http://example.org/gov.uk/def/profession', 
             'http://www.w3.org/ns/org#postIn'
             ]
            )
        # value url
        #print([cell['valueURL'] for cell in annotated_cells_list])
        self.assertEqual(
            [cell['valueURL'] for cell in annotated_cells_list],
            [None, 
             None, 
             'http://example.org/organization/bis.gov.uk', 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             'http://example.org/organization/hefce.ac.uk/post/90334', 
             None, 
             'http://example.org/organization/hefce.ac.uk', 
             'http://example.org/organization/hefce.ac.uk/person/1', 
             None, 
             None, 
             None, 
             None, 
             None, 
             None, 
             'http://example.org/organization/hefce.ac.uk', 
             'http://example.org/organization/hefce.ac.uk/person/2', 
             'http://example.org/organization/hefce.ac.uk/post/90115', 
             None, 
             None,
             None, 
             None, 
             None, 
             None, 
             'http://example.org/organization/hefce.ac.uk',
             'http://example.org/organization/hefce.ac.uk/post/90115',
             None,
             None,
             None, 
             None, 
             None, 
             None, 
             'http://example.org/organization/hefce.ac.uk'
             ]
            )
        
        # minimal mode
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='minimal'
                    )
        #print(json_ld)
        
        #print(json_ld[0])
        self.assertEqual(
            json_ld[0],
            {'@id': 'http://example.org/organization/hefce.ac.uk/post/90115', 
             'dc:identifier': '90115', 
             'http://example.org/gov.uk/def/grade': 'SCS1A', 
             'http://example.org/gov.uk/def/job': 'Deputy Chief Executive', 
             'org:reportsTo': 'http://example.org/organization/hefce.ac.uk/post/90334', 
             'http://example.org/gov.uk/def/profession': 'Finance', 
             'org:postIn': 'http://example.org/organization/hefce.ac.uk', 
             'org:heldBy': {
                 '@id': 'http://example.org/organization/hefce.ac.uk/person/1', 
                 'foaf:name': 'Steve Egan'
                 }
             }
            )
        
        #print(json_ld[1])
        self.assertEqual(
            json_ld[1],
            {'@id': 'http://example.org/organization/hefce.ac.uk/post/90334', 
             'dc:identifier': '90334', 
             'http://example.org/gov.uk/def/grade': 'SCS4', 
             'http://example.org/gov.uk/def/job': 'Chief Executive', 
             'http://example.org/gov.uk/def/profession': 'Policy', 
             'org:postIn': 'http://example.org/organization/hefce.ac.uk', 
             'org:heldBy': {
                 '@id': 'http://example.org/organization/hefce.ac.uk/person/2', 
                 'foaf:name': 'Sir Alan Langlands'
                 }
             }
            )
        
        #print(json_ld[2])
        self.assertEqual(
            json_ld[2],
            {'org:reportsTo': 'http://example.org/organization/hefce.ac.uk/post/90115', 
             'http://example.org/gov.uk/def/grade': '4', 
             'http://example.org/gov.uk/def/min_pay': 17426, 
             'http://example.org/gov.uk/def/max_pay': 20002, 
             'http://example.org/gov.uk/def/job': 'Administrator', 
             'http://example.org/gov.uk/def/number_of_posts': 8.67, 
             'http://example.org/gov.uk/def/profession': 'Operational Delivery', 
             'org:postIn': 'http://example.org/organization/hefce.ac.uk'
             }
            )
        
        #print(json_ld[3])
        self.assertEqual(
            json_ld[3],
            {'org:reportsTo': 'http://example.org/organization/hefce.ac.uk/post/90115', 
             'http://example.org/gov.uk/def/grade': '5', 
             'http://example.org/gov.uk/def/min_pay': 19546, 
             'http://example.org/gov.uk/def/max_pay': 22478, 
             'http://example.org/gov.uk/def/job': 'Administrator', 
             'http://example.org/gov.uk/def/number_of_posts': 0.5, 
             'http://example.org/gov.uk/def/profession': 'Operational Delivery', 
             'org:postIn': 'http://example.org/organization/hefce.ac.uk'
             }
            )
      
        
        
#%% TESTS - Generating RDF from Tabular Data on the Web

class TestSection7(unittest.TestCase):
    ""
    
    def test_section_7_1_simple_example(self):
        ""
        
        logging.info('TEST: test_section_7_1_simple_example')
        
        fp=r'generating_rdf_from_tabular_data_example_files/countries.csv'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(fp)
        
        rdf_ntriples=csvw_functions.get_rdf_from_annotated_table_group(
                annotated_table_group_dict=annotated_table_group_dict,
                mode='minimal'
                )
        rdf_ntriples=rdf_ntriples.replace(
            annotated_table_group_dict['tables'][0]['url'],
            'http://example.org/countries.csv'
            )
        g = Graph().parse(data=rdf_ntriples, format='ntriples')
        
        # check Andorra node
        n=Literal('Andorra',datatype=URIRef(XSD.string))
        bnode=list(g.subjects(object=n))[0]
        po=list(g.predicate_objects(subject=bnode))
        x=sorted([(p.n3(),o.value) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://example.org/countries.csv#countryCode>', 'AD'), 
             ('<http://example.org/countries.csv#latitude>', '42.5'), 
             ('<http://example.org/countries.csv#longitude>', '1.6'), 
             ('<http://example.org/countries.csv#name>', 'Andorra')
             ]
            )
        
        # check United Arab Emirates node
        n=Literal('United Arab Emirates',datatype=URIRef(XSD.string))
        bnode=list(g.subjects(object=n))[0]
        po=list(g.predicate_objects(subject=bnode))
        x=sorted([(p.n3(),o.value) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://example.org/countries.csv#countryCode>', 'AE'), 
             ('<http://example.org/countries.csv#latitude>', '23.4'), 
             ('<http://example.org/countries.csv#longitude>', '53.8'), 
             ('<http://example.org/countries.csv#name>', 'United Arab Emirates')
             ]
            )
        
        # check Afghanistan node
        n=Literal('Afghanistan',datatype=URIRef(XSD.string))
        bnode=list(g.subjects(object=n))[0]
        po=list(g.predicate_objects(subject=bnode))
        x=sorted([(p.n3(),o.value) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://example.org/countries.csv#countryCode>', 'AF'), 
             ('<http://example.org/countries.csv#latitude>', '33.9'), 
             ('<http://example.org/countries.csv#longitude>', '67.7'), 
             ('<http://example.org/countries.csv#name>', 'Afghanistan')
             ]
            )
        
    
    def test_section_7_2_Example_with_single_table_and_rich_annotations(self):
        ""
        logging.info('TEST: test_section_7_2_Example_with_single_table_and_rich_annotations')
        
        fp=r'generating_rdf_from_tabular_data_example_files/tree-ops-ext.csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(fp)
        
        rdf_ntriples=csvw_functions.get_rdf_from_annotated_table_group(
                annotated_table_group_dict=annotated_table_group_dict,
                mode='minimal'
                )
        rdf_ntriples=rdf_ntriples.replace(
            annotated_table_group_dict['tables'][0]['url'],
            'http://example.org/tree-ops-ext.csv'
            )
        g = Graph().parse(data=rdf_ntriples, format='ntriples')
        
        # check gid-1 node
        n=URIRef('http://example.org/tree-ops-ext#gid-1')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://example.org/tree-ops-ext.csv#dbh>', '"11"^^<http://www.w3.org/2001/XMLSchema#integer>'), 
             ('<http://example.org/tree-ops-ext.csv#inventory_date>', '"2010-10-18"^^<http://www.w3.org/2001/XMLSchema#date>'), 
             ('<http://example.org/tree-ops-ext.csv#kml>', '"<Point><coordinates>-122.156485,37.440963</coordinates></Point>"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral>'), 
             ('<http://example.org/tree-ops-ext.csv#on_street>', '"ADDISON AV"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#protected>', '"false"^^<http://www.w3.org/2001/XMLSchema#boolean>'), 
             ('<http://example.org/tree-ops-ext.csv#species>', '"Celtis australis"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#trim_cycle>', '"Large Tree Routine Prune"@en')
             ]
            )
        
        # check gid-2 node
        n=URIRef('http://example.org/tree-ops-ext#gid-2')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://example.org/tree-ops-ext.csv#dbh>', '"11"^^<http://www.w3.org/2001/XMLSchema#integer>'), 
             ('<http://example.org/tree-ops-ext.csv#inventory_date>', '"2010-06-02"^^<http://www.w3.org/2001/XMLSchema#date>'), 
             ('<http://example.org/tree-ops-ext.csv#kml>', '"<Point><coordinates>-122.156749,37.440958</coordinates></Point>"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral>'), 
             ('<http://example.org/tree-ops-ext.csv#on_street>', '"EMERSON ST"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#protected>', '"false"^^<http://www.w3.org/2001/XMLSchema#boolean>'), 
             ('<http://example.org/tree-ops-ext.csv#species>', '"Liquidambar styraciflua"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#trim_cycle>', '"Large Tree Routine Prune"@en')
             ]
            )
        
        # check gid-6 node
        n=URIRef('http://example.org/tree-ops-ext#gid-6')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://example.org/tree-ops-ext.csv#comments>', '"beware of BEES"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#comments>', '"cavity or decay"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#comments>', '"codominant leaders"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#comments>', '"included bark"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#comments>', '"large leader or limb decay"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#comments>', '"previous failure root damage"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#comments>', '"root decay"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#comments>', '"trunk decay"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#dbh>', '"29"^^<http://www.w3.org/2001/XMLSchema#integer>'), 
             ('<http://example.org/tree-ops-ext.csv#inventory_date>', '"2010-06-01"^^<http://www.w3.org/2001/XMLSchema#date>'), 
             ('<http://example.org/tree-ops-ext.csv#kml>', '"<Point><coordinates>-122.156299,37.441151</coordinates></Point>"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral>'), 
             ('<http://example.org/tree-ops-ext.csv#on_street>', '"ADDISON AV"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#protected>', '"true"^^<http://www.w3.org/2001/XMLSchema#boolean>'), 
             ('<http://example.org/tree-ops-ext.csv#species>', '"Robinia pseudoacacia"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/tree-ops-ext.csv#trim_cycle>', '"Large Tree Routine Prune"@en')
             ]
            )
        
        
    def test_section_7_3_Example_with_single_table_and_using_virtual_columns_to_produce_multiple_subjects_per_row(self):
        ""
        logging.info('TEST: test_section_7_3_Example_with_single_table_and_using_virtual_columns_to_produce_multiple_subjects_per_row')
        
        fp=r'generating_rdf_from_tabular_data_example_files/events-listing.csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(fp)
        
        rdf_ntriples=csvw_functions.get_rdf_from_annotated_table_group(
                annotated_table_group_dict=annotated_table_group_dict,
                mode='minimal'
                )
        rdf_ntriples=rdf_ntriples.replace(
            annotated_table_group_dict['tables'][0]['url'],
            'http://example.org/events-listing.csv'
            )
        g = Graph().parse(data=rdf_ntriples, format='ntriples')
        #print(g.serialize(format='ttl'))
        
        # check event-1 node
        n=URIRef('http://example.org/events-listing.csv#event-1')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://schema.org/location>', '<http://example.org/events-listing.csv#place-1>'), 
             ('<http://schema.org/name>', '"B.B. King"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://schema.org/offers>', '<http://example.org/events-listing.csv#offer-1>'), 
             ('<http://schema.org/startDate>', '"2014-04-12T19:30:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>'), 
             ('<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>', '<http://schema.org/MusicEvent>')
             ]
            )
        
        # check place-1 node
        n=URIRef('http://example.org/events-listing.csv#place-1')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://schema.org/address>', '"79 Washington St., Providence, RI"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://schema.org/name>', '"Lupoâ€™s Heartbreak Hotel"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>', '<http://schema.org/Place>')
             ]
            )
        
        # check offer-1 node
        n=URIRef('http://example.org/events-listing.csv#offer-1')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://schema.org/url>', '"https://www.etix.com/ticket/1771656"^^<http://www.w3.org/2001/XMLSchema#anyURI>'), 
             ('<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>', '<http://schema.org/Offer>')]
            )
        
        # check event-2 node
        n=URIRef('http://example.org/events-listing.csv#event-2')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://schema.org/location>', '<http://example.org/events-listing.csv#place-2>'), 
             ('<http://schema.org/name>', '"B.B. King"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://schema.org/offers>', '<http://example.org/events-listing.csv#offer-2>'), 
             ('<http://schema.org/startDate>', '"2014-04-13T20:00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>'), 
             ('<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>', '<http://schema.org/MusicEvent>')
             ]
            )
        
        # check place-2 node
        n=URIRef('http://example.org/events-listing.csv#place-2')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://schema.org/address>', '"Lynn, MA, 01901"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://schema.org/name>', '"Lynn Auditorium"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>', '<http://schema.org/Place>')
             ]
            )
        
        # check offer-2 node
        n=URIRef('http://example.org/events-listing.csv#offer-2')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://schema.org/url>', '"http://frontgatetickets.com/venue.php?id=11766"^^<http://www.w3.org/2001/XMLSchema#anyURI>'), 
             ('<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>', '<http://schema.org/Offer>')
             ]
            )
        
        
    def test_section_7_4_Example_with_table_group_comprising_four_interrelated_tables(self):
        ""
        logging.info('TEST: test_section_7_4_Example_with_table_group_comprising_four_interrelated_tables')
        
        fp=r'generating_rdf_from_tabular_data_example_files/csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(fp)
        
        rdf_ntriples=csvw_functions.get_rdf_from_annotated_table_group(
                annotated_table_group_dict=annotated_table_group_dict,
                mode='minimal'
                )
        #print(os.path.dirname(annotated_table_group_dict['tables'][2]['url']))
        rdf_ntriples=rdf_ntriples.replace(
            os.path.dirname(annotated_table_group_dict['tables'][2]['url']),
            'http://example.org'
            )
        g = Graph().parse(data=rdf_ntriples, format='ntriples')
        #print(g.serialize(format='ttl'))
        
        # check <http://example.org/organization/hefce.ac.uk/post/90115> node
        n=URIRef('http://example.org/organization/hefce.ac.uk/post/90115')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://example.org/gov.uk/def/grade>', '"SCS1A"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/gov.uk/def/job>', '"Deputy Chief Executive"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/gov.uk/def/profession>', '"Finance"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://purl.org/dc/terms/identifier>', '"90115"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://www.w3.org/ns/org#heldBy>', '<http://example.org/organization/hefce.ac.uk/person/1>'), 
             ('<http://www.w3.org/ns/org#postIn>', '<http://example.org/organization/hefce.ac.uk>'), 
             ('<http://www.w3.org/ns/org#reportsTo>', '<http://example.org/organization/hefce.ac.uk/post/90334>')
             ]
            )
        
        # check <http://example.org/organization/hefce.ac.uk/person/1> node
        n=URIRef('http://example.org/organization/hefce.ac.uk/person/1')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://xmlns.com/foaf/0.1/name>', '"Steve Egan"^^<http://www.w3.org/2001/XMLSchema#string>')]
            )
        
        # check <http://example.org/organization/hefce.ac.uk/post/90334> node
        n=URIRef('http://example.org/organization/hefce.ac.uk/post/90334')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://example.org/gov.uk/def/grade>', '"SCS4"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/gov.uk/def/job>', '"Chief Executive"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/gov.uk/def/profession>', '"Policy"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://purl.org/dc/terms/identifier>', '"90334"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://www.w3.org/ns/org#heldBy>', '<http://example.org/organization/hefce.ac.uk/person/2>'), 
             ('<http://www.w3.org/ns/org#postIn>', '<http://example.org/organization/hefce.ac.uk>')
             ]
            )
        
        # check <http://example.org/organization/hefce.ac.uk/person/2> node
        n=URIRef('http://example.org/organization/hefce.ac.uk/person/2')
        po=list(g.predicate_objects(subject=n))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://xmlns.com/foaf/0.1/name>', '"Sir Alan Langlands"^^<http://www.w3.org/2001/XMLSchema#string>')]
            )
        
        # check grade 4 node
        n=Literal('4',datatype=URIRef(XSD.string))
        bnode=list(g.subjects(object=n))[0]
        po=list(g.predicate_objects(subject=bnode))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://example.org/gov.uk/def/grade>', '"4"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/gov.uk/def/job>', '"Administrator"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/gov.uk/def/max_pay>', '"20002"^^<http://www.w3.org/2001/XMLSchema#integer>'), 
             ('<http://example.org/gov.uk/def/min_pay>', '"17426"^^<http://www.w3.org/2001/XMLSchema#integer>'), 
             ('<http://example.org/gov.uk/def/number_of_posts>', '"8.67"^^<http://www.w3.org/2001/XMLSchema#double>'), 
             ('<http://example.org/gov.uk/def/profession>', '"Operational Delivery"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://www.w3.org/ns/org#postIn>', '<http://example.org/organization/hefce.ac.uk>'), 
             ('<http://www.w3.org/ns/org#reportsTo>', '<http://example.org/organization/hefce.ac.uk/post/90115>')
             ]
            )
        
        # check grade 5 node
        n=Literal('5',datatype=URIRef(XSD.string))
        bnode=list(g.subjects(object=n))[0]
        po=list(g.predicate_objects(subject=bnode))
        x=sorted([(p.n3(),o.n3()) for p,o in po])
        #print(x)
        self.assertEqual(
            x,
            [('<http://example.org/gov.uk/def/grade>', '"5"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/gov.uk/def/job>', '"Administrator"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://example.org/gov.uk/def/max_pay>', '"22478"^^<http://www.w3.org/2001/XMLSchema#integer>'), 
             ('<http://example.org/gov.uk/def/min_pay>', '"19546"^^<http://www.w3.org/2001/XMLSchema#integer>'), 
             ('<http://example.org/gov.uk/def/number_of_posts>', '"0.5"^^<http://www.w3.org/2001/XMLSchema#double>'), 
             ('<http://example.org/gov.uk/def/profession>', '"Operational Delivery"^^<http://www.w3.org/2001/XMLSchema#string>'), 
             ('<http://www.w3.org/ns/org#postIn>', '<http://example.org/organization/hefce.ac.uk>'), 
             ('<http://www.w3.org/ns/org#reportsTo>', '<http://example.org/organization/hefce.ac.uk/post/90115>')
             ]
            )
        

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
      
        
#%% TESTS - CSVW TEST SUITE

class TestCSVWTestSuite(unittest.TestCase):
    ""
    
    def test_json_test001(self):
        ""
        
        name='test001'
        fp_action=f'_github_w3c_csvw_tests/{name}.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(fp_action)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
            
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test005(self):
        ""
        
        name='test005'
        fp_action=f'_github_w3c_csvw_tests/{name}.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(fp_action)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
            
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test006(self):
        ""
        
        name='test006'
        fp_action=f'_github_w3c_csvw_tests/{name}.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(fp_action)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
            
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test007(self):
        ""
        
        name='test007'
        fp_action=f'_github_w3c_csvw_tests/{name}.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(fp_action)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
            
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test008(self):
        ""
        
        name='test008'
        fp_action=f'_github_w3c_csvw_tests/{name}.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(fp_action)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
            
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test009(self):
        ""
        
        name='test009'
        fp_action=f'_github_w3c_csvw_tests/{name}.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(fp_action)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
            
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test010(self):
        ""
        
        name='test010'
        fp_action=f'_github_w3c_csvw_tests/{name}.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(fp_action)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
            
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test011(self):
        ""
        
        name='test011'
        fp_action=f'_github_w3c_csvw_tests/{name}/tree-ops.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(fp_action)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}/tree-ops.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
    
    def test_json_test012(self):
        ""
        
        name='test012'
        fp_action=f'_github_w3c_csvw_tests/{name}/tree-ops.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(fp_action)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}/tree-ops.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test013(self):
        ""
        
        name='test013'
        fp_action='_github_w3c_csvw_tests/tree-ops.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action,
                overriding_metadata_file_path_or_url='_github_w3c_csvw_tests/test013-user-metadata.json',
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/tree-ops.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test014(self):
        ""
        
        name='test014'
        fp_action=f'_github_w3c_csvw_tests/{name}/tree-ops.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action,
                _link_header='<linked-metadata.json>; rel="describedby"; type="application/csvm+json"'
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}/tree-ops.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test015(self):
        ""
        
        name='test015'
        fp_action=f'_github_w3c_csvw_tests/{name}/tree-ops.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action,
                overriding_metadata_file_path_or_url=f'_github_w3c_csvw_tests/{name}/user-metadata.json',
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}/tree-ops.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test016(self):
        ""
        
        name='test016'
        fp_action=f'_github_w3c_csvw_tests/{name}/tree-ops.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action,
                _link_header='<linked-metadata.json>; rel="describedby"; type="application/csvm+json"'
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}/tree-ops.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test017(self):
        ""
        
        name='test017'
        fp_action=f'_github_w3c_csvw_tests/{name}/tree-ops.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action,
                _link_header='<linked-metadata.json>; rel="describedby"; type="application/csvm+json"'
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}/tree-ops.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
    
    def test_json_test018(self):
        ""
        
        name='test018'
        fp_action=f'_github_w3c_csvw_tests/{name}/tree-ops.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action,
                overriding_metadata_file_path_or_url=f'_github_w3c_csvw_tests/{name}/user-metadata.json'
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string=f'http://www.w3.org/2013/csvw/tests/{name}/tree-ops.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.assertEqual(
            json_ld,
            json_ld_result
            )
    
    
    def test_json_test023(self):
        ""
        
        name='test023'
        fp_action='_github_w3c_csvw_tests/tree-ops.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action,
                overriding_metadata_file_path_or_url=f'_github_w3c_csvw_tests/{name}-user-metadata.json'
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/tree-ops.csv'
                    )    
        #print(json_ld)
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.maxDiff=None
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test027(self):
        ""
        
        name='test027'
        fp_action='_github_w3c_csvw_tests/tree-ops.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action,
                overriding_metadata_file_path_or_url=f'_github_w3c_csvw_tests/{name}-user-metadata.json'
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='minimal',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/tree-ops.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.maxDiff=None
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test028(self):
        ""
        
        name='test028'
        fp_action='_github_w3c_csvw_tests/countries.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/countries.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.maxDiff=None
        self.assertEqual(
            json_ld,
            json_ld_result
            )
    
    
    def test_json_test029(self):
        ""
        
        name='test029'
        fp_action='_github_w3c_csvw_tests/countries.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='minimal',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/countries.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.maxDiff=None
        self.assertEqual(
            json_ld,
            json_ld_result
            )
    
    
    def test_json_test030(self):
        ""
        
        name='test030'
        fp_action='_github_w3c_csvw_tests/countries.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.maxDiff=None
        self.assertEqual(
            json_ld,
            json_ld_result
            )
    
    
    def test_json_test031(self):
        ""
        
        name='test031'
        fp_action='_github_w3c_csvw_tests/countries.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='minimal',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.maxDiff=None
        self.assertEqual(
            json_ld,
            json_ld_result
            )
    
    
    def test_json_test032(self):
        ""
        
        name='test032'
        fp_action=f'_github_w3c_csvw_tests/{name}/csv-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/%s/{table_name}.csv' % name
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.maxDiff=None
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test033(self):
        ""
        
        name='test033'
        fp_action=f'_github_w3c_csvw_tests/{name}/csv-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='minimal',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/%s/{table_name}.csv' % name
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.maxDiff=None
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test034(self):
        ""
        
        name='test034'
        fp_action=f'_github_w3c_csvw_tests/{name}/csv-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/%s/{table_name}.csv' % name
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.maxDiff=None
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
        
    def test_json_test035(self):
        ""
        
        name='test035'
        fp_action=f'_github_w3c_csvw_tests/{name}/csv-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='minimal',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/%s/{table_name}.csv' % name
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
        self.maxDiff=None
        self.assertEqual(
            json_ld,
            json_ld_result
            )
        
    
    def test_json_test036(self):
        ""
        
        name='test036'
        fp_action=f'_github_w3c_csvw_tests/{name}/tree-ops-ext.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/%s/{table_name}.csv' % name
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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


    def test_json_test037(self):
        ""
        
        name='test037'
        fp_action=f'_github_w3c_csvw_tests/{name}/tree-ops-ext.csv'
        fp_result=f'_github_w3c_csvw_tests/{name}/result.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_csv(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='minimal',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/%s/{table_name}.csv' % name
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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


    def test_json_test038(self):
        ""
        
        name='test038'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test039(self):
        ""
        
        name='test039'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        annotated_table_group_dict=\
            csvw_functions.get_annotated_table_group_from_metadata(
                fp_action
                )
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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


    def test_json_test040(self):
        ""
        
        name='test040'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test041(self):
        ""
        
        name='test041'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test042(self):
        ""
        
        name='test042'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test043(self):
        ""
        
        name='test043'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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


    def test_json_test044(self):
        ""
        
        name='test044'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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

    
    def test_json_test045(self):
        ""
        
        name='test045'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
    
    
    def test_json_test046(self):
        ""
        
        name='test046'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test047(self):
        ""
        # Note this only works if the default for aboutUrl is set to ''
        
        name='test047'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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


    def test_json_test048(self):
        ""
        # Note this only works if the default for propertyUrl is set to ''
        
        name='test048'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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


    def test_json_test049(self):
        ""
        # Note this only works if the default for valueUrl is set to ''
        
        name='test049'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test059(self):
        ""
        
        name='test059'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test060(self):
        ""
        
        name='test060'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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


    def test_json_test061(self):
        ""
        
        name='test061'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test062(self):
        ""
        
        name='test062'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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


    def test_json_test063(self):
        ""
        
        name='test066'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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


    def test_json_test065(self):
        ""
        
        name='test065'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test066(self):
        ""
        
        name='test066'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test067(self):
        ""
        
        name='test067'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test068(self):
        ""
        
        name='test068'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test069(self):
        ""
        
        name='test069'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test070(self):
        ""
        
        name='test070'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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


    def test_json_test071(self):
        ""
        
        name='test071'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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


    def test_json_test072(self):
        ""
        
        name='test072'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test073(self):
        ""
        
        name='test073'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test074(self):
        ""
        
        name='test074'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)
                
           
    def test_json_test075(self):
        ""
        
        name='test075'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
           
            
    def test_json_test076(self):
        ""
        
        name='test076'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
            
        
    def test_json_test077(self):
        ""
        
        name='test077'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)   
        
        
    def test_json_test078(self):
        ""
        
        name='test078'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)   
           
        
    def test_json_test079(self):
        ""
        
        name='test079'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)   
        
        
    def test_json_test080(self):
        ""
        
        name='test080'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)   
        
        
    def test_json_test081(self):
        ""
        
        name='test081'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)   
        
        
    def test_json_test082(self):
        ""
        
        name='test082'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)   
        
        
    def test_json_test083(self):
        ""
        
        name='test083'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)  
        
    
    def test_json_test084(self):
        ""
        
        name='test084'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)  
    
        
    def test_json_test085(self):
        ""
        
        name='test085'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)  
        
        
    def test_json_test086(self):
        ""
        
        name='test086'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)  
    
    
    def test_json_test087(self):
        ""
        
        name='test087'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)  
        
        
    def test_json_test088(self):
        ""
        
        name='test088'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)  
        
        
    def test_json_test089(self):
        ""
        
        name='test089'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)  
        
        
    def test_json_test090(self):
        ""
        
        name='test090'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)  
        
        
    def test_json_test093(self):
        ""
        
        name='test093'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test095(self):
        ""
        
        name='test095'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test097(self):
        ""
        
        name='test097'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test098(self):
        ""
        
        name='test098'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)  
        
        
    def test_json_test099(self):
        ""
        
        name='test099'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test100(self):
        ""
        
        name='test100'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test101(self):
        ""
        
        name='test101'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test102(self):
        ""
        
        name='test102'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        fp_result=f'_github_w3c_csvw_tests/{name}.json'
        
        with self.assertWarns(csvw_functions.PropertyNotValidWarning) as w:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
            self.assertEqual(
                len(w.warnings),
                1
                )
                
            #print(w.warnings[0].message)
        
        json_ld=\
            csvw_functions.get_json_ld_from_annotated_table_group(
                    annotated_table_group_dict,
                    mode='standard',
                    _replace_url_string='http://www.w3.org/2013/csvw/tests/{table_name}.csv'
                    )    
        json_ld['tables'][0]['@id']='http://www.w3.org/2013/csvw/tests/test102-metadata.json'  # replaced local file path with solution file path
        #print(json_ld, '\n', '---')
        
        with open(fp_result) as f:
            json_ld_result=json.load(f)
        #print(json_ld_result)
           
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
        
        
    def test_json_test103(self):
        ""
        
        name='test103'
        fp_action=f'_github_w3c_csvw_tests/{name}-metadata.json'
        
        with self.assertRaises(csvw_functions.MetadataValidationError) as e:
        
            annotated_table_group_dict=\
                csvw_functions.get_annotated_table_group_from_metadata(
                    fp_action
                    )
                
        #print(e.exception)  

        
if __name__=='__main__':
    
    unittest.main()
    
    #unittest.main(TestTopLevelFunctions,'test_get_embedded_metadata_from_csv')
    
    #unittest.main(TestSection5_5_2,'test_section_5_5_2_example_30')
    
    #unittest.main(TestCSVWTestSuite())
    
    #unittest.main(TestCSVWTestSuite,'test_json_test103')
    
    
    