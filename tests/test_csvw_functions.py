# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 09:59:59 2022

@author: cvskf
"""

import unittest
import os
import json
import datetime

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
 
    
#%% TESTS - Model for Tabular Data and Metadata


#%% Section 6.4 - Parsing Cells

class TestSection6_4(unittest.TestCase):
    ""
    

#%% Section 8 - Parsing Tabular Data

class TestSection8(unittest.TestCase):
    ""
    
    def test_section_8_2_1_Simple_Example(self):
        ""
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
            [{'und': ['GID']}, 
             {'und': ['On Street']}, 
             {'und': ['Species']}, 
             {'und': ['Trim Cycle']}, 
             {'und': ['Inventory Date']}]
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
            [{'und': '1'}, 
             {'und': 'ADDISON AV'}, 
             {'und': 'Celtis australis'}, 
             {'und': 'Large Tree Routine Prune'}, 
             {'und': '10/18/2010'}]
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
            [{'und': '2'}, 
             {'und': 'EMERSON ST'}, 
             {'und': 'Liquidambar styraciflua'}, 
             {'und': 'Large Tree Routine Prune'}, 
             {'und': '6/2/2010'}]
            )
        

    def test_section_8_2_1_1_Using_overriding_metadata(self):
        ""
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
            [{'und': ['GID']}, 
             {'und': ['On Street']}, 
             {'und': ['Species']}, 
             {'und': ['Trim Cycle']}, 
             {'und': ['Inventory Date']}]
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
            [{'und': ['GID']}, 
             {'und': ['On Street']}, 
             {'und': ['Species']}, 
             {'und': ['Trim Cycle']}, 
             {'und': ['Inventory Date']}]
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
            [{'und': '1'}, 
             {'und': 'ADDISON AV'}, 
             {'und': 'Celtis australis'}, 
             {'und': 'Large Tree Routine Prune'}, 
             datetime.date(2010, 10, 18)]
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
            [{'und': '2'}, 
             {'und': 'EMERSON ST'}, 
             {'und': 'Liquidambar styraciflua'}, 
             {'und': 'Large Tree Routine Prune'}, 
             datetime.date(2010, 6, 2)]
            )
        
    def test_section_8_2_1_2_Using_a_Metadata_File(self):
        ""
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
            [{'en': ['GID', 'Generic Identifier']}, 
             {'en': ['On Street']}, 
             {'en': ['Species']}, 
             {'en': ['Trim Cycle']}, 
             {'en': ['Inventory Date']}]
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
            [{'und': '1'}, 
             {'und': 'ADDISON AV'}, 
             {'und': 'Celtis australis'}, 
             {'und': 'Large Tree Routine Prune'}, 
             datetime.date(2010, 10, 18)]
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
            [{'und': '2'}, 
             {'und': 'EMERSON ST'}, 
             {'und': 'Liquidambar styraciflua'}, 
             {'und': 'Large Tree Routine Prune'}, 
             datetime.date(2010, 6, 2)]
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
            [{'und': '1'}, 
             {'und': 'ADDISON AV'}, 
             {'und': 'Celtis australis'}, 
             {'und': 'Large Tree Routine Prune'}, 
             {'und': '10/18/2010'}]
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
            [{'und': '2'}, 
             None, 
             {'und': 'Liquidambar styraciflua'}, 
             {'und': 'Large Tree Routine Prune'}, 
             None]
            )

    def test_section_8_2_3_1_Naive_Parsing(self):
        ""
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
            [{'und': ['#\tpublisher\tCity of Palo Alto']}]
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
            [{'und': '#\tupdated\t12/31/2010'}, 
             {'und': '#name\tGID\ton_street\tspecies\ttrim_cycle\tinventory_date'}, 
             {'und': '#datatype\tstring\tstring\tstring\tstring\tdate:M/D/YYYY'}, 
             {'und': 'GID\tOn Street\tSpecies\tTrim Cycle\tInventory Date'}, 
             {'und': '1\tADDISON AV\tCeltis australis\tLarge Tree Routine Prune\t10/18/2010'}, 
             {'und': '2\tEMERSON ST\tLiquidambar styraciflua\tLarge Tree Routine Prune\t6/2/2010'}]
            )  # NOTE, example in standard seems to have tabs replaced with spaces??
        
    
    def test_section_8_2_3_2_Parsing_with_Flags(self):
        ""
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
            [{'und': ['GID']},  
             {'und': ['On Street']},  
             {'und': ['Species']},
             {'und': ['Trim Cycle']}, 
             {'und': ['Inventory Date']}, 
             ]
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
            [{'und': ['GID']},  
             {'und': ['On Street']},  
             {'und': ['Species']},
             {'und': ['Trim Cycle']}, 
             {'und': ['Inventory Date']}, 
             ]
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
            [{'und': '1'}, 
             {'und': 'ADDISON AV'}, 
             {'und': 'Celtis australis'}, 
             {'und': 'Large Tree Routine Prune'}, 
             datetime.date(2010, 10, 18)]
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
            [{'und': '2'}, 
             {'und': 'EMERSON ST'}, 
             {'und': 'Liquidambar styraciflua'}, 
             {'und': 'Large Tree Routine Prune'}, 
             datetime.date(2010, 6, 2)]
            )
        
        
    def test_section_8_2_4_Parsing_Multiple_Header_Lines(self):
        ""
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
            [{'und': ['Organization', '#org']}, 
             {'und': ['Sector', '#sector']}, 
             {'und': ['Subsector', '#subsector']}, 
             {'und': ['Department', '#adm1']}, 
             {'und': ['Municipality', '#adm2']}]
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
    
#%% 6. Normalization

class TestSection6(unittest.TestCase):
    
    def test_section_6_1_example_41(self):
        ""
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
        



#%% TESTS - General Functions

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