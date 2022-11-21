# -*- coding: utf-8 -*-
"""
Created on Mon Aug 15 06:48:49 2022

@author: cvskf
"""

#from csvw_functions import csvw_functions
import csvw_functions

import unittest
import os
import json
import datetime
from rdflib import Graph, Literal, URIRef, XSD
import csv
import time
import warnings
import rdflib.compare

test_dir='_github_w3c_csvw_tests'


    

#%% ---TESTCASE - Generating JSON from Tabular Data on the Web---


class Test_JSON_Section_6(unittest.TestCase):
    ""
    # TO DO
    
    # REWORK THIS SECTION
    
    # CREATE SOLUTION JSON FILES AND LOAD THEM
    
    # AVOID USING _replace_strings
    
    # INSTEAD USE local_path_replacement_url
    

    
    def test_section_6_1_simple_example(self):
        ""
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_1/countries.csv'
        annotated_table_group_dict=\
            csvw_functions.create_annotated_table_group(
                fp
                )
            
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
        
        
        # ---minimal mode---
        
        json_ld=\
            csvw_functions.create_json_ld(
                    annotated_table_group_dict,
                    mode='minimal'
                    )
        #print(json_ld)
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_1/countries-minimal.json'
        with open(fp) as f:
            json_ld_solution=json.load(f)
        #print(json_ld_solution)
            
        compare_json(
            self,
            json_ld,
            json_ld_solution
            )
        
        self.assertEqual(
            json_ld,
            json_ld_solution
            )
            
                
        # --- standard mode ---
        
        json_ld=\
            csvw_functions.create_json_ld(
                    annotated_table_group_dict,
                    mode='standard',
                    local_path_replacement_url='http://example.org/'
                    #convert_local_path_to_example_dot_org=True
                    #_replace_strings=_replace_strings
                    )
        json_ld=csvw_functions.csvw_functions.replace_string(
            json_ld, 
            r'generating_json_from_tabular_data_example_files/section_6_1/', 
            ''
            )
        #print(json_ld)
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_1/countries-standard.json'
        with open(fp) as f:
            json_ld_solution=json.load(f)
        #print(json_ld_solution)
        
        compare_json(
            self,
            json_ld,
            json_ld_solution
            )
        
        self.assertEqual(
            json_ld,
            json_ld_solution
            )
        
        
        
    def test_section_6_2_Example_with_single_table_and_rich_annotations(self):
        ""
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_2/tree-ops-ext.csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.create_annotated_table_group(fp)
        
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        annotated_cells_list=[cell 
                              for row in annotated_rows_list 
                              for cell in row['cells']]
        
        self.maxDiff=None
        
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
             {'@value': '<Point><coordinates>-122.156485,37.440963</coordinates></Point>', '@type': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral'}, 
             {'@value': '2', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'EMERSON ST', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Liquidambar styraciflua', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Large Tree Routine Prune', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'en'}, 
             {'@value': 11, '@type': 'http://www.w3.org/2001/XMLSchema#integer'}, 
             {'@value': '2010-06-02', '@type': 'http://www.w3.org/2001/XMLSchema#date'}, 
             [], 
             {'@value': False, '@type': 'http://www.w3.org/2001/XMLSchema#boolean'}, 
             {'@value': '<Point><coordinates>-122.156749,37.440958</coordinates></Point>', '@type': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral'}, 
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
             {'@value': '<Point><coordinates>-122.156299,37.441151</coordinates></Point>', '@type': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral'}
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
        
        
        # ---minimal mode---
        
        json_ld=\
            csvw_functions.create_json_ld(
                    annotated_table_group_dict,
                    mode='minimal'
                    )
        #print(json_ld)
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_2/tree-ops-ext-minimal.json'
        with open(fp) as f:
            json_ld_solution=json.load(f)
        #print(json_ld_solution)
            
        compare_json(
            self,
            json_ld,
            json_ld_solution
            )
        
        self.assertEqual(
            json_ld,
            json_ld_solution
            )
            
                
        # --- standard mode ---
        
        json_ld=\
            csvw_functions.create_json_ld(
                    annotated_table_group_dict,
                    mode='standard',
                    local_path_replacement_url='http://example.org/'
                    #convert_local_path_to_example_dot_org=True
                    #_replace_strings=_replace_strings
                    )
        json_ld=csvw_functions.csvw_functions.replace_string(
            json_ld, 
            r'generating_json_from_tabular_data_example_files/section_6_2/', 
            ''
            )
        #print(json_ld)
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_2/tree-ops-ext-standard.json'
        with open(fp) as f:
            json_ld_solution=json.load(f)
        #print(json_ld_solution)
        
        compare_json(
            self,
            json_ld,
            json_ld_solution
            )
        
        self.assertEqual(
            json_ld,
            json_ld_solution
            )
        
        

    def test_section_6_3_Example_with_single_table_and_using_virtual_columns_to_produce_multiple_subjects_per_row(self):
        ""
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_3/events-listing.csv-metadata.json'
        
        annotated_table_group_dict=\
            csvw_functions.create_annotated_table_group(fp)
        
        annotated_table_dict=annotated_table_group_dict['tables'][0]
        annotated_columns_list=annotated_table_dict['columns']
        annotated_rows_list=annotated_table_dict['rows']
        annotated_cells_list=[cell 
                              for row in annotated_rows_list 
                              for cell in row['cells']]
                
        self.maxDiff=None
        
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
             'Lupo’s Heartbreak Hotel', 
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
             {'@value': 'Lupo’s Heartbreak Hotel', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '79 Washington St., Providence, RI', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'https://www.etix.com/ticket/1771656', '@type': 'http://www.w3.org/2001/XMLSchema#anyURI'}, 
             None, 
             None, 
             None, 
             None, 
             None, 
             {'@value': 'B.B. King', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': '2014-04-13T20:00:00', '@type': 'http://www.w3.org/2001/XMLSchema#dateTime'}, 
             {'@value': 'Lynn Auditorium', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'Lynn, MA, 01901', '@type': 'http://www.w3.org/2001/XMLSchema#string', '@language': 'und'}, 
             {'@value': 'http://frontgatetickets.com/venue.php?id=11766', '@type': 'http://www.w3.org/2001/XMLSchema#anyURI'}, 
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
        #print([cell['valueURL'] for cell in annotated_cells_list])
        x=os.path.join(os.getcwd(),
                       'generating_json_from_tabular_data_example_files/section_6_3')
        x=x.replace('\\','/').replace(' ','%20')
        x='file:///'+x
        # print([cell['valueURL'].replace(x,'http://example.org')
        #        if (not cell['valueURL'] is None and cell['valueURL'].startswith('file'))
        #        else cell['valueURL']
        #        for cell in annotated_cells_list])
        self.assertEqual(
            [cell['valueURL'].replace(x,'http://example.org')
                   if (not cell['valueURL'] is None and cell['valueURL'].startswith('file'))
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
             'http://example.org/events-listing.csv#place-1',  
             'http://example.org/events-listing.csv#offer-1',  
             None, 
             None, 
             None, 
             None, 
             None, 
             'http://schema.org/MusicEvent', 
             'http://schema.org/Place', 
             'http://schema.org/Offer', 
             'http://example.org/events-listing.csv#place-2', 
             'http://example.org/events-listing.csv#offer-2'  
             ]
            )
        
        
        # ---minimal mode---
        
        json_ld=\
            csvw_functions.create_json_ld(
                    annotated_table_group_dict,
                    mode='minimal',
                    local_path_replacement_url='http://example.org/'
                    )
        json_ld=csvw_functions.csvw_functions.replace_string(
            json_ld, 
            r'generating_json_from_tabular_data_example_files/section_6_3/', 
            ''
            )
        #print(json_ld)
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_3/events-listing-minimal.json'
        with open(fp) as f:
            json_ld_solution=json.load(f)
        # fix errors...
        #... type
        json_ld_solution=csvw_functions.csvw_functions.replace_string(
            json_ld_solution, 
            'schema:offer', 
            'schema:offers'
            )
        #...issue with apostrophe
        json_ld_solution=csvw_functions.csvw_functions.replace_string(
            json_ld_solution, 
            'â€™', 
            "’"
            )
        #...dict key has wrong name
        for x in json_ld_solution:
            x['schema:offers']['schema:url']=x['schema:offers'].pop('schema:offers')
        #print(json_ld_solution)
            
        compare_json(
            self,
            json_ld,
            json_ld_solution
            )
        
        self.assertEqual(
            json_ld,
            json_ld_solution
            )
            
        
        # --- standard mode ---
        
        json_ld=\
            csvw_functions.create_json_ld(
                    annotated_table_group_dict,
                    mode='standard',
                    local_path_replacement_url='http://example.org/'
                    #convert_local_path_to_example_dot_org=True
                    #_replace_strings=_replace_strings
                    )
        json_ld=csvw_functions.csvw_functions.replace_string(
            json_ld, 
            r'generating_json_from_tabular_data_example_files/section_6_3/', 
            ''
            )
        #print(json_ld)
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_3/events-listing-standard.json'
        with open(fp) as f:
            json_ld_solution=json.load(f)
        # fix errors...
        #...issue with apostrophe
        json_ld_solution=csvw_functions.csvw_functions.replace_string(
            json_ld_solution, 
            'â€™', 
            "’"
            )
        #print(json_ld_solution)
        
        compare_json(
            self,
            json_ld,
            json_ld_solution
            )
        
        self.assertEqual(
            json_ld,
            json_ld_solution
            )
        
        
    def test_section_6_4_Example_with_table_group_comprising_four_interrelated_tables(self):
        ""
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_4/csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.create_annotated_table_group(fp)
            
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
             [{'@value': 'Payscale Minimum (£)', '@language': 'und'}], 
             [{'@value': 'Payscale Maximum (£)', '@language': 'und'}], 
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
        
        
        # ---minimal mode---
        
        json_ld=\
            csvw_functions.create_json_ld(
                    annotated_table_group_dict,
                    mode='minimal'
                    )
        #print(json_ld)
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_4/public-sector-roles-and-salaries-minimal.json'
        with open(fp) as f:
            json_ld_solution=json.load(f)
        #print(json_ld_solution)
            
        compare_json(
            self,
            json_ld,
            json_ld_solution
            )
        
        self.assertEqual(
            json_ld,
            json_ld_solution
            )
            
                
        # --- standard mode ---
        
        json_ld=\
            csvw_functions.create_json_ld(
                    annotated_table_group_dict,
                    mode='standard',
                    local_path_replacement_url='http://example.org/'
                    #convert_local_path_to_example_dot_org=True
                    #_replace_strings=_replace_strings
                    )
        json_ld=csvw_functions.csvw_functions.replace_string(
            json_ld, 
            r'generating_json_from_tabular_data_example_files/section_6_4/', 
            ''
            )
        #print(json_ld)
        
        fp=r'generating_json_from_tabular_data_example_files/section_6_4/public-sector-roles-and-salaries-standard.json'
        with open(fp) as f:
            json_ld_solution=json.load(f)
        #print(json_ld_solution)
        
        compare_json(
            self,
            json_ld,
            json_ld_solution
            )
        
        self.assertEqual(
            json_ld,
            json_ld_solution
            )
        
        

#%% ---TESTCASE - Generating RDF from Tabular Data on the Web---

class Test_RDF_Section_7(unittest.TestCase):
    ""
    
    def test_section_7_1_simple_example(self):
        ""
        
        
        fp=r'generating_rdf_from_tabular_data_example_files/section_7_1/countries.csv'
        annotated_table_group_dict=\
            csvw_functions.create_annotated_table_group(
                fp
                )
        #print(csvw_functions.display_annotated_table_group_dict(annotated_table_group_dict))
        
        # minimal mode
        
        rdf_ntriples=\
            csvw_functions.create_rdf(
                annotated_table_group_dict,
                mode='minimal',
                local_path_replacement_url = 'http://example.org/'
                )
            
        rdf_ntriples=rdf_ntriples.replace(
            r'generating_rdf_from_tabular_data_example_files/section_7_1/',
            ''
            )
        #print(rdf_ntriples)
            
        g1 = Graph().parse(data=rdf_ntriples, format='ntriples')
        #print(g1.serialize(format='ttl'))
        
        g2=Graph()
        g_solution=Graph().parse(
            r'generating_rdf_from_tabular_data_example_files/section_7_1/countries-minimal.ttl',
            format='ttl'
            )
        for s,p,o in g_solution:
            if isinstance(o,Literal) and o.datatype is None:
                o2=Literal(o,datatype=XSD.string)
            else:
                o2=o
            g2.add((s,p,o2))    
        
        compare_rdf(self,g1,g2)
        
        
        # standard mode
        
        rdf_ntriples=\
            csvw_functions.create_rdf(
                annotated_table_group_dict,
                mode='standard',
                local_path_replacement_url = 'http://example.org/'
                )
        rdf_ntriples=rdf_ntriples.replace(
            r'generating_rdf_from_tabular_data_example_files/section_7_1/',
            ''
            )
        #print(rdf_ntriples)
        
        g1 = Graph().parse(data=rdf_ntriples, format='ntriples')
        #print(g1.serialize(format='ttl'))
        
        g2=Graph()
        g_solution=Graph().parse(
            r'generating_rdf_from_tabular_data_example_files/section_7_1/countries-standard.ttl',
            format='ttl'
            )
        for s,p,o in g_solution:
            if isinstance(o,Literal) and o.datatype is None:
                o2=Literal(o,datatype=XSD.string)
            else:
                o2=o
            g2.add((s,p,o2))    
        #print(g2.serialize(format='ttl'))
        
        compare_rdf(self,g1,g2)
        
        
    
    def test_section_7_2_Example_with_single_table_and_rich_annotations(self):
        ""
        
        fp=r'generating_rdf_from_tabular_data_example_files/section_7_2/tree-ops-ext.csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.create_annotated_table_group(
                fp
                )
        #print(csvw_functions.display_annotated_table_group_dict(annotated_table_group_dict))
        
        # minimal mode
        
        rdf_ntriples=\
            csvw_functions.create_rdf(
                annotated_table_group_dict,
                mode='minimal',
                local_path_replacement_url = 'http://example.org/'
                )
            
        rdf_ntriples=rdf_ntriples.replace(
            r'generating_rdf_from_tabular_data_example_files/section_7_2/',
            ''
            )
        #print(rdf_ntriples)
            
        g1 = Graph().parse(data=rdf_ntriples, format='ntriples')
        #print(g1.serialize(format='ttl'))
        
        g2=Graph()
        g_solution=Graph().parse(
            r'generating_rdf_from_tabular_data_example_files/section_7_2/tree-ops-ext-minimal.ttl',
            format='ttl'
            )
        for s,p,o in g_solution:
            if isinstance(o,Literal) and o.datatype is None:
                o2=Literal(o,datatype=XSD.string)
            else:
                o2=o
            g2.add((s,p,o2))    
        
        compare_rdf(self,g1,g2)
        
        
        # standard mode
        
        rdf_ntriples=\
            csvw_functions.create_rdf(
                annotated_table_group_dict,
                mode='standard',
                local_path_replacement_url = 'http://example.org/'
                )
        rdf_ntriples=rdf_ntriples.replace(
            r'generating_rdf_from_tabular_data_example_files/section_7_2/',
            ''
            )
        #print(rdf_ntriples)
        
        g1 = Graph().parse(data=rdf_ntriples, format='ntriples')
        #print(g1.serialize(format='ttl'))
        
        g2=Graph()
        g_solution=Graph().parse(
            r'generating_rdf_from_tabular_data_example_files/section_7_2/tree-ops-ext-standard.ttl',
            format='ttl'
            )
        for s,p,o in g_solution:
            if isinstance(o,Literal) and o.datatype is None:
                o2=Literal(o,datatype=XSD.string)
            else:
                o2=o
            g2.add((s,p,o2))    
        #print(g2.serialize(format='ttl'))
        
        compare_rdf(self,g1,g2)
            
            
        
        
    def test_section_7_3_Example_with_single_table_and_using_virtual_columns_to_produce_multiple_subjects_per_row(self):
        ""
        
        fp=r'generating_rdf_from_tabular_data_example_files/section_7_3/events-listing.csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.create_annotated_table_group(
                fp
                )
        #print(csvw_functions.display_annotated_table_group_dict(annotated_table_group_dict))
        
        # minimal mode
        
        rdf_ntriples=\
            csvw_functions.create_rdf(
                annotated_table_group_dict,
                mode='minimal',
                local_path_replacement_url = 'http://example.org/'
                )
            
        rdf_ntriples=rdf_ntriples.replace(
            r'generating_rdf_from_tabular_data_example_files/section_7_3/',
            ''
            )
        #print(rdf_ntriples)
            
        g1 = Graph().parse(data=rdf_ntriples, format='ntriples')
        #print(g1.serialize(format='ttl'))
        
        g2=Graph()
        g_solution=Graph().parse(
            r'generating_rdf_from_tabular_data_example_files/section_7_3/events-listing-minimal.ttl',
            format='ttl'
            )
        for s,p,o in g_solution:
            if isinstance(o,Literal) and o.datatype is None:
                o2=Literal(o,datatype=XSD.string)
            else:
                o2=o
            g2.add((s,p,o2))    
        
        compare_rdf(self,g1,g2)
        
        
        # standard mode
        
        rdf_ntriples=\
            csvw_functions.create_rdf(
                annotated_table_group_dict,
                mode='standard',
                local_path_replacement_url = 'http://example.org/'
                )
        rdf_ntriples=rdf_ntriples.replace(
            r'generating_rdf_from_tabular_data_example_files/section_7_3/',
            ''
            )
        #print(rdf_ntriples)
        
        g1 = Graph().parse(data=rdf_ntriples, format='ntriples')
        #print(g1.serialize(format='ttl'))
        
        g2=Graph()
        g_solution=Graph().parse(
            r'generating_rdf_from_tabular_data_example_files/section_7_3/events-listing-standard.ttl',
            format='ttl'
            )
        for s,p,o in g_solution:
            if isinstance(o,Literal) and o.datatype is None:
                o2=Literal(o,datatype=XSD.string)
            else:
                o2=o
            g2.add((s,p,o2))    
        #print(g2.serialize(format='ttl'))
        
        compare_rdf(self,g1,g2)
            
        
        
    def test_section_7_4_Example_with_table_group_comprising_four_interrelated_tables(self):
        ""
        
        fp=r'generating_rdf_from_tabular_data_example_files/section_7_4/csv-metadata.json'
        annotated_table_group_dict=\
            csvw_functions.create_annotated_table_group(
                fp
                )
        #print(csvw_functions.display_annotated_table_group_dict(annotated_table_group_dict))
        
        # minimal mode
        
        rdf_ntriples=\
            csvw_functions.create_rdf(
                annotated_table_group_dict,
                mode='minimal',
                local_path_replacement_url = 'http://example.org/'
                )
            
        rdf_ntriples=rdf_ntriples.replace(
            r'generating_rdf_from_tabular_data_example_files/section_7_4/',
            ''
            )
        #print(rdf_ntriples)
            
        g1 = Graph().parse(data=rdf_ntriples, format='ntriples')
        #print(g1.serialize(format='ttl'))
        
        g2=Graph()
        g_solution=Graph().parse(
            r'generating_rdf_from_tabular_data_example_files/section_7_4/public-sector-roles-and-salaries-minimal.ttl',
            format='ttl'
            )
        for s,p,o in g_solution:
            if isinstance(o,Literal) and o.datatype is None:
                o2=Literal(o,datatype=XSD.string)
            else:
                o2=o
            g2.add((s,p,o2))    
        
        compare_rdf(self,g1,g2)
        
        
        # standard mode
        
        rdf_ntriples=\
            csvw_functions.create_rdf(
                annotated_table_group_dict,
                mode='standard',
                local_path_replacement_url = 'http://example.org/'
                )
        rdf_ntriples=rdf_ntriples.replace(
            r'generating_rdf_from_tabular_data_example_files/section_7_4/',
            ''
            )
        #print(rdf_ntriples)
        
        g1 = Graph().parse(data=rdf_ntriples, format='ntriples')
        #print(g1.serialize(format='ttl'))
        
        g2=Graph()
        g_solution=Graph().parse(
            r'generating_rdf_from_tabular_data_example_files/section_7_4/public-sector-roles-and-salaries-standard.ttl',
            format='ttl'
            )
        for s,p,o in g_solution:
            if isinstance(o,Literal) and o.datatype is None:
                o2=Literal(o,datatype=XSD.string)
            else:
                o2=o
            g2.add((s,p,o2))    
        #print(g2.serialize(format='ttl'))
        
        compare_rdf(self,g1,g2)
        
        
        


#%% ---TESTCASE - W3C CSVW Test Suite---

# This section runs the tests described in the W3C CSVW Test Cases 
# as provided here: https://w3c.github.io/csvw/tests/

class Test_W3C_CSVW_Test_Cases(unittest.TestCase):
    ""
    
    def test_W3C_CSVW_RDF_test_cases(self):
        ""
        
        warnings.filterwarnings("ignore",category=UserWarning)  # warnings not printed out
        
        #p=True
        p=False
    
        with open(os.path.join(test_dir,'manifest-rdf.jsonld')) as f:
            
            manifest=json.load(f)
            
        # loop through json tests
        for i,entry in enumerate(manifest['entries']):
            
            #if not i==23: continue
            
            #if p: print(i)
            
            #if p: print('-manifest-entry',entry)
            
            
            action_fp=os.path.join(test_dir,entry['action'])
            
            if 'result' in entry:
                result_fp=os.path.join(test_dir,entry['result'])
            
            # overriding metadata option
            if 'metadata' in entry['option']:
                overriding_metadata_file_path_or_url=\
                    os.path.join(test_dir,entry['option']['metadata'])
            else:
                overriding_metadata_file_path_or_url=None
                
            # Link Header
            _link_header=entry.get('httpLink')
            
            # well known text
            _well_known_text='{+url}-metadata.json\ncsv-metadata.json\n{+url}.json\ncsvm.json'
            
            # validate option
            validate=False
            
            #raise Exception
            
            #with warnings.catch_warnings() as w:
                
            if entry['type']=='csvt:ToRdfTest':
                
                annotated_table_group_dict=\
                    csvw_functions.create_annotated_table_group(
                            action_fp,
                            overriding_metadata_file_path_or_url,
                            validate=validate,
                            _link_header=_link_header,
                            _well_known_text=_well_known_text,
                            _save_intermediate_and_final_outputs_to_file=True,
                            #_print_intermediate_outputs=p
                            )
                    
            elif entry['type']=='csvt:ToRdfTestWithWarnings':    
            
                with self.assertWarns(UserWarning):
            
                    annotated_table_group_dict=\
                        csvw_functions.create_annotated_table_group(
                                action_fp,
                                overriding_metadata_file_path_or_url,
                                validate=validate,
                                _link_header=_link_header,
                                _well_known_text=_well_known_text,
                                _save_intermediate_and_final_outputs_to_file=True,
                                #_print_intermediate_outputs=p
                                )
                        
            elif entry['type']=='csvt:NegativeRdfTest':    
            
                with self.assertRaises(csvw_functions.CSVWError):
            
                    annotated_table_group_dict=\
                        csvw_functions.create_annotated_table_group(
                                action_fp,
                                overriding_metadata_file_path_or_url,
                                validate=validate,
                                _link_header=_link_header,
                                _save_intermediate_and_final_outputs_to_file=True,
                                #_print_intermediate_outputs=p
                                )
                        
            else:
                
                raise Exception(entry['type'])
                    
            #print(csvw_functions.display_annotated_table_group_dict(annotated_table_group_dict))
                
             
            # mode option
            if entry['option'].get('minimal') is True:
                mode='minimal'
            else:
                mode='standard'
                
                
            rdf_ntriples=\
                csvw_functions.create_rdf(
                    annotated_table_group_dict,
                    mode=mode,
                    )
            #print(rdf_ntriples)
                
            g1 = Graph().parse(data=rdf_ntriples, format='ntriples')
            #print(g1.serialize(format='ttl'))
            #g1.serialize('g1.ttl',format='ttl')
            
            # fix error where property full uris are in the solution file
            #if entry['id'] in ['manifest-rdf#test100','manifest-rdf#test263']:
                
            with open(result_fp) as f:
                result_text=f.read()
            if 'http://www.w3.org/2013/csvw/tests/' in result_text:
                result_text=result_text.replace('http://www.w3.org/2013/csvw/tests/','')
                result_fp=f'{os.path.splitext(result_fp)[0]}_TEMP_SKF.ttl'
                #print(result_fp_temp)
                with open(result_fp,'w') as f:
                    f.write(result_text)
                
            g2=Graph()
            g_solution=Graph().parse(
                result_fp,
                format='ttl'
                )
            for s,p,o in g_solution:
                if isinstance(o,Literal) and o.datatype is None and o.language is None:
                    o2=Literal(o,datatype=XSD.string)
                else:
                    o2=o
                g2.add((s,p,o2))   
            #print(g2.serialize(format='ttl'))
            #g2.serialize('g2.ttl',format='ttl')
            
            compare_rdf(self,g1,g2)
                
        
            #break
        
        warnings.filterwarnings("always",category=UserWarning)  # warnings always printed out
        
    
    
    def test_W3C_CSVW_JSON_test_cases(self):
        ""
        
        warnings.filterwarnings("ignore",category=UserWarning)  # warnings not printed out
        
        p=False
        #p=True
        
        with open(os.path.join(test_dir,'manifest-json.jsonld')) as f:
            
            manifest=json.load(f)
            
        # loop through json tests
        for i,entry in enumerate(manifest['entries']):
            
            #if not i==23: continue
            
            if p: print(i)
            
            if p: print('-manifest-entry',entry)
            
            
            action_fp=os.path.join(test_dir,entry['action'])
            
            if 'result' in entry:
                result_fp=os.path.join(test_dir,entry['result'])
            
            # overriding metadata option
            if 'metadata' in entry['option']:
                overriding_metadata_file_path_or_url=\
                    os.path.join(test_dir,entry['option']['metadata'])
            else:
                overriding_metadata_file_path_or_url=None
                
            # Link Header
            _link_header=entry.get('httpLink')
            
            # well known text
            _well_known_text='{+url}-metadata.json\ncsv-metadata.json\n{+url}.json\ncsvm.json'
            
            # validate option
            validate=False
            
            #raise Exception
            
            #with warnings.catch_warnings() as w:
                
            if entry['type']=='csvt:ToJsonTest':
                
                annotated_table_group_dict=\
                    csvw_functions.create_annotated_table_group(
                            action_fp,
                            overriding_metadata_file_path_or_url,
                            validate=validate,
                            _link_header=_link_header,
                            _well_known_text=_well_known_text,
                            _save_intermediate_and_final_outputs_to_file=True,
                            _print_intermediate_outputs=p
                            )
                
            
            elif entry['type']=='csvt:ToJsonTestWithWarnings':    
            
                with self.assertWarns(UserWarning):
            
                    annotated_table_group_dict=\
                        csvw_functions.create_annotated_table_group(
                                action_fp,
                                overriding_metadata_file_path_or_url,
                                validate=validate,
                                _link_header=_link_header,
                                _well_known_text=_well_known_text,
                                _save_intermediate_and_final_outputs_to_file=True,
                                _print_intermediate_outputs=p
                                )
                        
            elif entry['type']=='csvt:NegativeJsonTest':    
            
                with self.assertRaises(csvw_functions.CSVWError):
            
                    annotated_table_group_dict=\
                        csvw_functions.create_annotated_table_group(
                                action_fp,
                                overriding_metadata_file_path_or_url,
                                validate=validate,
                                _link_header=_link_header,
                                _save_intermediate_and_final_outputs_to_file=True,
                                _print_intermediate_outputs=p
                                )
                        
                        
                        
                        
            else:
                
                raise Exception(entry['type'])
                    
                # if entry['type']=='csvt:ToJsonTestWithWarnings':
                    
                #     print(w)
                    
                #     self.assertTrue(w is not None)
                #     self.assertTrue(len(w)>0)
                
            
                
            # print('---')
            # print(annotated_table_group_dict['tables'][0]['columns'][1]['name'])
            # print(annotated_table_group_dict['tables'][0]['columns'][1]['propertyURL'])
            # try:
            #     if p: print(annotated_table_group_dict['tables'][0]['columns'][3]['cells'][0]['value'])
            # except Exception:
            #     pass
            
            #return
                
            # mode option
            if entry['option'].get('minimal') is True:
                mode='minimal'
            else:
                mode='standard'
                
            x=os.path.join(os.getcwd(),'_github_w3c_csvw_tests').replace('\\','/')
            x=x.replace(' ','%20')
            _replace_strings=[
                (r'file:///'+x+'/',
                 'http://www.w3.org/2013/csvw/tests/')
                ]
                
            #
            json_ld=\
                csvw_functions.create_json_ld(
                        annotated_table_group_dict,
                        mode=mode,
                        _replace_strings=_replace_strings
                        )    
                        
            #print('-json_ld',json_ld)
                  
            with open(result_fp,encoding='utf-8') as f:
                json_ld_result=json.load(f)
            #print('-json_ld_result',json_ld_result)
            
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
            
            if p: print('---------------------------')
            #break
            
            #import time
            #time.sleep(.1)
            
        warnings.filterwarnings("always",category=UserWarning)  # warnings always printed out
        
            
#%% ---Useful comparison functions---          
        
def compare_json(self,json1,json2):
    """Campares two JSON objects.
    
    Tests that all parts of the JSON objects are equal.
    
    Useful to see which part of two JSON objects are not the same.
    
    """
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
        
        # self.assertEqual(
        #     len(json1),
        #     len(json2),
        #     msg=f'lists are not of the same length {json1} {json2}'
        #     )
        
        for i in range(len(json1)):
            
            compare_json(
                self,
                json1[i],
                json2[i]
                )
        
    else:
        
        self.assertEqual(json1,json2)
        
        self.assertEqual(type(json1),type(json2))




def compare_rdf(
        self,
        g1,  # rdflib graph
        g2, # rdflib graph
        ):
    """
    """
    iso1 = rdflib.compare.to_isomorphic(g1)
    iso2 = rdflib.compare.to_isomorphic(g2)
    
    if not iso1==iso2:
        
        def dump_nt_sorted(g):
            for l in sorted(g.serialize(format='nt').splitlines()):
                if l: print(l)
                
        in_both, in_first, in_second = rdflib.compare.graph_diff(iso1, iso2)
        print('--- iso1 == iso2 ---')
        print(iso1 == iso2)
        print('---in both---')
        dump_nt_sorted(in_both)
        print('---in first---')
        dump_nt_sorted(in_first)
        print('---in second---')
        dump_nt_sorted(in_second)
        
    self.assertEqual(
        iso1,
        iso2
        )
    
    
    
    




#%% ---unittest main call---

if __name__=='__main__':
    
    warnings.filterwarnings("always",category=UserWarning)  # warnings always printed out
    
    
    def run_single_test(test_kls,test_name):
        ""
        suite = unittest.TestSuite()
        suite.addTest(test_kls(test_name))
        runner = unittest.TextTestRunner()
        runner.run(suite)
        
    # TESTCASE - Generating JSON from Tabular Data on the Web
    unittest.main(Test_JSON_Section_6())
    #run_single_test(Test_JSON_Section_6,'test_section_6_1_simple_example')
    #run_single_test(Test_JSON_Section_6,'test_section_6_2_Example_with_single_table_and_rich_annotations')
    #run_single_test(Test_JSON_Section_6,'test_section_6_3_Example_with_single_table_and_using_virtual_columns_to_produce_multiple_subjects_per_row')
    #run_single_test(Test_JSON_Section_6,'test_section_6_4_Example_with_table_group_comprising_four_interrelated_tables')
    
    # TESTCASE - Generating RDF from Tabular Data on the Web
    unittest.main(Test_RDF_Section_7())
    #run_single_test(Test_RDF_Section_7,'test_section_7_1_simple_example')
    #run_single_test(Test_RDF_Section_7,'test_section_7_2_Example_with_single_table_and_rich_annotations')
    #run_single_test(Test_RDF_Section_7,'test_section_7_3_Example_with_single_table_and_using_virtual_columns_to_produce_multiple_subjects_per_row')
    #run_single_test(Test_RDF_Section_7,'test_section_7_4_Example_with_table_group_comprising_four_interrelated_tables')
    
    # TESTCASE - W3C CSVW Test Suite
    unittest.main(Test_W3C_CSVW_Test_Cases())
    #run_single_test(Test_W3C_CSVW_Test_Cases,'test_W3C_CSVW_JSON_test_cases')
    #run_single_test(Test_W3C_CSVW_Test_Cases,'test_W3C_CSVW_RDF_test_cases')
    
    
    #unittest.main()
    
    
    
