# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 06:11:59 2022

@author: cvskf
"""

import csvw_functions

import unittest
import os
import json

from rdflib import Graph, Literal, URIRef, XSD



class TestSection1(unittest.TestCase):
    ""
    
    def test_Section1_3(self):
        ""
        
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
            
    
        
class TestSection4(unittest.TestCase):
    ""
    
    def test_Section4_2(self):
        ""
        
        annotated_table_group_dict=csvw_functions.create_annotated_table_group(
            input_file_path_or_url='example_46.json'
            )
        
        json_ld=csvw_functions.create_json_ld(
            annotated_table_group_dict,
            mode='minimal'
            )
        
        print(json_ld)
        
        with open('json_ld.json','w') as f:
            json.dump(json_ld,
                      f,
                      indent=4
                      )
        
        
        rdf_ntriples=csvw_functions.create_rdf(
            annotated_table_group_dict,
            mode='minimal',
            local_path_replacement_url='http://example.org'
            )
        
        #print(rdf_ntriples)
        
        with open('rdf.ntriples','w') as f:
            f.write(rdf_ntriples)
        
        g = Graph().parse(data=rdf_ntriples, format='ntriples')
        
        rdf_ttl=g.serialize(format="ttl")
        
        print(rdf_ttl)
        
        with open('rdf.ttl','w') as f:
            f.write(rdf_ttl)
        
        
if __name__=='__main__':
    
    initial_current_working_directory=os.getcwd()

    os.chdir('csvw_primer_example_files')
    
    unittest.main()
    
    os.chdir(initial_current_working_directory)
    
        