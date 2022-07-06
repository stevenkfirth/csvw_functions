# -*- coding: utf-8 -*-



import unittest
import json
import os

csvw_test_dir='_github_w3c_csvw_tests'

with open(os.path.join(csvw_test_dir,'manifest-validation.jsonld')) as f:
    manifest=json.load(f)
    
print(manifest)




class TestValidation(unittest.TestCase):
    ""
    
    def test_run_validation_tests(self):
        ""
        
        
        
        
        
if __name__=='__main__':
    
    unittest.main()





