# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 14:47:35 2023

@author: cvskf
"""

# This module contains extra functions based on the CSVW format.
# These are not part of the CSVW standards, rather additional functionality.

from . import csvw_functions
import os
import json
import urllib.request
import urllib.parse


def download_table(
        metadata_document_location,
        data_folder='_data'
        ):
    """
    """
    
    # create data_folder if it doesn't exist
    if not os.path.exists(data_folder):
        
        os.makedirs(data_folder)
        
    # # create download log if it doesn't exist
    # fp_download_log=os.path.join(data_folder,'__download_log__.json')
    
    # if not os.path.exists(fp_download_log):
        
    #     with open(fp_download_log, 'w') as f:
    #         json.dump([],f)
            
    # # get download log
    # with open(fp_download_log) as f:
    #     download_log_json=json.load(f)
    
    # get normalised metadata_table_dict
    metadata_table_dict = \
        csvw_functions.validate_table_metadata(
            metadata_document_location
            )
    print(metadata_table_dict)
    
    # download table
    table_name,fp_csv=\
        _download_table(
            metadata_table_dict,
            data_folder,
            # download_log_json,
            # fp_download_log
            )
        
    # update and save metadata
    fp_metadata=f'{fp_csv}-metadata.json'
    metadata_table_dict['url']=f'{table_name}.csv'
    with open(fp_metadata, 'w') as f:
        json.dump(metadata_table_dict,f,indent=4)
    
    
def _download_table(
        metadata_table_dict,
        data_folder,
        # download_log_json,
        # fp_download_log
        ):
    """
    """
    
    # get info for downloading
    csv_download_url=metadata_table_dict['https://purl.org/berg/csvw_functions/vocab/csv_download_url']['@value']
    print('csv_download_url:', csv_download_url)
    table_name=metadata_table_dict['https://purl.org/berg/csvw_functions/vocab/table_name']['@value']
    print('table_name:',table_name)
    fp_csv=os.path.join(data_folder, f'{table_name}.csv')
    print('fp_csv:', fp_csv)
    metadata_url=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/metadata_download_url',{'@value':None})['@value']
    print('metadata_url:',metadata_url)
    metadata_file_suffix=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/metadata_file_suffix',{'@value':'-metadata.txt'})['@value']
    print('metadata_file_suffix:',metadata_file_suffix)
    
    if csv_download_url is None or csv_download_url=='':
        
        pass  # zip file?
        
        # unzip data file if needed
        # if ext=='.zip':
            
        #     with zipfile.ZipFile(fp) as z:
                
        #         for y in x['extract']:
                
        #             fp2=os.path.join(data_folder,y['data_filename'])
                    
        #             if not os.path.exists(fp2):
                
        #                 with open(fp2, 'wb') as f:
                            
        #                     f.write(z.read(y['data_filepath']))
                    
        
    else:
        
        # try:
            
        #     table_download_log=\
        #         _get_table_download_log(
        #                 table_name,
        #                 download_log_json
        #                 )
        
        # except ValueError:
            
        #     table_download_log=None
            
        # if table_download_log is None:
            
        if not os.path.exists(fp_csv):
            
            # download csv
            urllib.request.urlretrieve(
                url=csv_download_url, 
                filename=fp_csv
                )
            
        # download metadata
        if not metadata_url is None:
            
            fp_metadata=f'{fp_csv}-{metadata_file_suffix}'
            
            if not os.path.exists(fp_metadata):
                
                urllib.request.urlretrieve(
                    url=metadata_url, 
                    filename=fp_metadata
                    )
                
            # # add to log and save
            # download_log_json.append(
            #     dict(table_name=table_name)
            #     )
            # with open(fp_download_log,'w') as f:
            #     json.dump(download_log_json,f,indent=4)
            
            
    return table_name,fp_csv
            
            
        
# def _get_table_download_log(
#         table_name,
#         download_log_json
#         ):
#     ""
#     for x in download_log_json:
        
#         if x['table_name']==table_name:
            
#             return x
        
#     raise ValueError('table_name not in download log')
        
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    