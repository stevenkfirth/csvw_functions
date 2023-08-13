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
import sqlite3
import subprocess
import zipfile


def import_table_group_to_sqlite(
        metadata_document_location,
        data_folder='_data',
        database_name='data.sqlite',
        verbose=True,
        _reload_all_database_tables=False
        ):
    """
    """
    # create data_folder if it doesn't exist
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        
    # set database fp
    fp_database=os.path.join(data_folder,database_name)
    
    # get normalised metadata_table_group_dict
    metadata_table_group_dict = \
        csvw_functions.validate_table_group_metadata(
            metadata_document_location
            )
    print(metadata_table_group_dict)
    
    for i, metadata_table_dict in enumerate(metadata_table_group_dict['tables']):
        
        table_name=metadata_table_dict['https://purl.org/berg/csvw_functions/vocab/table_name']['@value']
        
        if _reload_all_database_tables or \
            not _check_if_table_exists_in_database(
                    fp_database, 
                    table_name
                    ):
        
            # import table data to database
            _import_table_to_sqlite(
                    metadata_table_dict,
                    metadata_document_location,
                    fp_database,
                    verbose=verbose
                    )
            

def import_table_to_sqlite(
        metadata_document_location,
        data_folder='_data',
        database_name='data.sqlite',
        verbose=True,
        _reload_database_table=False
        ):
    """
    """
    # create data_folder if it doesn't exist
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        
    # set database fp
    fp_database=os.path.join(data_folder,database_name)
    
    # get normalised metadata_table_group_dict
    metadata_table_dict = \
        csvw_functions.validate_table_metadata(
            metadata_document_location
            )
    print(metadata_table_dict)
    
    table_name=metadata_table_dict['https://purl.org/berg/csvw_functions/vocab/table_name']['@value']
        
    if _reload_database_table or \
        not _check_if_table_exists_in_database(
                fp_database, 
                table_name
                ):
    
        # import table data to database
        _import_table_to_sqlite(
                metadata_table_dict,
                metadata_document_location,
                fp_database,
                verbose=verbose
                )
    
    
def _import_table_to_sqlite(
        metadata_table_dict,
        metadata_document_location,
        fp_database,
        verbose=True
        ):
    """
    """
    """Creates a table in the sqlite database.
    
    Replaces any existing table.
    
    """
    
    # get info for importing
    table_name=metadata_table_dict['https://purl.org/berg/csvw_functions/vocab/table_name']['@value']
    print('table_name:',table_name)
    fp_csv=metadata_table_dict['url']
    print('fp_csv:', fp_csv)
    
    
    # drop table in database
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        query=f'DROP TABLE IF EXISTS "{table_name}";'
        print(query)
        c.execute(query)
        conn.commit()
    
    # create query
    datatype_map={
    'integer':'INTEGER',
    'decimal':'REAL'
    }
    query=f'CREATE TABLE "{table_name}" ('
    for column_dict in metadata_table_dict['tableSchema']['columns']:
        name=column_dict['name']
        datatype=datatype_map.get(column_dict['datatype']['base'],'TEXT')
        query+=f"{name} {datatype}"
        query+=", "
    query=query[:-2]
    query+=');'
    
    if verbose:
        print('---QUERY TO CREATE TABLE---')
        print(query)
    
    # create empty table in database
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        c.execute(query)
        conn.commit()
        
    # create indexes
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        for column_dict in metadata_table_dict['tableSchema']['columns']:
            column_name=column_dict['name']
            setindex=column_dict.get('https://purl.org/berg/csvw_functions/vocab/sqlsetindex',False)
            if setindex:
                index_name=f'{table_name}_{column_name}'
                query=f'CREATE INDEX "{index_name}" ON "{table_name}"("{column_name}")'
                if verbose:
                    print(query)
                c.execute(query)
                conn.commit()
                
                
    # import data into table
    fp_database2=fp_database.replace('\\','\\\\')
    fp_csv2=fp_csv.replace('\\','\\\\')
    command=f'sqlite3 {fp_database2} -cmd ".mode csv" ".import --skip 1 {fp_csv2} {table_name}"'
    if verbose:
        print('---COMMAND LINE TO IMPORT DATA---')
        print(command)
    subprocess.run(command)
    if verbose:
        print('Number of rows after import: ', _get_row_count_in_database_table(fp_database,table_name))
    
    
def _check_if_table_exists_in_database(
        fp_database,
        table_name
        ):
    ""
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        query=f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        return True if c.execute(query).fetchall()[0][0] else False
    
    
    
def _get_row_count_in_database_table(
        fp_database,
        table_name,
        column_name='*'
        ):
    """Gets number of rows in table
    
    """
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        query=f'SELECT COUNT({column_name}) FROM "{table_name}"'
        return c.execute(query).fetchone()[0]
    
    
    



def download_table_group(
        metadata_document_location,
        data_folder='_data'
        ):
    """
    """

    # create data_folder if it doesn't exist
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        
    # get normalised metadata_table_group_dict
    metadata_table_group_dict = \
        csvw_functions.validate_table_group_metadata(
            metadata_document_location
            )
    print(metadata_table_group_dict)
    
    for i, metadata_table_dict in enumerate(metadata_table_group_dict['tables']):
        
        # download table
        table_name,fp_csv,metadata_table_dict=\
            _download_table(
                metadata_table_dict,
                data_folder,
                )
    
        # update metadata_table_dict in metadata_table_group_dict
        metadata_table_group_dict['tables'][i]=metadata_table_dict
        
    # save updated metadata_table_group_dict
    
    fp_metadata=os.path.join(data_folder,os.path.basename(metadata_document_location))
    with open(fp_metadata, 'w') as f:
        json.dump(metadata_table_group_dict,f,indent=4)
        
        

def download_table(
        metadata_document_location,
        data_folder='_data'
        ):
    """
    """
    
    # create data_folder if it doesn't exist
    if not os.path.exists(data_folder):
        
        os.makedirs(data_folder)
    
    # get normalised metadata_table_dict
    metadata_table_dict = \
        csvw_functions.validate_table_metadata(
            metadata_document_location
            )
    print(metadata_table_dict)
    
    # download table
    table_name,fp_csv,metadata_table_dict=\
        _download_table(
            metadata_table_dict,
            data_folder,
            )
    
    # save updated metadata_table_dict
    fp_metadata=f'{fp_csv}-metadata.json'
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
    table_name=metadata_table_dict['https://purl.org/berg/csvw_functions/vocab/table_name']['@value']
    print('table_name:',table_name)
    csv_download_url=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/csv_download_url',{'@value':None})['@value']
    print('csv_download_url:', csv_download_url)
    zip_download_url=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/zip_download_url',{'@value':None})['@value']
    print('zip_download_url',zip_download_url)
    zip_filename=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/zip_filename',{'@value':None})['@value']
    print('zip_filename',zip_filename)
    csv_zip_extract_path=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/csv_zip_extract_path',{'@value':None})['@value']
    print('csv_zip_extract_path',csv_zip_extract_path)
    metadata_url=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/metadata_download_url',{'@value':None})['@value']
    print('metadata_url:',metadata_url)
    metadata_file_suffix=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/metadata_file_suffix',{'@value':'-metadata.txt'})['@value']
    print('metadata_file_suffix:',metadata_file_suffix)
    
    fp_csv=os.path.join(data_folder, f'{table_name}.csv')
    print('fp_csv:', fp_csv)
    if zip_filename is None:
        fp_zip = None
    else:
        fp_zip=os.path.join(data_folder, zip_filename)
    print('fp_zip:', fp_zip)
    
    if not csv_download_url is None:
        
        # download csv
        if not os.path.exists(fp_csv):
            
            urllib.request.urlretrieve(
                url=csv_download_url, 
                filename=fp_csv
                )
            
    else:  # zip file
        
        # download zip
        if not os.path.exists(fp_zip):
            
            print('downloading zip file...')
            urllib.request.urlretrieve(
                url=zip_download_url, 
                filename=fp_zip
                )
        
        # extract csv
        if not os.path.exists(fp_csv):
            
            print('extracting csv file...')
            with zipfile.ZipFile(fp_zip) as z:
    
                with open(fp_csv, 'wb') as f:
                    
                    f.write(z.read(csv_zip_extract_path))
              
            
    # download metadata
    if not metadata_url is None:
        
        if not csv_download_url is None:
        
            fp_metadata=f'{fp_csv}-{metadata_file_suffix}'
            
        else:
            
            fp_metadata=f'{fp_zip}-{metadata_file_suffix}'
        
        if not os.path.exists(fp_metadata):
            
            urllib.request.urlretrieve(
                url=metadata_url, 
                filename=fp_metadata
                )
            
    # update metadata_table_dict
    metadata_table_dict['url']=f'{table_name}.csv'
        
    return table_name,fp_csv,metadata_table_dict
            
            
        
# def _get_table_download_log(
#         table_name,
#         download_log_json
#         ):
#     ""
#     for x in download_log_json:
        
#         if x['table_name']==table_name:
            
#             return x
        
#     raise ValueError('table_name not in download log')
        
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    