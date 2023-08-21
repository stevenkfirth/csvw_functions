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


#%% download csv files

def _get_download_info(
        metadata_table_dict,
        data_folder,
        verbose=False
        ):
    ""
    # get info for downloading
    csv_file_name=metadata_table_dict['https://purl.org/berg/csvw_functions/vocab/csv_file_name']['@value']
    if verbose:
        print('csv_file_name:',csv_file_name)
    csv_download_url=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/csv_download_url',{'@value':None})['@value']
    if verbose:
        print('csv_download_url:', csv_download_url)
    zip_download_url=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/zip_download_url',{'@value':None})['@value']
    if verbose:
        print('zip_download_url',zip_download_url)
    zip_filename=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/zip_filename',{'@value':None})['@value']
    if verbose:
        print('zip_filename',zip_filename)
    csv_zip_extract_path=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/csv_zip_extract_path',{'@value':None})['@value']
    if verbose:
        print('csv_zip_extract_path',csv_zip_extract_path)
    metadata_url=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/metadata_download_url',{'@value':None})['@value']
    if verbose:
        print('metadata_url:',metadata_url)
    metadata_file_suffix=metadata_table_dict.get('https://purl.org/berg/csvw_functions/vocab/metadata_file_suffix',{'@value':'-metadata.txt'})['@value']
    if verbose:
        print('metadata_file_suffix:',metadata_file_suffix)
    
    fp_csv=os.path.join(data_folder,csv_file_name)
    if verbose:
        print('fp_csv',fp_csv)
    if zip_filename is None:
        fp_zip = None
    else:
        fp_zip=os.path.join(data_folder, zip_filename)
    if verbose:
        print('fp_zip:', fp_zip)
    
    result = dict(
        csv_file_name=csv_file_name, 
        csv_download_url=csv_download_url, 
        zip_download_url=zip_download_url, 
        zip_filename=zip_filename,
        csv_zip_extract_path=csv_zip_extract_path,
        metadata_url=metadata_url,
        metadata_file_suffix=metadata_file_suffix,
        fp_csv=fp_csv,
        fp_zip=fp_zip
        )
    
    return result


def _download_table_and_metadata(
        csv_download_url=None,
        fp_csv=None,
        zip_download_url=None,
        fp_zip=None,
        csv_zip_extract_path=None,
        metadata_url=None,
        metadata_file_suffix=None,
        overwrite_existing_files=False,
        verbose=False,
        **kwargs  # to pick up unused keywords in **download_info
        ):
    """
    """
    
    if not csv_download_url is None:
        
        # download csv
        if overwrite_existing_files or not os.path.exists(fp_csv):
            
            urllib.request.urlretrieve(
                url=csv_download_url, 
                filename=fp_csv
                )
            
    else:  # zip file
        
        # download zip
        if overwrite_existing_files or not os.path.exists(fp_zip):
            
            if verbose:
                print('downloading zip file...')
            urllib.request.urlretrieve(
                url=zip_download_url, 
                filename=fp_zip
                )
        
        # extract csv
        if not os.path.exists(fp_csv):
            
            if verbose:
                print('extracting csv file...')
            with zipfile.ZipFile(fp_zip) as z:
    
                with open(fp_csv, 'wb') as f:
                    
                    f.write(z.read(csv_zip_extract_path))
              
            
    # download metadata
    if not metadata_url is None:
        
        if not csv_download_url is None:
        
            fp_metadata=f"{fp_csv}-{metadata_file_suffix}"
            
        else:
            
            fp_metadata=f"{fp_zip}-{metadata_file_suffix}"
        
        if overwrite_existing_files or not os.path.exists(fp_metadata):
            
            urllib.request.urlretrieve(
                url=metadata_url, 
                filename=fp_metadata
                )
            
        
def download_table_group(
        metadata_document_location,
        data_folder='_data',
        overwrite_existing_files=False,
        verbose=False
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
    if verbose:
        print(metadata_table_group_dict)
    
    for i, metadata_table_dict in enumerate(metadata_table_group_dict['tables']):
        
        download_info=_get_download_info(
            metadata_table_dict,
            data_folder,
            verbose,
            )
        
        # download table
        _download_table_and_metadata(
            overwrite_existing_files=overwrite_existing_files,
            verbose=verbose,
            **download_info
            )
    
        # update metadata_table_dict
        metadata_table_dict['url']=download_info['csv_file_name']
        
        
    # save updated metadata_table_group_dict
    fp_metadata=os.path.join(data_folder,os.path.basename(metadata_document_location))
    with open(fp_metadata, 'w') as f:
        json.dump(metadata_table_group_dict,f,indent=4)
        
    return fp_metadata
    

    
# def download_table(
#         metadata_document_location,
#         data_folder='_data',
#         verbose=True
#         ):
#     """
#     """
    
#     # create data_folder if it doesn't exist
#     if not os.path.exists(data_folder):
        
#         os.makedirs(data_folder)
    
#     # get normalised metadata_table_dict
#     metadata_table_dict = \
#         csvw_functions.validate_table_metadata(
#             metadata_document_location
#             )
#     #print(metadata_table_dict)
    
#     # download table
#     fp_csv,metadata_table_dict=\
#         _download_table(
#             metadata_table_dict,
#             data_folder,
#             verbose=verbose
#             )
    
#     # save updated metadata_table_dict
#     fp_metadata=f'{fp_csv}-metadata.json'
#     with open(fp_metadata, 'w') as f:
#         json.dump(metadata_table_dict,f,indent=4)




#%% import data to sqlite

def _check_if_table_exists_in_database(
        fp_database,
        table_name
        ):
    ""
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        query=f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        return True if c.execute(query).fetchall()[0][0] else False


def _create_table_from_csvw(
        metadata_table_dict,
        fp_database,
        table_name,
        verbose=False
        ):
    ""
    # create query
    datatype_map={
    'integer':'INTEGER',
    'decimal':'REAL'
    }
    query=f'CREATE TABLE "{table_name}" ('
    for column_dict in metadata_table_dict['tableSchema']['columns']:
        #print(column_dict)
        name=column_dict['name']
        datatype=datatype_map.get(column_dict['datatype']['base'],'TEXT')
        query+=f'"{name}" {datatype}'
        query+=", "
    query=query[:-2]
    
    if 'primaryKey' in metadata_table_dict['tableSchema']:
        
        pk=metadata_table_dict['tableSchema']['primaryKey']
        if isinstance(pk,str):
            pk=[pk]
        query+=', PRIMARY KEY ('
        for x in pk:
            query+=f'"{x}"'
            query+=", "
        query=query[:-2]
        query+=') '
    
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
                

def _drop_table(
        fp_database,
        table_name,
        verbose=False
        ):
    ""
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        query=f'DROP TABLE IF EXISTS "{table_name}";'
        if verbose:
            print(query)
        c.execute(query)
        conn.commit()


def _get_import_info(
        metadata_table_dict,
        metadata_document_location,
        verbose=False,
        ):
    ""
    # get info for importing
    table_name=metadata_table_dict['https://purl.org/berg/csvw_functions/vocab/sql_table_name']['@value']
    if verbose:
        print('table_name:',table_name)
    url=metadata_table_dict['url']
    if verbose:
        print('url',url)
    fp_csv=os.path.join(os.path.dirname(metadata_document_location),url)
    if verbose:
        print('fp_csv:', fp_csv)
    remove_existing_table=metadata_table_dict.get(
        "https://purl.org/berg/csvw_functions/vocab/sql_remove_existing_table",
        False
        )
    
    return table_name, fp_csv, remove_existing_table
    
    
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
    
        
def _import_csv_file(
        fp_csv,
        fp_database,
        table_name,
        verbose=False
        ):
    """
    """                
    fp_database2=fp_database.replace('\\','\\\\')
    fp_csv2=fp_csv.replace('\\','\\\\')
    command=f'sqlite3 {fp_database2} -cmd ".mode csv" ".import --skip 1 {fp_csv2} {table_name}"'
    if verbose:
        print('---COMMAND LINE TO IMPORT DATA---')
        print(command)
    subprocess.run(command)
    if verbose:
        print('Number of rows after import: ', _get_row_count_in_database_table(fp_database,table_name))


def import_table_group_to_sqlite(
        metadata_document_location,
        data_folder='_data',
        database_name='data.sqlite',
        remove_existing_tables=False,
        verbose=False
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
    #print(metadata_table_group_dict)
    
    # remove existing tables if requested
    for metadata_table_dict in metadata_table_group_dict['tables']:
    
        # get import info
        table_name, fp_csv, remove_existing_table = _get_import_info(
                metadata_table_dict,
                metadata_document_location,
                verbose=verbose,
                ) 
            
        if _check_if_table_exists_in_database(
            fp_database, 
            table_name
            ):
            
            if remove_existing_table or remove_existing_tables:

                _drop_table(
                    fp_database,
                    table_name
                    )
        
    # create and import tables
    for metadata_table_dict in metadata_table_group_dict['tables']:
        
        # get import info
        table_name, fp_csv, remove_existing_table = _get_import_info(
                metadata_table_dict,
                metadata_document_location,
                verbose=verbose,
                ) 
        
        # create empty table if needed
        if not _check_if_table_exists_in_database(
                fp_database, 
                table_name
                ):
            
            _create_table_from_csvw(
                metadata_table_dict, 
                fp_database, 
                table_name)
            
        # import table data to database
        _import_csv_file(
                fp_csv,
                fp_database,
                table_name,
                verbose=verbose
                )
        
            

# def import_table_to_sqlite(
#         metadata_document_location,
#         data_folder='_data',
#         database_name='data.sqlite',
#         verbose=True,
#         _reload_database_table=False
#         ):
#     """
#     """
#     # create data_folder if it doesn't exist
#     if not os.path.exists(data_folder):
#         os.makedirs(data_folder)
        
#     # set database fp
#     fp_database=os.path.join(data_folder,database_name)
    
#     # get normalised metadata_table_group_dict
#     metadata_table_dict = \
#         csvw_functions.validate_table_metadata(
#             metadata_document_location
#             )
#     print(metadata_table_dict)
    
#     table_name=metadata_table_dict['https://purl.org/berg/csvw_functions/vocab/table_name']['@value']
        
    
#     if _reload_database_table \
#         or not _check_if_table_exists_in_database(
#                 fp_database, 
#                 table_name
#                 ):
    
#         # import table data to database
#         _import_table_to_sqlite(
#                 metadata_table_dict,
#                 metadata_document_location,
#                 fp_database,
#                 verbose=verbose
#                 )



