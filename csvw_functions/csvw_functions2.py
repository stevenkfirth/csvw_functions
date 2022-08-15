# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 17:32:29 2022

@author: cvskf
"""

# csvw_functions
#
# A Python implementation of the set of W3C Standards on CSV on the Web (CSVW).
#
# This Python package implements the following standards:
# - Model for Tabular Data and Metadata on the Web
# - Metadata Vocabulary for Tabular Data
# - Generating JSON from Tabular Data on the Web
# - Generating RDF from Tabular Data on the Web
#
# These standards are all available via the CSV on the Web: A Primer
# document here: https://www.w3.org/TR/tabular-data-primer/
#
# In this Python package the algorithms and processes of the CSVW standards
# are implemented as a series of Python functions.
#

#%% ---Package imports---

import requests
import json
import os
import urllib
import warnings
import hyperlink
import uritemplate
import langcodes
import datetime


#%% ---Module Level Variables---
#
# This section sets up a number of variables at the module level
# which are used by the functions in the package.

# Schema Prefixes

prefixes=\
    {
    "as": "https://www.w3.org/ns/activitystreams#",
    "cc": "http://creativecommons.org/ns#",
    "csvw": "http://www.w3.org/ns/csvw#",
    "ctag": "http://commontag.org/ns#",
    "dc": "http://purl.org/dc/terms/",
    "dc11": "http://purl.org/dc/elements/1.1/",
    "dcat": "http://www.w3.org/ns/dcat#",
    "dcterms": "http://purl.org/dc/terms/",
    "dqv": "http://www.w3.org/ns/dqv#",
    "duv": "https://www.w3.org/ns/duv#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "gr": "http://purl.org/goodrelations/v1#",
    "grddl": "http://www.w3.org/2003/g/data-view#",
    "ical": "http://www.w3.org/2002/12/cal/icaltzd#",
    "jsonld": "http://www.w3.org/ns/json-ld#",
    "ldp": "http://www.w3.org/ns/ldp#",
    "ma": "http://www.w3.org/ns/ma-ont#",
    "oa": "http://www.w3.org/ns/oa#",
    "odrl": "http://www.w3.org/ns/odrl/2/",
    "og": "http://ogp.me/ns#",
    "org": "http://www.w3.org/ns/org#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "prov": "http://www.w3.org/ns/prov#",
    "qb": "http://purl.org/linked-data/cube#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfa": "http://www.w3.org/ns/rdfa#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "rev": "http://purl.org/stuff/rev#",
    "rif": "http://www.w3.org/2007/rif#",
    "rr": "http://www.w3.org/ns/r2rml#",
    "schema": "http://schema.org/",
    "sd": "http://www.w3.org/ns/sparql-service-description#",
    "sioc": "http://rdfs.org/sioc/ns#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "skosxl": "http://www.w3.org/2008/05/skos-xl#",
    "sosa": "http://www.w3.org/ns/sosa/",
    "ssn": "http://www.w3.org/ns/ssn/",
    "time": "http://www.w3.org/2006/time#",
    "v": "http://rdf.data-vocabulary.org/#",
    "vcard": "http://www.w3.org/2006/vcard/ns#",
    "void": "http://rdfs.org/ns/void#",
    "wdr": "http://www.w3.org/2007/05/powder#",
    "wdrs": "http://www.w3.org/2007/05/powder-s#",
    "xhv": "http://www.w3.org/1999/xhtml/vocab#",
    "xml": "http://www.w3.org/XML/1998/namespace",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "describedby": "http://www.w3.org/2007/05/powder-s#describedby",
    "license": "http://www.w3.org/1999/xhtml/vocab#license",
    "role": "http://www.w3.org/1999/xhtml/vocab#role"
  }


# Datatypes - Metadata Section 5.11.1
datatypes={
    'anyAtomicType':'http://www.w3.org/2001/XMLSchema#anyAtomicType',
    'anyURI':'http://www.w3.org/2001/XMLSchema#anyURI',
    'base64Binary':'http://www.w3.org/2001/XMLSchema#base64Binary',
    'boolean':'http://www.w3.org/2001/XMLSchema#boolean',
    'date':'http://www.w3.org/2001/XMLSchema#date',
    'dateTime':'http://www.w3.org/2001/XMLSchema#dateTime',
    'dateTimeStamp':'http://www.w3.org/2001/XMLSchema#dateTimeStamp',
    'decimal':'http://www.w3.org/2001/XMLSchema#decimal',
    'integer':'http://www.w3.org/2001/XMLSchema#integer',
    'long':'http://www.w3.org/2001/XMLSchema#long',
    'int':'http://www.w3.org/2001/XMLSchema#int',
    'short':'http://www.w3.org/2001/XMLSchema#short',
    'byte':'http://www.w3.org/2001/XMLSchema#byte',
    'nonNegativeInteger':'http://www.w3.org/2001/XMLSchema#nonNegativeInteger',
    'postiveInteger':'http://www.w3.org/2001/XMLSchema#positiveInteger',
    'unsignedLong':'http://www.w3.org/2001/XMLSchema#unsignedLong',
    'unsignedInt':'http://www.w3.org/2001/XMLSchema#unsignedInt',
    'unsignedShort':'http://www.w3.org/2001/XMLSchema#UnsignedShort',
    'unsignedByte':'http://www.w3.org/2001/XMLSchema#unsignedByte',
    'nonPositiveInteger':'http://www.w3.org/2001/XMLSchema#nonPositiveInteger',
    'negativeInteger':'http://www.w3.org/2001/XMLSchema#negativeInteger',
    'double':'http://www.w3.org/2001/XMLSchema#double',
    'duration':'http://www.w3.org/2001/XMLSchema#duration',
    'dayTimeDuration':'http://www.w3.org/2001/XMLSchema#dayTimeDuration',
    'yearMonthDuration':'http://www.w3.org/2001/XMLSchema#yearMonthDuration',
    'float':'http://www.w3.org/2001/XMLSchema#float',
    'gDay':'http://www.w3.org/2001/XMLSchema#gDay',
    'gMonth':'http://www.w3.org/2001/XMLSchema#gMonth',
    'gYear':'http://www.w3.org/2001/XMLSchema#gYear',
    'gYearMonth':'http://www.w3.org/2001/XMLSchema#gYearMonth',
    'hexBinary':'http://www.w3.org/2001/XMLSchema#hexBinary',
    'QName':'http://www.w3.org/2001/XMLSchema#QName',
    'string':'http://www.w3.org/2001/XMLSchema#string',
    'normalizedString':'http://www.w3.org/2001/XMLSchema#normalisedString',
    'token':'http://www.w3.org/2001/XMLSchema#token',
    'language':'http://www.w3.org/2001/XMLSchema#language',
    'Name':'http://www.w3.org/2001/XMLSchema#Name',
    'NMTOKEN':'http://www.w3.org/2001/XMLSchema#NMTOKEN',
    'xml':'http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral',  #  indicates the value is an XML fragment
    'html':'http://www.w3.org/1999/02/22-rdf-syntax-ns#HTML',  #  indicates the value is an HTML fragment
    'json':'http://www.w3.org/ns/csvw#JSON',  # indicates the value is serialized JSON
    'time':'http://www.w3.org/2001/XMLSchema#time'
    }

datatypes['number']=datatypes['double']
datatypes['binary']=datatypes['base64Binary']
datatypes['datetime']=datatypes['dateTime']
datatypes['any']=datatypes['anyAtomicType']


# Datatype collections

datatypes_tokens=[
    'token',
    'language',
    'Name',
    'NMTOKEN'
    ]

datatypes_normalizedStrings=['normalizedString']+datatypes_tokens

datatypes_strings=['string']+datatypes_normalizedStrings+['xml','html','json'] 

datatypes_longs=['long','int','short','byte']
datatypes_nonNegativeIntegers=['nonNegativeInteger',
                               'positiveInteger',
                               'unsignedLong',
                               'unsingedInt',
                               'unsingedShort',
                               'unsingedByte'
                               ]
datatypes_nonPositiveIntegers=['nonPositiveInteger',
                              'negativeInteger']
datatypes_integers=(['integer']
                    +datatypes_longs
                    +datatypes_nonNegativeIntegers
                    +datatypes_nonPositiveIntegers)
datatypes_decimals=(['decimal']
                    +datatypes_integers)
datatypes_numbers=['double','number']+datatypes_decimals

datatypes_dates_and_times=['date','dateTime','datetime','dateTimeStamp','time']



#%% ---Model for Tabular Data and Metadata---

#%% Section 5 - Locating Metadata

def locate_metadata(
        tabular_data_file_url,
        tabular_data_file_text,
        tabular_data_file_headers,
        overriding_metadata_file_path_or_url
        ):
    """
    """
    # As described in section 4. Tabular Data Models, tabular data may have 
    # a number of annotations associated with it. 
    # Here we describe the different methods that can be used to locate 
    # metadata that provides those annotations.

    # In the methods of locating metadata described here, metadata is 
    # provided within a single document. 
    # The syntax of such documents is defined in [tabular-metadata]. 
    # Metadata is located using a specific order of precedence:
    
    # 1. metadata supplied by the user of the implementation that is 
    #    processing the tabular data, see section 5.1 Overriding Metadata.
    # 2. metadata in a document linked to using a Link header associated 
    #    with the tabular data file, see section 5.2 Link Header.
    # 3. metadata located through default paths which may be overridden 
    #    by a site-wide location configuration, see section 5.3 Default 
    #    Locations and Site-wide Location Configuration.
    # 4. metadata embedded within the tabular data file itself, see 
    #    section 5.4 Embedded Metadata.
    
    # Processors must use the first metadata found for processing a 
    # tabular data file by using overriding metadata, if provided. 
    # Otherwise processors must attempt to locate the first metadata 
    # document from the Link header or the metadata located through 
    # site-wide configuration. 
    # If no metadata is supplied or found, processors must use 
    # embedded metadata. 
    # If the metadata does not originate from the embedded metadata, 
    # validators must verify that the table group description within 
    # that metadata is compatible with that in the embedded metadata, 
    # as defined in [tabular-metadata].
    
    # NOTE
    # When feasible, processors should start from a metadata file and 
    # publishers should link to metadata files directly, rather than 
    # depend on mechanisms outlined in this section for locating metadata 
    # from a tabular data file. 
    # Otherwise, if possible, publishers should provide a Link header 
    # on the tabular data file as described in section 5.2 Link Header.
    
    # NOTE
    # If there is no site-wide location configuration, section 5.3 Default 
    # Locations and Site-wide Location Configuration specifies default 
    # URI patterns or paths to be used to locate metadata.    
        
    # 1
    if not overriding_metadata_file_path_or_url is None:
        
        get_overriding_metadata()
    
    # 2
    elif not tabular_data_file_headers is None:
        
        get_metadata_from_link_header()
        
    # 3
    else:
        
        metadata_document_dict, metadata_document_location=\
            get_metadata_from_default_or_site_wide_location(
                tabular_data_file_url,
                tabular_data_file_headers
                )
    # 4
    if metadata_document_dict is None:
    
        metadata_document_dict=None
        metadata_document_location=None
            
    return metadata_document_dict, metadata_document_location

    



#%% 5.1 - Overriding Metadata

def get_overriding_metadata(
        
        ):
    """
    """
    
    raise NotImplementedError


#%% 5.2 - Link Header

def get_metadata_from_link_header(
        
        ):
    """
    """
    
    raise NotImplementedError
    


#%% 5.3 - Default Locations and Site-wide Location Configuration


def get_metadata_from_default_or_site_wide_location(
        tabular_data_file_url,
        tabular_data_file_headers
        ):
    """
    """
    print('tabular_data_file_url',tabular_data_file_url)
    print('tabular_data_file_headers',tabular_data_file_headers)
    
    # If the user has not supplied a metadata file as overriding metadata, 
    # described in section 5.1 Overriding Metadata, and no applicable 
    # metadata file has been discovered through a Link header, described 
    # in section 5.2 Link Header, processors must attempt to locate a 
    # metadata documents through site-wide configuration.
    
    # In this case, processors must retrieve the file from the well-known 
    # URI /.well-known/csvm. (Well-known URIs are defined by [RFC5785].) 
    # If no such file is located (i.e. the response results in a client 
    # error 4xx status code or a server error 5xx status code), processors 
    # must proceed as if this file were found with the following content 
    # which defines default locations:
    #  {+url}-metadata.json
    #  csv-metadata.json
    
    # The response to retrieving /.well-known/csvm may be cached, subject 
    # to cache control directives. 
    # This includes caching an unsuccessful response such as a 404 Not Found.
    
    # This file must contain a URI template, as defined by [URI-TEMPLATE], 
    # on each line. 
    
    if tabular_data_file_headers is None:   # i.e. a local file
    
        absolute_path=os.path.abspath(tabular_data_file_url)
        
        absolute_dir=os.path.dirname(absolute_path)
        
        well_known_path=os.path.join(
            absolute_dir,
            '.well-known',
            'csvm'
            )
        
        try:
            
            with open(well_known_path) as f:
                well_known_text=f.read()
                
        except FileNotFoundError:
            
            well_known_text=None
        
    else:  # tabular data is a remote file
        
        url_defraged=urllib.parse.urldefrag(tabular_data_file_url)[0]
    
        well_known_url=urllib.parse.urljoin(url_defraged,'/.well-known/csvm')
        
        try:
            
            well_known_text=requests.get(well_known_url, stream=True).text 
    
        except requests.ConnectionError:
            
            well_known_text=None
    
    if well_known_text is None:
        
        well_known_text='{+url}-metadata.json\ncsv-metadata.json'
    
    
    # Starting with the first such URI template, processors must:
        
    locations=[x.strip() for x in well_known_text.split('\n')]
        
    for uri_template in locations:
        
        #print('location',location)
        
        # 1. Expand the URI template, with the variable url being set to 
        #    the URL of the requested tabular data file (with any fragment 
        #    component of that URL removed).
        
        if tabular_data_file_headers is None:
            
            variables={'url':absolute_path}
            
        else:
        
            variables={'url':url_defraged}
    
        #print(variables)
        
        expanded_url_quoted=uritemplate.expand(uri_template,
                                               variables)
        #print('expanded_url_quoted',expanded_url_quoted)
        
        expanded_url=urllib.parse.unquote(expanded_url_quoted)   # needed for file paths as they get quoted in the expand process
        #print('expanded_url', expanded_url)
        
        # 2. Resolve the resulting URL against the URL of the requested 
        #    tabular data file.
        
        if tabular_data_file_headers is None:
            
            metadata_url=\
                os.path.join(
                    absolute_dir,
                    expanded_url
                    )
        
        else:
            
            metadata_url=\
                urllib.parse.urljoin(
                    tabular_data_file_url,
                    expanded_url
                    )
        print('metadata_url',metadata_url)
        
        # 3. Attempt to retrieve a metadata document at that URL.
        try:
            
            with open(metadata_url) as f:
                
                metadata_document_dict=json.load(f)
                metadata_document_headers=None
                
        except FileNotFoundError:
            
            try:
            
                response=requests.get(metadata_url)
                text=response.text
                metadata_document_dict=json.loads(text)
                metadata_document_headers=response.headers
            
            except (requests.exceptions.MissingSchema, 
                    requests.ConnectionError,
                    requests.exceptions.InvalidSchema):
                
                metadata_document_dict=None
                metadata_document_headers=None
    
        
        # 4. If no metadata document is found at that location, or if the 
        #    metadata file found at the location does not explicitly 
        #    include a reference to the relevant tabular data file, 
        #    perform these same steps on the next URI template, otherwise 
        #    use that metadata document.
    
        if metadata_document_dict is None:
            
            continue
        
        else:
    
            # top-level properties
            base_url, default_language=\
                validate_top_level_properties(
                    metadata_document_dict,
                    metadata_url
                    )       
            #print('base_url',base_url)
            
            if 'tables' in metadata_document_dict:
                
                table_url=metadata_document_dict['tables'][0]['url']
            
            elif 'url' in metadata_document_dict:
                
                table_url=metadata_document_dict['url']
            
            else:
            
                raise ValueError
            
            if metadata_document_headers is None:  # local path
            
                table_url_resolved=\
                    os.path.join(
                        os.path.dirname(base_url),
                        table_url
                        )
            
            else:  # remote path
                
                table_url_resolved=\
                    urllib.parse.urljoin(
                        base_url,
                        table_url
                        )
                
            #print('table_url_resolved',table_url_resolved)
            
            if tabular_data_file_headers is None:
                
                if table_url_resolved==absolute_path:
                    
                    return metadata_document_dict,metadata_url
                
            else:
                
                if table_url_resolved==tabular_data_file_url:
                    
                    return metadata_document_dict,metadata_url
    
    # if no matches
    metadata_document_dict=None
    metadata_url=None
    
    return metadata_document_dict,metadata_url
        

#%% 5.4 - Embedded Metadata

def get_metadata_from_embedded_metadata(
        ):
    """
    """
    # Most syntaxes for tabular data provide a facility for embedding 
    # metadata within the tabular data file itself. 
    # The definition of a syntax for tabular data should include a 
    # description of how the syntax maps to an annotated data model, 
    # and in particular how any embedded metadata is mapped into the 
    # vocabulary defined in [tabular-metadata]. 
    # Parsing based on the default dialect for CSV, as described in 8. 
    # Parsing Tabular Data, will extract column titles from the first 
    # row of a CSV file.

    # EXAMPLE 6: http://example.org/tree-ops.csv
    # GID,On Street,Species,Trim Cycle,Inventory Date
    # 1,ADDISON AV,Celtis australis,Large Tree Routine Prune,10/18/2010
    # 2,EMERSON ST,Liquidambar styraciflua,Large Tree Routine Prune,6/2/2010
    # The results of this can be found in section 8.2.1 Simple Example.
    
    # For another example, the following tab-delimited file contains 
    # embedded metadata where it is assumed that comments may be added 
    # using a #, and that the column types may be indicated using a 
    # #datatype annotation:
    
    # EXAMPLE 7: Tab-separated file containing embedded metadata
    # # publisher City of Palo Alto
    # # updated 12/31/2010
    # #name GID on_street species trim_cycle  inventory_date
    # #datatype string  string  string  string  date:M/D/YYYY
    #   GID On Street Species Trim Cycle  Inventory Date
    #   1 ADDISON AV  Celtis australis  Large Tree Routine Prune  10/18/2010
    #   2 EMERSON ST  Liquidambar styraciflua Large Tree Routine Prune  6/2/2010
    
    # A processor that recognises this format may be able to extract and 
    # make sense of this embedded metadata.
        
    pass
        
    

#%% Section 6 - Processing Tables

#%% 6.1 - Creating Annotated Tables

def create_annotated_table_group(
        input_file_path_or_url,
        overriding_metadata_file_path_or_url=None,
        validate=False
        ):
    """
    
    
    :param input_file_path_or_url: Path/url to metadata document (json) or 
        tabular data file (csv). 
    :type input_file_path_ir_url: str
    
    :param overriding_metadata_file_path_or_url: Location of a metadata.json
        file to be used as "overriding metadata".
        Either a) a relative local file path; b) 
        an absolute local file path; or c) a full URL
    :type overriding_metadata_file_path_or_url: str
    
    :param validate: Sets validator
    
    """
    
    tabular_data_file_text=None
    
    # After locating metadata, metadata is normalized and coerced into a 
    # single table group description. 
    # When starting with a metadata file, this involves normalizing the 
    # provided metadata file and verifying that the embedded metadata for 
    # each tabular data file referenced from the metadata is compatible 
    # with the metadata. 
    # When starting with a tabular data file, this involves locating the 
    # first metadata file as described in section 5. Locating Metadata and 
    # normalizing into a single descriptor.
    
    if input_file_path_or_url.endswith('.json'):
        
        input_is_tabular_data_file=False
        
    elif input_file_path_or_url.endswith('.csv'):
        
        input_is_tabular_data_file=True
    
    else:
    
        message='"input_file_path_or_url" must end with either ".json" or ".csv"/'    
    
        raise ValueError(message)
        
        
    # If processing starts with a tabular data file, implementations:
    if input_is_tabular_data_file:
        
        # 1. Retrieve the tabular data file.
        
        try:
            
            with open(input_file_path_or_url) as f:
                
                tabular_data_file_text=f.read()
                tabular_data_file_headers=None
                tabular_data_file_url=os.path.abspath(
                    input_file_path_or_url
                    )
                
        except FileNotFoundError:
            
            try:
            
                response=requests.get(input_file_path_or_url)
                tabular_data_file_text=response.text
                tabular_data_file_headers=response.headers
                tabular_data_file_url=input_file_path_or_url
            
            except (requests.MissingSchema, requests.ConnectionError):
                
                message='"input_file_path_or_url" does not refer to a local or remote file.'    
            
                raise ValueError(message)
                
                
        # 2. Retrieve the first metadata file (FM) as described in 
        #    section 5. Locating Metadata:
        
        # 2.1. metadata supplied by the user (see section 5.1 
        #      Overriding Metadata).
        # 2.2. metadata referenced from a Link Header that may be returned 
        #      when retrieving the tabular data file (see section 5.2 
        #      Link Header).
        # 2.3. metadata retrieved through a site-wide location configuration 
        #      (see section 5.3 Default Locations and Site-wide Location 
        #      Configuration).
        # 2.4. embedded metadata as defined in section 5.4 Embedded 
        #      Metadata with a single tables entry where the url property 
        #      is set from that of the tabular data file.
        
        metadata_document_dict, metadata_document_location=\
            locate_metadata(
                tabular_data_file_url,
                tabular_data_file_text,
                tabular_data_file_headers,
                overriding_metadata_file_path_or_url
                )
        
        if metadata_document_dict is None:  # i.e. using embedded metadata
            
            metadata_document_dict={
                '@context': 'http://www.w3.org/ns/csvw',
                'url': tabular_data_file_url,
                'tableSchema':{}
                }
            
            metadata_document_location=''
            
            use_embedded_metadata_flag=True
            
        else:
            
            use_embedded_metadata_flag=False
        
        # 3. Proceed as if the process starts with FM.
        
        
    # If the process starts with a metadata file:
    else:
        
        # 1. Retrieve the metadata file yielding the metadata UM (which is 
        #    treated as overriding metadata, see section 5.1 Overriding 
        #    Metadata).
                
        try:
            
            with open(input_file_path_or_url) as f:
                
                metadata_document_dict=json.load(f)
                metadata_document_location=os.path.abs(input_file_path_or_url)
                
        except FileNotFoundError:
            
            try:
            
                response=requests.get(input_file_path_or_url)
                text=response.text
                metadata_document_dict=json.loads(text)
                metadata_document_location=input_file_path_or_url
            
            except (requests.MissingSchema, requests.ConnectionError):
                
                message='"input_file_path_or_url" does not refer to a local or remote file.'    
            
                raise ValueError(message)
        
        
    # 2. Normalize UM using the process defined in Normalization 
    # in [tabular-metadata], coercing UM into a table group 
    # description, if necessary.
        
    if 'tables' in metadata_document_dict:  # it's a TableGroup object
        
        metadata_table_group_dict=metadata_document_dict
        
    elif 'url' in metadata_document_dict:  # it's a Table object
    
        # convert to TableGroup object
        metadata_table_group_dict={
            '@context': metadata_document_dict['@context'],
            'tables': [metadata_document_dict]
            }
        metadata_table_group_dict['tables'][0].pop('@context', None)
        
    
    base_url, default_language=\
        validate_and_normalize_metadata_table_group_dict(
            metadata_table_group_dict,
            metadata_document_location
            )
        
        
    # 3 For each table (TM) in UM in order, create one or more annotated tables:
    annotated_table_group_dict={
        'id':None,
        'notes':[],
        'tables':[]
        }
    
    for table_index,metadata_table_dict in \
        enumerate(metadata_table_group_dict['tables']):
               
        # 3.1 Extract the dialect description (DD) from UM for the table 
        #     associated with the tabular data file. If there is no such 
        #     dialect description, extract the first available dialect 
        #     description from a group of tables in which the tabular data 
        #     file is described. Otherwise use the default dialect description.

        default_dialect_flag=False

        dialect_description_dict=\
            metadata_table_dict.get('dialect',None)
            
        # gets the first dialect description in the group of tables
        
        if dialect_description_dict is None:
            dialect_description_dict=\
                metadata_table_group_dict.get('dialect',None)
        
        if dialect_description_dict is None:
            for metadata_table_dict2 in \
                metadata_table_group_dict['tables']:
                    if 'dialect' in metadata_table_dict2:
                        dialect_description_dict=\
                            metadata_table_dict2['dialect']
                        break
                    
        # gets the default dialect description
        if dialect_description_dict is None:
            
            default_dialect_flag=True
            
            dialect_description_dict=dict(
                commentPrefix='#',
                delimiter=',',
                doubleQuote=True,
                encoding='utf-8',
                header=True,
                headerRowCount=1,
                lineTerminators=["\r\n", "\n"],
                quoteChar='"',
                skipBlankRows=False,
                skipColumns=0,
                skipInitialSpace=False,
                skipRows=0,
                trim=True
                )
        
        
        # 3.2 If using the default dialect description, override default values 
        #     in DD based on HTTP headers found when retrieving the tabular data file:
        #     - If the media type from the Content-Type header is text/tab-separated-values, 
        #       set delimiter to TAB in DD.
        #     - If the Content-Type header includes the header parameter with a 
        #       value of absent, set header to false in DD.
        #     - If the Content-Type header includes the charset parameter, set 
        #       encoding to this value in DD.
        
        if tabular_data_file_text is None:
            
            url=metadata_table_dict['url']
        
            try:
                
                with open(url) as f:
                    
                    tabular_data_file_text=f.read()
                    tabular_data_file_headers=None
                    tabular_data_file_url=os.path.abspath(
                        url
                        )
                    
            except FileNotFoundError:
                
                try:
                
                    response=requests.get(url)
                    tabular_data_file_text=response.text
                    tabular_data_file_headers=response.headers
                    tabular_data_file_url=url
                
                except (requests.MissingSchema, requests.ConnectionError):
                    
                    message=f'Property "url" with value "{url}" does not '
                    message+='refer to a local or remote file.'    
                
                    raise ValueError(message)
        
        
        if default_dialect_flag:
            
            if not tabular_data_file_headers is None:
        
                content_type=tabular_data_file_headers.get('Content-Type',None)
                if not content_type is None:
                    if 'text/tab-separated-values' in content_type:  # NEEDS TESTING
                        dialect_description_dict['delimter']='\t'
                    if 'header=absent' in content_type:  # NEEDS TESTING
                        dialect_description_dict['header']=False
                    if 'charset' in content_type:  # NEEDS TESTING
                        charset_value=\
                            content_type.split('charset')[1].split(';')[0].strip()[1:]  # NEEDS TESTING
                        dialect_description_dict['encoding']=charset_value
            
        
        
        # 3.3 Parse the tabular data file, using DD as a guide, to create a 
        #     basic tabular data model (T) and extract embedded metadata (EM), 
        #     for example from the header line.

        annotated_table_dict, embedded_metadata_dict=\
            parse_tabular_data_from_text(
                tabular_data_file_text,
                tabular_data_file_url,
                dialect_description_dict
                )
            
        # if using embedded metadata
        if use_embedded_metadata_flag:
            metadata_table_dict=embedded_metadata_dict
            metadata_table_dict.pop('@context')
            metadata_table_group_dict['tables'][table_index]=metadata_table_dict
            #print(metadata_table_group_obj_dict)
            
        # if metadata_table_obj_dict does not contain a tableSchema object,
        # then set default column names
        # - used to pass test023, test100
        if not 'tableSchema' in metadata_table_dict \
            or len(metadata_table_dict['tableSchema'].get('columns',[]))==0:
                
            columns=[{'name': f'_col.{i+1}'} 
                     for i in range(len(embedded_metadata_dict['tableSchema']['columns']))]
            
            metadata_table_dict['tableSchema']=\
                {'columns':columns}
        #print('metadata_table_obj_dict',metadata_table_obj_dict)  
        
        # include virtual columns from metadata
        #  as this isn't included when parsing the tabular data from text.
        for i, metadata_column_dict in \
            enumerate(metadata_table_dict['tableSchema']['columns']):
                
            if metadata_column_dict.get('virtual')==True:
                
                annotated_column_dict=dict(
                    table=annotated_table_dict, 
                    number=i+1,
                    sourceNumber=None,
                    name=None,
                    titles=[],
                    virtual=False,
                    suppressOutput=False,
                    datatype='string', 
                    default='',
                    lang='und',
                    null='',
                    ordered=False,
                    required=False,
                    separator=None,
                    textDirection='auto',
                    aboutURL=None,
                    propertyURL=None,
                    valueURL=None,
                    cells=[]
                    )
                
                for annotated_row_dict in annotated_table_dict['rows']:
                
                    annotated_cell_dict=dict(
                        table=annotated_table_dict, 
                        column=annotated_column_dict, 
                        row=annotated_row_dict, 
                        stringValue='',
                        value=None,
                        errors=[],
                        textDirection='auto',
                        ordered=False,
                        aboutURL=None,
                        propertyURL=None,
                        valueURL=None
                        )
                
                    annotated_column_dict['cells'].append(annotated_cell_dict)
                    annotated_row_dict['cells'].append(annotated_cell_dict)
        
                annotated_table_dict['columns'].append(annotated_column_dict)
            
        
        # 3.4 If a Content-Language HTTP header was found when retrieving the 
        #     tabular data file, and the value provides a single language, set 
        #     the lang inherited property to this value in TM, unless TM 
        #     already has a lang inherited property.
        
        if not tabular_data_file_headers is None:
        
            content_language=tabular_data_file_headers.get('Content-Language',None)
            
            if not content_language is None:
                
                if not 'lang' in metadata_table_dict:
                    
                    metadata_table_dict['lang']=content_language  # NEEDS TESTING
    
        # 3.5 Verify that TM is compatible with EM using the procedure defined 
        #     in Table Description Compatibility in [tabular-metadata]; if TM 
        #     is not compatible with EM validators must raise an error, other 
        #     processors must generate a warning and continue processing.
        
        compare_table_descriptions(
            metadata_table_dict,
            embedded_metadata_dict,
            validate=validate
            )
        
        annotated_table_group_dict['tables'].append(annotated_table_dict)
        
        
        #???????
        
        # Not directly in this section of the standard, but Section 8.2.1.1
        # suggests that the metadata_table_obj_dict is merged with the embedded metadata
        # Here this is done for certain properties which are present in the 
        # embedded metadata but not present in the metadata_table_obj_dict
        # NOTE - may need improving, might not work for all properties and cases
        # at present
        
        # if from_csv:        
        
        #     metadata_table_dict=\
        #         merge_metadata_objs(
        #             metadata_table_obj_dict,
        #             embedded_metadata_dict
        #             )
                
        #???????
        
        
    # 3.6 Use the metadata TM to add annotations to the tabular data model 
    #     T as described in Section 2 Annotating Tables in [tabular-metadata].
    
    annotated_table_group_dict=\
        annotate_table_group(
            annotated_table_group_dict,
            metadata_table_group_dict,
            default_language,
            base_url
            )
            
    # Not directly in this section of the standard, but at this stage the 
    # cell values are parsed.
    # This is done after the metadata annotations are included in the 
    # annotated_table_group_dict
    
    for annotated_table_dict in annotated_table_group_dict['tables']:
        #print(annotated_table_dict)
        
        for annotated_column_dict in annotated_table_dict['columns']:
            
            for annotated_cell_dict in annotated_column_dict['cells']:
                
                value,errors=\
                    parse_cell(
                        string_value=annotated_cell_dict['stringValue'],
                        datatype=annotated_column_dict['datatype'],
                        default=annotated_column_dict['default'],
                        lang=annotated_column_dict['lang'],
                        null=annotated_column_dict['null'],
                        required=annotated_column_dict['required'],
                        separator=annotated_column_dict['separator'],
                        trim=dialect_description_dict.get('trim',True)
                        )
                    
                annotated_cell_dict['value']=value
                annotated_cell_dict['errors'].append(errors)
                #print(annotated_column_dict['aboutURL'])
     
                #print(value)           
     
    # generate URIs
                
    for annotated_table_dict in annotated_table_group_dict['tables']:
        
        tabular_data_file_path, tabular_data_file_url=\
            get_path_and_url_from_file_location(
                annotated_table_dict['url']
                )
        
        for annotated_column_dict in annotated_table_dict['columns']:
            
            for annotated_cell_dict in annotated_column_dict['cells']:
                
                tabular_data_file_path, tabular_data_file_url=\
                    get_path_and_url_from_file_location(
                        tabular_data_file_path_or_url
                        )
                
                # If there is a about URL annotation on the column, it becomes 
                # the about URL annotation on the cell, after being transformed 
                # into an absolute URL as described in URI Template Properties 
                # of [tabular-metadata].
                if not annotated_cell_dict['aboutURL'] is None:
                    annotated_cell_dict['aboutURL']=\
                        get_URI_from_URI_template(
                            annotated_cell_dict['aboutURL'],
                            annotated_cell_dict,
                            tabular_data_file_path, 
                            tabular_data_file_url
                            )  
                        
                # If there is a property URL annotation on the column, it becomes 
                # the property URL annotation on the cell, after being transformed 
                # into an absolute URL as described in URI Template Properties 
                # of [tabular-metadata].
                if not annotated_cell_dict['propertyURL'] is None:
                    annotated_cell_dict['propertyURL']=\
                        get_URI_from_URI_template(
                            annotated_cell_dict['propertyURL'],
                            annotated_cell_dict,
                            tabular_data_file_path, 
                            tabular_data_file_url
                            )  
                        
                # If there is a value URL annotation on the column, it becomes 
                # the value URL annotation on the cell, after being transformed 
                # into an absolute URL as described in URI Template Properties 
                # of [tabular-metadata]. The value URL annotation is null if the cell value is null and the column virtual annotation is false.
                if not annotated_cell_dict['valueURL'] is None:
                    
                    # ATTEMPT TO ALLOW LISTS IN valueURL - now removed
                    # if isinstance(annotated_cell_dict['value'],list):
                        
                    #     result=[]
                        
                    #     for value in annotated_cell_dict['value']:
                            
                    #         result.append(get_URI_from_URI_template(
                    #                         annotated_cell_dict['valueURL'],
                    #                         annotated_cell_dict,
                    #                         tabular_data_file_path, 
                    #                         tabular_data_file_url,
                    #                         value=value
                    #                         )  
                    #                     )
                            
                    #     annotated_cell_dict['valueURL']=result
                        
                    # else:
                    
                    annotated_cell_dict['valueURL']=\
                        get_URI_from_URI_template(
                            annotated_cell_dict['valueURL'],
                            annotated_cell_dict,
                            tabular_data_file_path, 
                            tabular_data_file_url
                            )  
         
                    #print(annotated_cell_dict['value'])
                    #print(annotated_cell_dict['valueURL'])
             
     
 
 
        
    #print(metadata_table_group_obj_dict)
        
    return annotated_table_group_dict
     
        
        
    raise NotImplementedError
        

#%% 6.4 - Parsing Cells

def parse_cell(
        string_value,
        datatype,
        default,
        lang,
        null,
        required,
        separator,
        trim=True,
        p=False
        ):
    """
    
    
    :returns: (Cell value {@value:..., @type:...},
               errors)
    :rtype: tuple
    
    
    """
    
    if not isinstance(datatype,dict):
        datatype={'base':datatype}
    #print(datatype)
    
    if not isinstance(null,list):
        null=[null]
    
    base=datatype['base']
    
    errors=[]
    
    # The process of parsing the string value into a single value or a list 
    # of values is as follows:
        
    # 1 unless the datatype base is string, json, xml, html or anyAtomicType, 
    # replace all carriage return (#xD), line feed (#xA), and tab (#x9) 
    # characters with space characters.
    if base not in ['string','json','xml','html','anyAtomicType']:
        string_value=string_value.replace('\r',' ')
        string_value=string_value.replace('\n',' ')
        string_value=string_value.replace('\t',' ')
    
    # 2 unless the datatype base is string, json, xml, html, anyAtomicType, 
    # or normalizedString, strip leading and trailing whitespace from the 
    # string value and replace all instances of two or more whitespace #
    # characters with a single space character.
    if base not in ['string','json','xml','html','anyAtomicType','normalizedString']:
        string_value=string_value.strip()
        string_value=" ".join(string_value.split()) # includes whitespace such as '\n' etc.
                                                      # if this should be spaces only,  
                                                      # could use st=re.sub(' +',' ', st)
                                
    # 3 if the normalized string is an empty string, apply the remaining 
    # steps to the string given by the column default annotation.
    if separator is None and string_value=='':
        string_value=str(default)
        
    # 4 if the column separator annotation is not null and the normalized 
    # string is an empty string, the cell value is an empty list. If the 
    # column required annotation is true, add an error to the list of errors for the cell.
    if not separator is None and string_value=='':
        
        list_of_cell_values=[]
        
        if required:
            
            # ADD ERROR
            errors.append('Cell is required but not provided')
            
        #cell_value={'@value':list_of_json_values,
        #            '@type':type_}
            
        return list_of_cell_values,errors
        
    # 5 if the column separator annotation is not null, the cell value is a 
    # list of values; set the list annotation on the cell to true, and create 
    # the cell value created by:
    if not separator is None:
        
        # "set the list annotation on the cell to true"
        # - not done, as there is no 'list' annotation on the cell object
        
        # 5.1 if the normalized string is the same as any one of the values 
        # of the column null annotation, then the resulting value is null.
        if string_value in null:
            
            cell_value=None
            
            # json_value=None
            
            # cell_value={'@value':json_value,
            #             '@type':type_}
            
            return cell_value, errors
            
        # 5.2 split the normalized string at the character specified by the 
        # column separator annotation.
        else:
            
            list_of_string_values=string_value.split(separator)
                
            # 5.3 unless the datatype base is string or anyAtomicType, strip 
            # leading and trailing whitespace from these strings.  
            if not base not in ['string','anyAtomicType']:
                list_of_string_values=[x.strip() for x in list_of_string_values]
                
            # 5.4 applying the remaining steps to each of the strings in turn.
            
            list_of_cell_values=[]
            for string_value in list_of_string_values:
                
                json_value,language,type_,errors=parse_cell_part_2(
                        string_value,
                        errors,
                        datatype,
                        default,
                        lang,
                        null,
                        required,
                        separator,
                        p=p
                        )
                
                # needed for test036
                if isinstance(json_value,str):
                    json_value=get_trimmed_cell_value(
                        json_value,
                        trim
                        )
                
                cell_value={'@value':json_value,
                            '@type':datatypes[type_]}
                
                if not language is None:
                    cell_value['@language']=lang
                
                list_of_cell_values.append(cell_value)
                
            return list_of_cell_values,errors
                
                    
    json_value,language,type_,errors=\
        parse_cell_part_2(
            string_value,
            errors,
            datatype,
            default,
            lang,
            null,
            required,
            separator,
            p=p
            )
        
    if not json_value is None:
        
        cell_value={'@value':json_value,
                    '@type':datatypes[type_]}
            
        if not language is None:
            cell_value['@language']=lang
            
    else:
        
        cell_value=None
        
    return cell_value,errors


def parse_cell_part_2(
        string_value,
        errors,
        datatype,
        default,
        lang,
        null,
        required,
        separator,
        p=False
        ):
    """
    
    
    :returns: (json_value, language, errors)
    :rtype: tuple
    
    """
    
    if p: print('datatype', datatype)
    
    if not datatype['base'] is None:
        type_=datatype['base']
    else:
        type_='string'
    
    
    language=None
    
    # 6 if the string is an empty string, apply the remaining steps to the 
    # string given by the column default annotation.
    if string_value=='':
        string_value=str(default)
        
    # 7 if the string is the same as any one of the values of the column null 
    # annotation, then the resulting value is null. If the column separator 
    # annotation is null and the column required annotation is true, add 
    # an error to the list of errors for the cell.
    if string_value in null:
        
        json_value=None
        language=None
        
        if separator is None and required==True:
            
            # ADD ERROR
            raise NotImplementedError
        
        return json_value, language, type_, errors  # returns None
    
    # 8 parse the string using the datatype format if one is specified, as 
    # described below to give a value with an associated datatype. 
    # If the datatype base is string, or there is no datatype, the value has 
    # an associated language from the column lang annotation. 
    # If there are any errors, add them to the list of errors for the cell; 
    # in this case the value has a datatype of string; if the datatype base 
    # is string, or there is no datatype, the value has an associated language 
    # from the column lang annotation.
    
    # numbers
    if datatype['base'] in datatypes_numbers:
        
        json_value,type_,errors=parse_number(
            string_value,
            type_,
            datatype.get('format',None),
            errors
            )
        
    # booleans
    elif datatype['base']=='boolean':
        
        json_value,type_,errors=parse_boolean(
            string_value,
            type_,
            datatype.get('format',None),
            errors
            )
        
    # date
    elif datatype['base']=='date':
        
        json_value,type_,errors=parse_date(
            string_value,
            type_,
            datatype.get('format',None),
            errors
            )
        
    # time
    elif datatype['base']=='time':
        
        json_value,type_,errors=parse_time(
            string_value,
            type_,
            datatype.get('format',None),
            errors
            )
    
    # datetime
    elif datatype['base'] in ['dateTime', 'datetime']:
        
        json_value,type_,errors=parse_datetime(
            string_value,
            type_,
            datatype.get('format',None),
            errors
            )
    
    # datetimestamp
    elif datatype['base']=='dateTimeStamp':
        
        json_value,type_,errors=parse_datetimestamp(
            string_value,
            type_,
            datatype.get('format',None),
            errors
            )
        
    # durations
    
    
    # other types
    else:
    
        # format & validate value
        # TO DO
        
        language=lang
        
        json_value, type_, errors=parse_other_types(
            string_value,
            type_,
            datatype.get('format',None),
            errors)
            
    
    # 9 validate the value based on the length constraints described in 
    # section 4.6.1 Length Constraints, the value constraints described 
    # in section 4.6.2 Value Constraints and the datatype format annotation 
    # if one is specified, as described below. 
    # If there are any errors, add them to the list of errors for the cell.
    
    # TO DO
    
    
    
    #print(json_value, language, errors)
    
    return json_value, language, type_, errors


#%% 6.4.2 Formats for numeric type

def parse_number(
        string_value,
        datatype_base,
        datatype_format,
        errors
        ):
    """
    """
    
    type_=datatype_base
    
    # the datatype format annotation indicates the expected format for that 
    # number. Its value must be either a single string or an object with one 
    # or more of the properties
    
    # decimalChar
    # A string whose value is used to represent a decimal point within the number. 
    # The default value is ".". If the supplied value is not a string, 
    # implementations must issue a warning and proceed as if the property 
    # had not been specified.
    
    decimal_char='.'
    
    if isinstance(datatype_format,dict) and 'decimalChar' in datatype_format:
        
        decimal_char=datatype_format['decimalChar']
            
        if not isinstance(decimal_char,str):
         
             # ISSUE WARNING
             decimal_char='.'
        
    # groupChar
    # A string whose value is used to group digits within the number. 
    # The default value is null. If the supplied value is not a string, 
    # implementations must issue a warning and proceed as if the property had 
    # not been specified.
    
    group_char=None
    
    if isinstance(datatype_format,dict) and 'groupChar' in datatype_format:
        
        group_char=datatype_format['groupChar']
            
        if not isinstance(group_char,str):
         
             # ISSUE WARNING
             group_char=None
    
    
    # pattern
    # A number format pattern as defined in [UAX35]. 
    # Implementations must recognise number format patterns containing the 
    # symbols 0, #, the specified decimalChar (or "." if unspecified), the 
    # specified groupChar (or "," if unspecified), E, +, % and ‰. 
    # Implementations may additionally recognise number format patterns 
    # containing other special pattern characters defined in [UAX35]. 
    # If the supplied value is not a string, or if it contains an invalid 
    # number format pattern or uses special pattern characters that the 
    # implementation does not recognise, implementations must issue a warning 
    # and proceed as if the property had not been specified.
    
    pattern=None
    
    if isinstance(datatype_format,dict) and 'pattern' in datatype_format:
        
        pattern=datatype_format['pattern']
        
        if not isinstance(pattern,str):
            
            # ISSUE WARNING
            pattern=None
            
    # If the datatype format annotation is a single string, this is 
    # interpreted in the same way as if it were an object with a pattern 
    # property whose value is that string.
    elif isinstance(datatype_format,str):
                  
        pattern=datatype_format
        
        if not isinstance(pattern,str):
            
            # ISSUE WARNING
            pattern=None
    
    
    #  If the groupChar is specified, but no pattern is supplied, when parsing 
    # the string value of a cell against this format specification, 
    # implementations must recognise and parse numbers that consist of:

    # 1. an optional + or - sign,
    # 2. followed by a decimal digit (0-9),
    # 3. followed by any number of decimal digits (0-9) and the string 
    #    specified as the groupChar,
    # 4. followed by an optional decimalChar followed by one or more decimal 
    #    digits (0-9),
    # 5. followed by an optional exponent, consisting of an E followed by an 
    #    optional + or - sign followed by one or more decimal digits (0-9), or
    # 6. followed by an optional percent (%) or per-mille (‰) sign.
    
    # or that are one of the special values:
    
    # 1. NaN,
    # 2. INF, or
    # 3. -INF.
    
    

    
    
    # Implementations may also recognise numeric values that are in any of the 
    # standard-decimal, standard-percent or standard-scientific formats listed 
    # in the Unicode Common Locale Data Repository.
    
    # TO DO
    
    
    # Implementations must add a validation error to the errors annotation 
    # for the cell, and set the cell value to a string rather than a number 
    # if the string being parsed:

    # - is not in the format specified in the pattern, if one is defined
    
    if not pattern is None:
        
        raise NotImplementedError
    
    # - otherwise, if the string
    #     - does not meet the numeric format defined above,
    #     - contains two consecutive groupChar strings,
    
    # TO DO
    
    # - contains the decimalChar, if the datatype base is integer or one of 
    #   its sub-types,
    
    if datatype_base in datatypes_integers:
        
        if decimal_char in string_value:
            
            json_value=string_value
            
            type_='string'
            
            errors.append(f'Value "{string_value}" not valid as it contains the decimalChar character "{decimal_char}"')
            
            return json_value, type_, errors  
            
    # - contains an exponent, if the datatype base is decimal or one of its 
    #   sub-types, or
    
    # DONE BELOW
    
    # - is one of the special values NaN, INF, or -INF, if the datatype base 
    #   is decimal or one of its sub-types.
    
    if datatype_base=='decimal':
        
        if string_value in ['Nan','INF','-INF']:
            
            json_value=string_value
            
            type_='string'
            
            errors.append(f'Value "{string_value}" not valid as it ...')  # TO DO
            
            return json_value, type_, errors  
    
    
    # Implementations must use the sign, exponent, percent, and per-mille signs 
    # when parsing the string value of a cell to provide the value of the cell. 
    # For example, the string value "-25%" must be interpreted as -0.25 and 
    # the string value "1E6" as 1000000.
    
    
    
    
    # deals with percent and permille
    modifier=1
    if string_value.endswith('%'):
        string_value=string_value[:-1]
        modifier=0.01
    elif string_value.endswith('‰'):
        string_value=string_value[:-1]
        modifier=0.001
    
    
    # replace decimal_char and group_char characters
    if not decimal_char=='.':
        string_value=string_value.replace(decimal_char,'.')
    if not group_char is None: 
        string_value=string_value.replace(group_char,'')
    
    # convert string to number    
    if datatype_base in datatypes_integers:
    
        try:
            json_value=int(string_value)  # will fail if an exponential number
            json_value=json_value*modifier
        except ValueError:
            json_value=string_value
            type_='string'
            errors.append(f'Value "{string_value}" is not a valid integer')
    
    elif datatype_base=='decimal':
        
        try:
            json_value=float(string_value)  # will not fail if an exponential number
            json_value=json_value*modifier
            
        except ValueError:
            json_value=string_value
            type_='string'
            errors.append(f'Value "{string_value}" is not a valid decimal')
    
        try:
            for x in string_value.split['.']:
                int(x)  # will fail if an expoential number
             
        except ValueError:
            json_value=string_value
            type_='string'
            errors.append(f'Value "{string_value}" is not a valid decimal')
    
    
    else:
        
        try:
            json_value=float(string_value)  # assuming that this will work for all valid cases of ULDM number formats...
            json_value=json_value*modifier
        except ValueError:
            json_value=string_value
            errors.append(f'Value "{string_value}" is not a valid number')
            
    
    return json_value, type_, errors


def parse_number_pattern(
        pattern,
        p=False
        ):
    """Breaks down a number pattern into constituent components.
    
    :param pattern: A number patter as specified in the Unicode Locale
        Data Markup Language.
    
    """
    if p: print('pattern',pattern)
    
    x=pattern.split(';')
    positive_pattern=x[0].strip()
    if len(x)==2:
        negative_pattern=x[1].strip()
    else:
        negative_pattern=None
    if p: print('positive_pattern',positive_pattern)
    if p: print('negative_pattern',negative_pattern)
        
    # mantissa and exponent in scientific notation
    x=positive_pattern.split('E')
    positive_pattern_mantissa_part=x[0]
    if len(x)==2:
        positive_pattern_exponent_part=x[1]
    else:
        positive_pattern_exponent_part=None
    if p: print('positive_pattern_mantissa_part',
                positive_pattern_mantissa_part)
    if p: print('positive_pattern_exponent_part',
                positive_pattern_exponent_part)
    
    # integral and fractional parts
    x=positive_pattern_mantissa_part.split('.')
    positive_pattern_integral_part=x[0]
    if len(x)==2:
        positive_pattern_fractional_part=x[1]
    else:
        postive_pattern_fractional_part=None
    if p: print('positive_pattern_integral_part',
                positive_pattern_integral_part)
    if p: print('positive_pattern_fractional_part',
                positive_pattern_fractional_part)

    reverse_positive_pattern_integral_part=positive_pattern_integral_part[::-1]
    if p: print('reverse_positive_pattern_integral_part',
                reverse_positive_pattern_integral_part)    
    
    # integral grouping size
    positive_pattern_integral_part_primary_grouping_size=None
    positive_pattern_integral_part_secondary_grouping_size=None
    positions_of_group_char=[i for i, x 
                             in enumerate(reverse_positive_pattern_integral_part) 
                             if x==',']
    if p: print('positions_of_group_char',positions_of_group_char)
    if len(positions_of_group_char)>0:
        positive_pattern_integral_part_primary_grouping_size=positions_of_group_char[0]
    if len(positions_of_group_char)>1:
        positive_pattern_integral_part_secondary_grouping_size=\
            positions_of_group_char[1]-positions_of_group_char[0]
    if p: print('positive_pattern_integral_part_primary_grouping_size',
                positive_pattern_integral_part_primary_grouping_size)
    if p: print('positive_pattern_integral_part_secondary_grouping_size',
                positive_pattern_integral_part_secondary_grouping_size)
        
    
    reverse_positive_pattern_integral_part_no_group_char=\
        reverse_positive_pattern_integral_part.replace(',','')
    if p: print('reverse_positive_pattern_integral_part_no_group_char',
          reverse_positive_pattern_integral_part_no_group_char)
    
    # positive_pattern_zero_padding_count
    i=0
    positive_pattern_integral_part_zero_padding_count=0
    while True:
        if i==len(reverse_positive_pattern_integral_part_no_group_char):
            break
        if reverse_positive_pattern_integral_part_no_group_char[i]=='0':
            positive_pattern_integral_part_zero_padding_count+=1
            i+=1
        else:
            break
    if p: print('positive_pattern_integral_part_zero_padding_count',
          positive_pattern_integral_part_zero_padding_count)
           
    # skip past hash symbols
    while True:
        if i==len(reverse_positive_pattern_integral_part_no_group_char):
            break
        if reverse_positive_pattern_integral_part_no_group_char[i]=='#':
            i+=1
        else:
            break
    
    # prefix
    reverse_positive_pattern_integral_part_prefix=''
    while True:
        if i==len(reverse_positive_pattern_integral_part_no_group_char):
            break
        x=reverse_positive_pattern_integral_part_no_group_char[i]
        if x in ['+','-','%','‰']:
            reverse_positive_pattern_integral_part_prefix+=x
            i+=1
        else:
            raise Exception
    positive_pattern_integral_part_prefix=reverse_positive_pattern_integral_part_prefix[::-1]
    if p: print('positive_pattern_integral_part_prefix',
                positive_pattern_integral_part_prefix)
    
    # positive_pattern_fractional_part_zero_padding_count
    i=0
    positive_pattern_fractional_part_zero_padding_count=0
    while True:
        if i==len(positive_pattern_fractional_part):
            break
        if positive_pattern_fractional_part[i]=='0':
            positive_pattern_fractional_part_zero_padding_count+=1
            i+=1
        else:
            break
    if p: print('positive_pattern_fractional_part_zero_padding_count',
                positive_pattern_fractional_part_zero_padding_count)
           
    # positive_pattern_fractional_part_hash_padding_count
    positive_pattern_fractional_part_hash_padding_count=0
    while True:
        if i==len(positive_pattern_fractional_part):
            break
        if positive_pattern_fractional_part[i]=='#':
            positive_pattern_fractional_part_hash_padding_count+=1
            i+=1
        else:
            break
    if p: print('positive_pattern_fractional_part_hash_padding_count',
                positive_pattern_fractional_part_hash_padding_count)
    
    # suffix
    positive_pattern_fractional_part_suffix=''
    while True:
        if i==len(positive_pattern_fractional_part):
            break
        x=positive_pattern_fractional_part[i]
        if x in ['+','-','%','‰']:
            positive_pattern_fractional_part_suffix+=x
            i+=1
        else:
            raise Exception
    if p: print('positive_pattern_fractional_part_suffix',
                positive_pattern_fractional_part_suffix)
    
    return dict(
        positive_pattern_integral_part_primary_grouping_size=\
            positive_pattern_integral_part_primary_grouping_size,
        positive_pattern_integral_part_secondary_grouping_size=\
            positive_pattern_integral_part_secondary_grouping_size,
        positive_pattern_integral_part_zero_padding_count=\
            positive_pattern_integral_part_zero_padding_count,
        positive_pattern_integral_part_prefix=\
            positive_pattern_integral_part_prefix,
        positive_pattern_fractional_part_zero_padding_count=\
            positive_pattern_fractional_part_zero_padding_count,
        positive_pattern_fractional_part_hash_padding_count=\
            positive_pattern_fractional_part_hash_padding_count,
        positive_pattern_fractional_part_suffix=\
            positive_pattern_fractional_part_suffix,
        
        
        )
        
        
        
    print(positive_pattern.index('#'))
    print(positive_pattern.index('0'))
    print(positive_pattern.index('+'))
    print(positive_pattern.index('-'))
    


#%% 6.4.3 Formats for booleans

def parse_boolean(
        string_value,
        datatype_base,
        datatype_format,
        errors
        ):
    """
    """
    type_=datatype_base
    
    # Boolean values may be represented in many ways aside from the standard 
    # 1 and 0 or true and false.
    
    if string_value in ['1','true']:
        
        json_value=True
        
        return json_value, type_, errors
    
    elif string_value in ['0','false']:
        
        json_value=False
        
        return json_value, type_, errors
    
    #If the datatype base for a cell is boolean, the datatype format 
    # annotation provides the true value followed by the false value, 
    # separated by |. 
    # For example if format is Y|N then cells must hold either Y or N with 
    # Y meaning true and N meaning false. 
    # If the format does not follow this syntax, implementations must 
    # issue a warning and proceed as if no format had been provided.
    
    if not datatype_format is None:
        
        x=datatype_format.split('|')
        
        if len(x)==0:
            
            warnings.warn('')
            
        elif len(x)>2:
            
            warnings.warn('')
            
        else:
            
            true_string=x[0]
            
            if len(true_string)==0:
                
                warnings.warn()
                
            else:
                
                false_string=x[1]
                
                if len(false_string)==0:
                    
                    warnings.warn()
                    
                else:
                    
                    if string_value in true_string:
                        
                        json_value=True
                        
                        return json_value, type_, errors
                    
                    elif string_value in false_string:
                        
                        json_value=False
                        
                        return json_value, type_, errors
            
    # if no match
    json_value=string_value
    type_='string'
    errors.append('no match to boolean')        
    
    return json_value, type_, errors
    
    



#%% 6.4.4. Formats for dates and times

date_formats=[
    'yyyy-MM-dd',
    'yyyyMMdd',
    'dd-MM-yyyy',
    'd-M-yyyy',
    'MM-dd-yyyy',
    'M-d-yyyy',
    'dd/MM/yyyy',
    'd/M/yyyy',
    'MM/dd/yyyy',
    'M/d/yyyy',
    'dd.MM.yyyy',
    'd.M.yyyy',
    'MM.dd.yyyy',
    'M.d.yyyy',
    ]


def parse_date(
        string_value,
        datatype_base,
        datatype_format,
        errors
        ):
    """
    """
    
    type_=datatype_base
    
    # By default, dates and times are assumed to be in the format defined 
    # in [xmlschema11-2]. However dates and times are commonly represented 
    # in tabular data in other formats.

    if datatype_format is None:
        
        datatype_format='yyyy-MM-dd'  # NEEDS CHECKING ??

    # If the datatype base is a date or time type, the datatype format 
    # annotation indicates the expected format for that date or time.
    
    # The supported date and time format patterns listed here are 
    # expressed in terms of the date field symbols defined in [UAX35]. 
    # These formats must be recognised by implementations and must be 
    # interpreted as defined in that specification. 
    # Implementations may additionally recognise other date format patterns. 
    # Implementations must issue a warning if the date format pattern is 
    # invalid or not recognised and proceed as if no date format 
    # pattern had been provided.
    
    date_and_timezone_format=datatype_format
    
    # separate date format and possible timezone format
    timezone_format, timezone_gap=get_timezone_format(date_and_timezone_format)

    if not timezone_format is None:

        date_format=date_and_timezone_format[:-len(timezone_format)-len(timezone_gap)]
        
    else:

        date_format=date_and_timezone_format        

    # check if date format is in the approved list
    if not date_format in date_formats:
        
        raise Exception
        
    # reformat timezone in string_value for Python parsing
    if not timezone_format is None:
        
        timezone_string=get_timezone_string(string_value)
        
        reformatted_timezone_string=\
            reformat_timezone_in_string_value(
                timezone_string,
                timezone_format
                )
    
        string_value=\
            string_value.replace(
                timezone_string,
                reformatted_timezone_string)
    
    # reformat date_format for Python parsing
    
    x=date_and_timezone_format
    x=x.replace('yyyy','%Y')
    x=x.replace('MM','%m')
    x=x.replace('M','%m')
    if 'dd' in x:
        x=x.replace('dd','%d')
    else:
        x=x.replace('d','%d')
    
    if not timezone_format is None:
        x=x.replace(timezone_format,'%z')
    
    # parse date
    #print(string_value)
    dt=datetime.datetime.strptime(string_value, x)  
    dt_isoformat=dt.isoformat()
    
    # set date value
    json_value=dt_isoformat.split('T')[0]
    
    # include timezone info as date
    if not timezone_format is None:
        json_value=json_value+dt_isoformat[-6:]
    
    return json_value, type_, errors
    

def reformat_timezone_in_string_value(
        timezone_string,
        timezone_format
        ):
    """
    """
    if timezone_string=='Z':
        
        if timezone_format in ['X','XX','XXX']:
        
            return '+0000'
        
        else:
            
            raise Exception  # 'Z' not allowed in formats x, xx, or xxx
    
    elif len(timezone_string)==3:
        
        if timezone_format in ['X','x']:
            
            return timezone_string+'00'
        
        else:
            
            raise Exception  # timezone string should include minutes
            
    elif len(timezone_string)==5:
        
        if timezone_format in ['X','XX','x','xx']:
            
            return timezone_string
        
        else:
            
            raise Exception  
            
    elif len(timezone_string)==6:
        
        if timezone_format in ['XXX','xxx']:
            
            return timezone_string.replace(':','')
        
        else:
            
            raise Exception  
            
            
def get_timezone_string(
        string_value
        ):
    """
    """
    x=string_value.strip()
    
    if x.endswith('Z'):
        
        return 'Z'
    
    elif x[-3] in ['+','-']:
        
        return x[-3:]
    
    elif x[-5] in ['+','-']:
        
        return x[-5:]
    
    elif x[-6] in ['+','-']:
        
        return x[-6:]
    
    else:
        
        raise Exception
    
        
def get_timezone_format(
        datatype_format
        ):
    """
    """
    x=datatype_format.strip()
    
    if x.endswith('XXX'):
        timezone_format='XXX'
    elif x.endswith('XX'):
        timezone_format='XX'
    elif x.endswith('X'):
        timezone_format='X'
    elif x.endswith('xxx'):
        timezone_format='xxx'
    elif x.endswith('xx'):
        timezone_format='xx'
    elif x.endswith('x'):
        timezone_format='x'
    else:
        timezone_format=None
        timezone_gap=''
    
    if not timezone_format is None:
        x=len(timezone_format)
        if datatype_format[-x-1]==' ':
            timezone_gap=' '
        else:
            timezone_gap=''
        
    return timezone_format,timezone_gap
        
        
def parse_time(
        string_value,
        datatype_base,
        datatype_format,
        errors
        ):
    """
    """
    
    type_=datatype_base
    
    # By default, dates and times are assumed to be in the format defined 
    # in [xmlschema11-2]. However dates and times are commonly represented 
    # in tabular data in other formats.

    if datatype_format is None:
        
        datatype_format='HH:mm:ss'  

    # If the datatype base is a date or time type, the datatype format 
    # annotation indicates the expected format for that date or time.
    
    # The supported date and time format patterns listed here are 
    # expressed in terms of the date field symbols defined in [UAX35]. 
    # These formats must be recognised by implementations and must be 
    # interpreted as defined in that specification. 
    # Implementations may additionally recognise other date format patterns. 
    # Implementations must issue a warning if the date format pattern is 
    # invalid or not recognised and proceed as if no date format 
    # pattern had been provided.
    
    time_and_timezone_format=datatype_format
    
    # separate time format and possible timezone format
    timezone_format, timezone_gap=get_timezone_format(time_and_timezone_format)

    if not timezone_format is None:

        time_format=time_and_timezone_format[:-len(timezone_format)-len(timezone_gap)]
        
    else:

        time_format=time_and_timezone_format        
    
    # separate main and fractional part
    x=time_format.split('.')
    main_time_format=x[0]
    if len(x)==2:
        fractional_time_format=x[1]
    else:
        fractional_time_format=None
        
    # check if main time format is in the approved list
    main_time_formats=[
        'HH:mm:ss',
        'HHmmss',
        'HH:mm',
        'HHmm'
        ]
    if not main_time_format in main_time_formats:
        raise Exception
        
    # check fractional time format
    if not fractional_time_format is None:
        for x in fractional_time_format:
            if not x=='S':
                raise Exception
            
    # reformat timezone in string_value for Python parsing
    if not timezone_format is None:
        
        timezone_string=get_timezone_string(string_value)
        
        reformatted_timezone_string=\
            reformat_timezone_in_string_value(
                timezone_string,
                timezone_format
                )
    
        string_value=\
            string_value.replace(
                timezone_string,
                reformatted_timezone_string)
    
    # reformat date_format for Python parsing
    x=time_and_timezone_format
    x=x.replace('HH','%H')
    x=x.replace('mm','%M')
    x=x.replace('S','%f')
    x=x.replace('S','')
    x=x.replace('ss','%S')
    
    if not timezone_format is None:
        x=x.replace(timezone_format,'%z')
    
    # parse time
    dt=datetime.datetime.strptime(string_value, x)  
    
    # split isoformat
    x=dt.isoformat()
    
    if not timezone_format is None:
        timezone_part=x[-6:]
        x=x[:-6]
    else:
        timezone_part=''
        
    x=x.split('T')[1].split('.')
    
    main_part=x[0]
    
    if len(x)==2:
        
        time_frac=float('0.'+x[1])
        
        st='{:0.%sf}' % len(fractional_time_format)
        
        fractional_part=st.format(time_frac)[1:]
        
    else:
        
        fractional_part=''
        
    # create return value
    json_value=main_part+fractional_part+timezone_gap+timezone_part
    
    
    return json_value, type_, errors


    
def parse_datetime(
        string_value,
        datatype_base,
        datatype_format,
        errors
        ):
    """
    """
    
    type_=datatype_base
    
    # By default, dates and times are assumed to be in the format defined 
    # in [xmlschema11-2]. However dates and times are commonly represented 
    # in tabular data in other formats.

    if datatype_format is None:
        
        datatype_format='yyyy-MM-ddTHH:mm:ss'  

    # If the datatype base is a date or time type, the datatype format 
    # annotation indicates the expected format for that date or time.
    
    # The supported date and time format patterns listed here are 
    # expressed in terms of the date field symbols defined in [UAX35]. 
    # These formats must be recognised by implementations and must be 
    # interpreted as defined in that specification. 
    # Implementations may additionally recognise other date format patterns. 
    # Implementations must issue a warning if the date format pattern is 
    # invalid or not recognised and proceed as if no date format 
    # pattern had been provided.
    
    datetime_format=datatype_format
    
    # identify separator
    if 'T' in datetime_format:
        separator='T'
    else:
        separator=' '
    
    # separate date and time formats
    date_format, time_format = datetime_format.split(separator)
        
    # separate date_string_value and time_string_value
    #print(string_value)
    date_string_value, time_string_value = string_value.split(separator)
    
    # get date json value
    date_json_value, date_type_, errors=\
        parse_date(
            string_value=date_string_value,
            datatype_base='date',
            datatype_format=date_format,
            errors=errors
            )
        
    # get time json value
    time_json_value, time_type_, errors=\
        parse_time(
            string_value=time_string_value,
            datatype_base='time',
            datatype_format=time_format,
            errors=errors
            )
    
    json_value=date_json_value+'T'+time_json_value
    
    return json_value, type_, errors
    

def parse_datetimestamp(
        string_value,
        datatype_base,
        datatype_format,
        errors
        ):
    """
    """
    return parse_datetime(
            string_value,
            datatype_base,
            datatype_format,
            errors
            )

    
#%% 6.4.6 Formats for other types

def parse_other_types(
        string_value,
        datatype_base,
        datatype_format,
        errors
        ):
    """
    """

    json_value=string_value
    
    type_=datatype_base

    return json_value,type_,errors

    

#%% 8 - Parsing Tabular Data

def parse_tabular_data_from_text(
        tabular_data_text,
        tabular_data_file_path_or_url,
        dialect_description_obj_dict
        ):
    """
    """
    
    comment_prefix=dialect_description_obj_dict.get('commentPrefix',None)
    delimiter=dialect_description_obj_dict.get('delimiter',',')
    escape_character=dialect_description_obj_dict.get('escapeCharacter','"')
    header=dialect_description_obj_dict.get('headerRowCount',True)
    header_row_count=dialect_description_obj_dict.get('headerRowCount',1 if header else 0)
    line_terminators=dialect_description_obj_dict.get('lineTerminators',['\r\n', '\n'])
    quote_character=dialect_description_obj_dict.get('quoteCharacter','"')
    skip_blank_rows=dialect_description_obj_dict.get('skipBlankRows',False)
    skip_columns=dialect_description_obj_dict.get('skipColumns',0)
    skip_rows=dialect_description_obj_dict.get('skipRows',0)
    trim=dialect_description_obj_dict.get('trim',True)
        # one of True, False, "true", "false", "start", "end"
        # skipInitialSpace is ignored as there is a contradiction here...
        # - if the trim property has a default of True, then this always overrides
        #   the skipInitialSpace property...
                
    #print('dialect_description_obj_dict',dialect_description_obj_dict)
    #print('header_row_count',header_row_count)
        
        
    # The algorithm for using these flags to parse a document containing 
    # tabular data to create a basic annotated tabular data model and to 
    # extract embedded metadata is as follows:
    
    # 1 Create a new table T with the annotations:
    # columns set to an empty list
    # rows set to an empty list
    # id set to null
    # url set to the location of the file, if known, or null
    # table direction set to auto
    # suppress output set to false
    # notes set to false
    # foreign keys set to an empty list
    # transformations set to an empty list    
    
    table_dict=dict(
        columns=[],
        rows=[],
        id=None,
        url=tabular_data_file_path_or_url,
        tableDirection='auto',
        suppressOutput=False,
        notes=[],  # not False as stated in the standard
        foreignKeys=[],
        transformations=[]       
        )
    
    # 2 Create a metadata document structure M that looks like:
    # {
    #   "@context": "http://www.w3.org/ns/csvw",
    #   "rdfs:comment": []
    #   "tableSchema": {
    #     "columns": []
    #   }
    # }

    metadata_table_dict={
        "@context": "http://www.w3.org/ns/csvw",
        "rdfs:comment": [],
        "tableSchema": {
            "columns": []
            }
      }
    

    # 3 If the URL of the tabular data file being parsed is known, set the 
    # url property on M to that URL.
    
    if not tabular_data_file_path_or_url is None:
        metadata_table_dict['url']=tabular_data_file_path_or_url

    # 4 Set source row number to 1.
    source_row_number=1
    
    # 5 Read the file using the encoding, as specified in [encoding], using 
    # the replacement error mode. If the encoding is not a Unicode encoding, 
    # use a normalizing transcoder to normalize into Unicode Normal Form C 
    # as defined in [UAX15].

    # ALREADY READ THE FILE
    character_index=0  # index for processing each character in the file

    # 6 Repeat the following the number of times indicated by skip rows:
    
    for _ in range(skip_rows): 
        
        #print(_)
        
        # 6.1 Read a row to provide the row content.
        character_index, row_content=\
            get_row_content(
                tabular_data_text,
                character_index,
                escape_character,
                quote_character,
                line_terminators
                )
            
        #print(character_index)
        #print(row_content)
        
        # 6.2 If the comment prefix is not null and the row content begins 
        # with the comment prefix, strip that prefix from the row content, 
        # and add the resulting string to the M.rdfs:comment array.
        if not comment_prefix is None \
            and row_content.startswith(comment_prefix):
                
            metadata_table_dict['rdfs:comment'].append(
                row_content[len(comment_prefix):]
                )
        
        # 6.3 Otherwise, if the row content is not an empty string, add the 
        # row content to the M.rdfs:comment array.
        elif not row_content=='':
            metadata_table_dict['rdfs:comment'].append(
                row_content
                )
    
        # 6.4 Add 1 to the source row number.
        source_row_number+=1
        
    # 7 Repeat the following the number of times indicated by header row count:
    for _ in range(header_row_count):
        
        # 7.1 Read a row to provide the row content.
        character_index, row_content=\
            get_row_content(
                tabular_data_text,
                character_index,
                escape_character,
                quote_character,
                line_terminators
                )
        
        # 7.2 If the comment prefix is not null and the row content begins 
        # with the comment prefix, strip that prefix from the row content, 
        # and add the resulting string to the M.rdfs:comment array.
        if not comment_prefix is None \
            and row_content.startswith(comment_prefix):
                
            metadata_table_dict['rdfs:comment'].append(
                row_content[len(comment_prefix):]
                )
                
        # 7.3 Otherwise, parse the row to provide a list of cell values, and:
        else:
            
            list_of_cell_values=\
                get_list_of_cell_values(
                    row_content,
                    escape_character,
                    quote_character,
                    delimiter,
                    trim
                    )
            
            # 7.3.1 Remove the first skip columns number of values from the 
            # list of cell values.
            list_of_cell_values_non_skipped=list_of_cell_values[skip_columns:]
            
            # 7.3.2 For each of the remaining values at index i in the list 
            # of cell values:
                
            # sets up the metatdata column description objects
            if len(metadata_table_dict['tableSchema']['columns'])==0:
                
                metadata_table_dict['tableSchema']['columns']=\
                    [{'titles':[],
                      '@type':'Column'} 
                     for x in range(len(list_of_cell_values_non_skipped))]
                
            for i, value in enumerate(list_of_cell_values_non_skipped):
                
                # 7.3.2.1 If the value at index i in the list of cell values 
                # is an empty string or consists only of whitespace, do nothing.
                if value.strip()=='':
                    continue
                
                # 7.3.2.2 Otherwise, if there is no column description object 
                # at index i in M.tableSchema.columns, create a new one with 
                # a title property whose value is an array containing a single 
                # value that is the value at index i in the list of cell values.
                # AND
                # 7.3.2.3 Otherwise, add the value at index i in the list of 
                # cell values to the array at M.tableSchema.columns[i].titles.
                metadata_table_dict['tableSchema']['columns'][i]['titles'].append(
                    value
                    )
                
            # 7.4 Add 1 to the source row number.
            source_row_number+=1
                
    # 8 If header row count is zero, create an empty column description object 
    # in M.tableSchema.columns for each column in the current row after skip 
    # columns.
    
    if header_row_count==0:
        
        original_character_index=character_index
        
        while True: # loops until a non-comment row is found
            character_index, row_content=\
                get_row_content(
                    tabular_data_text,
                    character_index,
                    escape_character,
                    quote_character,
                    line_terminators
                    )
            if comment_prefix is None \
                or not row_content.startswith(comment_prefix):
                    break
            
        list_of_cell_values=\
            get_list_of_cell_values(
                row_content,
                escape_character,
                quote_character,
                delimiter,
                trim
                )
        list_of_cell_values_non_skipped=list_of_cell_values[skip_columns:]
        
        metadata_table_dict['tableSchema']['columns']=\
            [{'@type':'Column'} 
             for x in range(len(list_of_cell_values_non_skipped))]
            
        character_index=original_character_index
            
                
    # 9 Set row number to 1.
    row_number=1
    
    # 10 While it is possible to read another row, do the following:
    
    while True:
        
        if character_index>len(tabular_data_text)-1:
            break
        
        # 10.1 Set the source column number to 1.
        source_column_number=1
        
        # 10.2 Read a row to provide the row content.
        character_index, row_content=\
            get_row_content(
                tabular_data_text,
                character_index,
                escape_character,
                quote_character,
                line_terminators
                )
        
        # 10.3 If the comment prefix is not null and the row content begins 
        # with the comment prefix, strip that prefix from the row content, 
        # and add the resulting string to the M.rdfs:comment array.
        if not comment_prefix is None \
            and row_content.startswith(comment_prefix):
                
            metadata_table_dict['rdfs:comment'].append(
                row_content[len(comment_prefix):]
                )
        
        else:
            
            # 10.4 Otherwise, parse the row to provide a list of cell values, and:
            list_of_cell_values=\
                get_list_of_cell_values(
                    row_content,
                    escape_character,
                    quote_character,
                    delimiter,
                    trim
                    )
            
            # 10.4.1 If all of the values in the list of cell values are empty 
            # strings, and skip blank rows is true, add 1 to the source row 
            # number and move on to process the next row.
            if all(x=='' for x in list_of_cell_values) and skip_blank_rows==True:
                pass
                
            # 10.4.2 Otherwise, create a new row R, with:
            # table set to T
            # number set to row number
            # source number set to source row number
            # primary key set to an empty list
            # referenced rows set to an empty list
            # cells set to an empty list
            else:
                row_dict=dict(
                    table=table_dict, 
                    number=row_number,
                    sourceNumber=source_row_number,
                    primaryKey=[],
                    referencedRows=[],
                    cells=[],
                    titles=[]
                    )
                
            # 10.4.3 Append R to the rows of table T.
            table_dict['rows'].append(row_dict)
            
            # 10.4.4 Remove the first skip columns number of values from the 
            # list of cell values and add that number to the source column number.
            list_of_cell_values_non_skipped=list_of_cell_values[skip_columns:]

            source_column_number+=skip_columns
            
            # 10.4.5 For each of the remaining values at index i in the list 
            # of cell values (where i starts at 1):
            for i, value in enumerate(list_of_cell_values_non_skipped):
                
                # 10.4.5.1 Identify the column C at index i within the columns 
                # of table T. If there is no such column:
                    
                try:
                    column_dict=table_dict['columns'][i]
                
                except IndexError:
                    
                    # 10.4.5.1.1 Create a new column C with:
                    # table set to T
                    # number set to i
                    # source number set to source column number
                    # name set to null
                    # titles set to an empty list
                    # virtual set to false
                    # suppress output set to false
                    # datatype set to string
                    # default set to an empty string
                    # lang set to und
                    # null set to an empty string
                    # ordered set to false
                    # required set to false
                    # separator set to null
                    # text direction set to auto
                    # about URL set to null
                    # property URL set to null
                    # value URL set to null
                    # cells set to an empty list
                
                    column_dict=dict(
                        table=table_dict, #table_name,
                        number=i+1,
                        sourceNumber=source_column_number,
                        name=None,
                        titles=[],
                        virtual=False,
                        suppressOutput=False,
                        datatype='string', 
                        default='',
                        lang='und',
                        null='',
                        ordered=False,
                        required=False,
                        separator=None,
                        textDirection='auto',
                        aboutURL=None,
                        propertyURL=None,
                        valueURL=None,
                        cells=[]
                        )
                
                    # 10.4.5.1.2 Append C to the columns of table T (at index i).
                    table_dict['columns'].append(column_dict)
                    
                # 10.4.5.2 Create a new cell D, with:
                # table set to T
                # column set to C
                # row set to R
                # string value set to the value at index i in the list of cell values
                # value set to the value at index i in the list of cell values
                # errors set to an empty list
                # text direction set to auto
                # ordered set to false
                # about URL set to null
                # property URL set to null
                # value URL set to null
                
                cell_dict=dict(
                    table=table_dict, #table_name,
                    column=column_dict, #f'{table_name}C{i+1}',
                    row=row_dict, #f'{table_name}R{row_number}',
                    stringValue=value,
                    value=value,
                    errors=[],
                    textDirection='auto',
                    ordered=False,
                    aboutURL=None,
                    propertyURL=None,
                    valueURL=None
                    )
                
                # 10.4.5.3 Append cell D to the cells of column C.
                column_dict['cells'].append(cell_dict)
                
                # 10.4.5.4 Append cell D to the cells of row R (at index i).
                row_dict['cells'].append(cell_dict)  
                    # NOTE THAT cell_dict NOW IS STORED IN BOTH column_dict 
                    # AND row_dict 
                
                # 10.4.5.5 Add 1 to the source column number.
                source_column_number+=1
                
        # 10.5 Add 1 to the source row number.
        source_row_number+=1
        row_number+=1  # I also added this to increment the row_number
        
    # 11 If M.rdfs:comment is an empty array, remove the rdfs:comment property from M.
    if len(metadata_table_dict['rdfs:comment'])==0:
        
        metadata_table_dict.pop('rdfs:comment')
    
    # 12 Return the table T and the embedded metadata M.
        
    normalized_metadata_dict=\
        normalize_metadata_root_object(
            metadata_dict,
            metadata_file_path='.', 
            metadata_file_url=None
            )
    

    return table_dict, normalized_metadata_dict
    
    

def get_row_content(
        tabular_data_text,
        i,
        escape_character,
        quote_character,
        line_terminators
        ):
    """
    """
    #print(tabular_data_text)
    #print(i)
    # print(escape_character)
    # print(quote_character)
    # print(line_terminators)
    
    # To read a row to provide row content, perform the following steps:
    
    # 1 Set the row content to an empty string.
    row_content=''
    
    # 2 Read initial characters and process as follows:
        
    while True:
    
        # 2.1 If the string starts with the escape character followed by the 
        # quote character, append both strings to the row content, and move on 
        # to process the string following the quote character.
        if tabular_data_text[i]==escape_character \
            and tabular_data_text[i+1]==quote_character:
            row_content+=escape_character+quote_character
            i+=2
        
        # 2.2 Otherwise, if the string starts with the escape character and the 
        # escape character is not the same as the quote character, append the 
        # escape character and the single character following it to the row 
        # content and move on to process the string following that character.
        elif tabular_data_text[i]==escape_character \
           and escape_character!=quote_character:
           row_content+=tabular_data_text[i:i+2]
           i+=2
        
        # 2.3 Otherwise, if the string starts with the quote character, append 
        # the quoted value obtained by reading a quoted value to the row content 
        # and move on to process the string following the quoted value.
        elif tabular_data_text[i]==quote_character:
            j, quoted_value=get_quoted_value(
                tabular_data_text[i:],
                escape_character,
                quote_character
                )
            row_content+=quoted_value
            i+=j
            
        # 2.4 Otherwise, if the string starts with one of the line terminators, 
        # return the row content.
        elif tabular_data_text[i] in line_terminators:
            i+=1
            break
            
        # 2.5 Otherwise, append the first character to the row content and move 
        # on to process the string following that character.
        else:
            row_content+=tabular_data_text[i]
            i+=1
    
        # 3 If there are no more characters to read, return the row content.
        if i>len(tabular_data_text)-1:
            break
    
    #print(i, row_content)
    
    return i, row_content
    
    
def get_quoted_value(
        characters,
        escape_character,
        quote_character,
        ):
    """
    """
    logging=False
    
    if logging: logging.info('    FUNCTION: get_quoted_value')
    if logging: logging.debug(f'        ARGUMENT: characters: {characters}')
    if logging: logging.debug(f'        ARGUMENT: escape_character: {escape_character}')
    if logging: logging.debug(f'        ARGUMENT: quote_character: {quote_character}')
    
    # To read a quoted value to provide a quoted value, perform the following steps:
        
    # 1 Set the quoted value to an empty string.
    quoted_value=''
    
    # 2 Read the initial quote character and add a quote character to the quoted value.
    initial_quote_character=characters[0]
    quoted_value+=initial_quote_character
    
    # 3 Read initial characters and process as follows:
    i=1
    
    while True:    
        
        current_character=characters[i]
        try:
            next_character=characters[i+1]
        except IndexError:
            next_character=None
    
        # 3.1 If the string starts with the escape character followed by the quote 
        # character, append both strings to the quoted value, and move on to 
        # process the string following the quote character.
        if (current_character==escape_character 
            and next_character==quote_character):
            quoted_value+=escape_character+quote_character
            i+=2
        
        # 3.2 Otherwise, if string starts with the escape character and the escape 
        # character is not the same as the quote character, append the escape 
        # character and the character following it to the quoted value and move 
        # on to process the string following that character.
        elif (current_character==escape_character 
              and escape_character!=quote_character):
            quoted_value+=escape_character+next_character
            i+=2
        
        # 3.3 Otherwise, if the string starts with the quote character, return 
        # the quoted value.
        elif current_character==quote_character:
            quoted_value+=quote_character
            i+=1
            break
        
        # 3.4 Otherwise, append the first character to the quoted value and move 
        # on to process the string following that character.
        else:
            quoted_value+=current_character
            i+=1
            
    if logging: logging.debug(f'        RETURN VALUE: i, quoted_value: {i}, {quoted_value}')
            
    return i, quoted_value

    
def get_list_of_cell_values(
        characters, ## the row_content
        escape_character,
        quote_character,
        delimiter,
        trim
        ):
    """
    """
    #logging.info('FUNCTION: get_list_of_cell_values')
    
    
    #print('get_list_of_cell_values')
    #print(characters)
    
    # To parse a row to provide a list of cell values, perform the following steps:
        
    # 1 Set the list of cell values to an empty list and the current cell 
    # value to an empty string.
    list_of_cell_values=[]
    current_cell_value=''
    
    # 2 Set the quoted flag to false.
    quoted_flag=False
    
    # 3 Read initial characters and process as follows:
    i=0
    while True:
    
        current_character=characters[i]
        try:
            next_character=characters[i+1]
        except IndexError:
            next_character=None
    
        # 3.1 If the string starts with the escape character followed by the 
        # quote character, append the quote character to the current cell 
        # value, and move on to process the string following the quote character.
        if (current_character==escape_character
            and next_character==quote_character):
            current_cell_value+=quote_character
            i+=2
        
        # 3.2 Otherwise, if the string starts with the escape character and 
        # the escape character is not the same as the quote character, append 
        # the character following the escape character to the current cell 
        # value and move on to process the string following that character.
        elif (current_character==escape_character 
              and escape_character!=quote_character):
            current_cell_value+=next_character
            i+=2
        
        # 3.3 Otherwise, if the string starts with the quote character then:
        elif current_character==quote_character:
            
            # 3.3.1 If quoted is false, set the quoted flag to true, and move on 
            # to process the remaining string. If the current cell value is not 
            # an empty string, raise an error.
            if quoted_flag==False:
                quoted_flag=True
                if not current_cell_value=='':
                    #logging.critical(f'characters: {characters}')
                    #print(f'characters: {characters}')
                    #logging.critical(f'i: {i}')
                    #print(f'i: {i}')
                    #logging.critical(f'current_cell_value: {current_cell_value}')
                    #print(f'current_cell_value: "{current_cell_value}"')
                    raise Exception('quote character encountered not at start of cell')
                i+=1
        
            # 3.3.2 Otherwise, set quoted to false, and move on to process the 
            # remaining string. If the remaining string does not start with the 
            # delimiter, raise an error.
            else:
                quoted_flag=False
                if not next_character is None:
                    if not next_character==delimiter:
                        #logging.critical(f'characters: {characters}')
                        #logging.critical(f'i: {i}')
                        #logging.critical(f'current_cell_value: {current_cell_value}')
                        #logging.critical(f'next_character: {next_character}')
                        raise Exception
                i+=1
        
        # 3.4 Otherwise, if the string starts with the delimiter, then:
        elif current_character==delimiter:
        
            # 3.4.1 If quoted is true, append the delimiter string to the current 
            # cell value and move on to process the remaining string.
            if quoted_flag==True:
                current_cell_value+=delimiter
                i+=1
            
            # 3.4.2 Otherwise, conditionally trim the current cell value, add the 
            # resulting trimmed cell value to the list of cell values and move on 
            # to process the following string.
            else:
                trimmed_cell_value=get_trimmed_cell_value(
                    current_cell_value,
                    trim
                    )
                #print('trimmed_cell_value',trimmed_cell_value)
                list_of_cell_values.append(trimmed_cell_value)
                current_cell_value=''
                i+=1
        
        # 3.5 Otherwise, append the first character to the current cell value 
        # and move on to process the remaining string.
        else:
            current_cell_value+=current_character
            i+=1
            
        # 4 If there are no more characters to read, conditionally trim the 
        # current cell value, add the resulting trimmed cell value to the list 
        # of cell values and return the list of cell values.
        if i>len(characters)-1:
            trimmed_cell_value=get_trimmed_cell_value(
                current_cell_value,
                trim
                )
            list_of_cell_values.append(trimmed_cell_value)
            break

    #print(list_of_cell_values)

    #logging.debug(f'    RETURN VALUE: list_of_cell_values: {list_of_cell_values}')

    return list_of_cell_values


def get_trimmed_cell_value(
        cell_value,
        trim
        ):
    """
    """
    
    
    
    # To conditionally trim a cell value to provide a trimmed cell value, 
    # perform the following steps:
        
    # 1 Set the trimmed cell value to the provided cell value.
    trimmed_cell_value=cell_value
    
    # 2 If trim is true or start then remove any leading whitespace from 
    # the start of the trimmed cell value and move on to the next step.
    if trim==True or trim=='true' or trim=='start':
        trimmed_cell_value=trimmed_cell_value.lstrip()

    # 3 If trim is true or end then remove any trailing whitespace from 
    # the end of the trimmed cell value and move on to the next step.
    if trim==True or trim=='true' or trim=='end':
        trimmed_cell_value=trimmed_cell_value.rstrip()

    # 4 Return the trimmed cell value.
    return trimmed_cell_value



            


        
#%% ---Metadata Vocabulary for Tabular Data---

#%% 4- Annotating Tables

# The metadata defined in this specification is used to provide 
# annotations on an annotated table or group of tables, as defined 
# in [tabular-data-model]. 
# Annotated tables form the basis for all further processing, such 
# as validating, converting, or displaying the tables.

# All compliant applications must create annotated tables based on the 
# algorithm defined here. 
# All compliant applications must generate errors and stop 
# processing if a metadata document:
# - does not use valid JSON syntax defined by [RFC7159].
# - uses any JSON outside of the restrictions defined in section A. JSON-LD Dialect.
# - does not specify a property that it is required to specify.

# Compliant applications must ignore properties (aside from common 
# properties) which are not defined in this specification and must 
# generate a warning when they are encoutered.
    
# If a property has a value that is not permitted by this specification, 
# then if a default value is provided for that property, compliant 
# applications must generate a warning and use that default value. 
# If no default value is provided for that property, compliant 
# applications must generate a warning and behave as if the property 
# had not been specified. Additionally, including:
# - properties (aside from common properties) which are not defined in 
#   this specification, and
# - properties having invalid values for a given property.

    

#%% Section 5 - Metadata Format

#%% 5.1 - Property Syntax

#%% 5.1.1 - Array Properties

def validate_array_property(
        metadata_obj_dict,
        property_name,
        property_value,
        expected_types
        ):
    """
    """
    # Array properties hold an array of one or more objects, which are 
    # usually description objects.

    # For example, the tables property is an array property. 
    # A table group description might contain:
    
    # "tables": [{
    #   "url": "https://example.org/countries.csv",
    #   "tableSchema": "https://example.org/countries.json"
    # }, {
    #   "url": "https://example.org/country_slice.csv",
    #   "tableSchema": "https://example.org/country_slice.json"
    # }]
              
    # in which case the tables property has a value that is an array of 
    # two table description objects.
    
    # Any items within an array that are not valid objects of the type 
    # expected are ignored. 
    # If the supplied value of an array property is not an array 
    # (e.g. if it is an integer), compliant applications must issue a 
    # warning and proceed as if the property had been supplied with an 
    # empty array.
        
    if isinstance(property_value,list):
        
        if not expected_types is None:
        
            # remove invalid objects
            for item in property_value[::-1]:
                
                if not type(item) in expected_types:
                    
                    property_value.remove(item)
    
    else:
        
        # replace non-array with empty array
        property_value_type=type(property_value).__name__
        
        message=f'Property "{property_name}" with value "{property_value}" ({property_value_type}) is not valid.'
        message+=' Array expected.'
        message+=' Value replaced with an empty array.'
        
        warnings.warn(message)
        
        property_value=[]
        
        metadata_obj_dict[property_name]=property_value
    
    
#%% 5.1.2 - Link Properties
    
def validate_link_property(
        metadata_obj_dict,
        property_name,
        property_value
        ):
    """
    """
    # Link properties hold a single reference to another resource by URL. 
    # Their value is a string — resolved as a URL against the base URL. 
    # If the supplied value of a link property is not a string (e.g. if 
    # it is an integer), compliant applications must issue a warning and 
    # proceed as if the property had been supplied with an empty string.

    # For example, the url property is a link property. 
    # A table description might contain:
    
    # EXAMPLE 5
    # "url": "example-2014-01-03.csv"
    # in which case the url property on the table would have a single value, 
    # a link to example-2014-01-03.csv, resolved against the base URL of 
    # the metadata document in which this was located. 
    
    # For example if the metadata document contained:
    
    # EXAMPLE 6
    # "@context": [ "http://www.w3.org/ns/csvw", { "@base": "http://example.org/" }]
    
    # this is equivalent to specifying:
    
    # EXAMPLE 7
    # "url": "http://example.org/example-2014-01-03.csv"
    
    if not isinstance(property_value,str):
        
        property_value_type=type(property_value).__name__
        
        message=f'Property "{property_name}" with value "{property_value} "' 
        message+=f'({property_value_type}) is not valid.'
        message+=' String expected.'
        message+=' Value replaced with an empty string.'
        
        warnings.warn(message)
        
        property_value=''
        
        metadata_obj_dict[property_name]=property_value
        
        
    
    
#%% 5.1.5 - Object Properties

def validate_object_property(
        metadata_obj_dict,
        property_name,
        property_value
        ):
    """
    """
    # Object properties hold either a single object or a reference to an 
    # object by URL. Their values may be:
    # - strings — resolved as URLs against the base URL.
    # - objects — interpreted as structured objects.
    
    # If the supplied value of an object property is not a string or 
    # object (e.g. if it is an integer), compliant applications must issue 
    # a warning and proceed as if the property had been specified as an 
    # object with no properties.
    
    if not isinstance(property_value,str) or isinstance(property_value,dict):
        
        message=f'Property "{property_name}" with value "{property_value}" is not valid. '
        message+='Value changed to {}'
        
        property_value={}
        metadata_obj_dict[property_name]=property_value
        
        warnings.warn(message)
        
    # Object properties are often used when the values can be or should 
    # be values within controlled vocabularies, or structured information 
    # which may be held elsewhere. 
    # For example, the dialect of a table is an object property. 
    # It could be provided as a URL that indicates a commonly used 
    # dialect, like this:
    
    # EXAMPLE 17
    # "dialect": "http://example.org/tab-separated-values"
    
    # or a structured object, like this:
    
    # EXAMPLE 18
    # "dialect": {
    #   "delimiter": "\t",
    #   "encoding": "utf-8"
    # }
    
    # When specified as a string, the resolved URL is used to fetch the 
    # referenced object during normalization as described in section 6. 
    # Normalization. For example, if http://example.org/tab-separated-values 
    # resolved to:
    
    # EXAMPLE 19
    # {
    #   "@context": "http://www.w3.org/ns/csvw",
    #   "quoteChar": null,
    #   "header": true,
    #   "delimiter": "\t"
    # }
    
    # Following normalization, the value of the dialect property would then be:
    
    # EXAMPLE 20
    # "dialect": {
    #   "@id": "http://example.org/tab-separated-values",
    #   "quoteChar": null,
    #   "header": true,
    #   "delimiter": "\t"
    # }
    
    
    
#%% 5.1.7 - Atomic Properties

def validate_atomic_property(
        metadata_obj_dict,
        property_name,
        property_value,
        expected_types=None,
        expected_values=None,
        default_value=None,
        required_values=None
        ):
    """
    """
    # Atomic properties hold atomic values. Their values may be:

    # numbers — interpreted as integers or doubles.
    # booleans — interpreted as booleans (true or false).
    # strings — interpreted as defined by the property.
    # objects — interpreted as defined by the property.
    # arrays — lists of numbers, booleans, strings, or objects.
    
    # The annotation value of a boolean atomic property is false if unset; 
    # otherwise, the annotation value of an atomic property is normalized 
    # value of that property, or the defined default value or null, if unset. 
    # Processors must issue a warning if a property is set to an invalid 
    # value type, such as a boolean atomic property being set to the 
    # number 1 or a numeric atomic property being set to the string "3.1415", 
    # and act as if the property had not been specified (which may mean 
    # using the default value for the property, or may mean raising an 
    # error and halting processing if the property is a required property).

    if not expected_types is None:

        if not type(property_value) in expected_types:
        
            property_value_type=type(property_value).__name__
            
            message=f'Property "{property_name}" with value '
            message+=f'"{property_value}" ({property_value_type}) is not valid.'
            message+=f' One of these types expected: "{expected_types}". '
            message+=f' Value replaced with "{default_value}". '
            
            warnings.warn(message)
            
            property_value=default_value
            
        
    if not expected_values is None:
        
        if not property_value in expected_values:
            
            message=f'Property "{property_name}" with value '
            message+=f'"{property_value}" ({property_value_type}) is not valid.'
            message+=f' One of these values expected: "{expected_values}". '
            message+=f' Value replaced with "{default_value}". '
            
            warnings.warn(message)
    
            property_value=default_value
            
            
    if not required_values is None:
        
        if not property_value in required_values:
            
            message=f'Property "{property_name}" with value '
            message+=f'"{property_value}" ({property_value_type}) is not valid.'
            message+=f' One of these values is required: "{expected_values}". '
            message+=f' Value replaced with "{default_value}". '
                
            raise ValueError(message)
            
    
    metadata_obj_dict[property_name]=property_value
        
        
    

#%% 5.2 - Top-Level Properties

def validate_top_level_properties(
        metadata_obj_dict,
        metadata_document_location
        ):
    """
    """
    # The top-level object of a metadata document or object referenced 
    # through an object property (whether it is a table group description, 
    # table description, schema, dialect description or transformation 
    # definition) must have a @context property. 
    
    try: 
        
        context=metadata_obj_dict['@context']
    
    except KeyError:
        
        message='Property "@context" is a required property.'
        
        raise KeyError(message)
    
    # This is an array property, as defined in Section 8.7 of [JSON-LD]. 
    
    # The @context must have one of the following values:

    # A string value of http://www.w3.org/ns/csvw, or
    if context=='http://www.w3.org/ns/csvw':
        
        base_url=metadata_document_location
        default_language='und'
    
    # An array composed of a string followed by an object, where the 
    # string is http://www.w3.org/ns/csvw and the object represents a 
    # local context definition, which is restricted to contain either or 
    # both of the following members:
    elif (isinstance(context,list) 
          and len(context)==2
          and context[0]=='http://www.w3.org/ns/csvw'
          and isinstance(context[1],dict)
          and len(context[1])>0
          ):
        
        # @base
        # an atomic property that provides the base URL against which 
        # other URLs within the metadata file are resolved. 
        # If present, its value must be a string that is interpreted 
        # as a URL which is resolved against the location of the 
        # metadata document to provide the base URL for other URLs 
        # in the metadata document; 
        # if unspecified, the base URL used for interpreting relative 
        # URLs within the metadata document is the location of the 
        # metadata document itself.
        
        # NOTE
        # Note that the @base property of the @context object provides the 
        # base URL used for URLs within the metadata document, not the URLs 
        # that appear as data within the group of tables or table it describes. 
        # URI template properties are not resolved against this base URL: 
        # they are resolved against the URL of the table.
        
        if '@base' in context[1]:
            
            context_base_string=context[1]['@base']
            
            base_url=urllib.parse.urljoin(
                metadata_document_location,
                context_base_string
                )
        else:
            
            base_url=metadata_document_location
            
            
        # @language
        # an atomic property that indicates the default language for the 
        # values of natural language or string-valued common properties in 
        # the metadata document; if present, its value must be a language 
        # code [BCP47]. 
        # The default is und.
        
        # NOTE
        # Note that the @language property of the @context object, which 
        # gives the default language used within the metadata file, is 
        # distinct from the lang property on a description object, which 
        # gives the language used in the data within a group of tables, 
        # table, or column.
            
        if '@language' in context[1]:
            
            default_language=context[1]['@language']
            
            if not langcodes.tag_is_valid(default_language):
        
                message='Property "@language" is not a valid language code.'        
        
                raise ValueError(message)
        
        else:
            
            default_language='und'
        
        
        # check only @base and @language are present
        for k in context[1]:
            
            if not k in ['@base','@language']:
                
                message='@context object can only contain properties "@base" and "@language". '
                message+=f'Property "{k}" is not valid.'
                
                raise ValueError(message)
        
    
    return base_url, default_language
    

#%% 5.3 - Table Groups

def validate_and_normalize_metadata_table_group_dict(
        metadata_table_group_dict,
        metadata_document_location
        ):
    """
    """
    # A table group description is a JSON object that describes a group of tables.
    
    # top-level properties
    base_url, default_language=\
        validate_top_level_properties(
            metadata_table_group_dict,
            metadata_document_location
            )
    
    
    # required properties
    if not 'tables' in metadata_table_group_dict:
        
        message='Property "tables" is a required property.'
        
        raise KeyError(message)
    
    
    # loop through items
    for k, v in metadata_table_group_dict.items():
        
        # @context
        if k=='@context':
            
            pass  # already done above
        
        # tables
        elif k=='tables':
            
            # An array property of table descriptions for the tables in 
            # the group, namely those listed in the tables annotation on 
            # the group of tables being described. 
            
            validate_array_property(
                metadata_table_group_dict,
                k,
                v,
                [dict]
                )
            
            # Compliant application must raise an error if this array does not contain one or more table descriptions.
            
            if len(v)<1:
                
                message='Property "tables" must contain one or more table descriptions.'
                
                raise ValueError(message)
                
            # loop through tables
            for metadata_table_dict in v:
                
                validate_and_normalize_metadata_table_dict(
                        metadata_table_dict,
                        base_url,
                        default_language,
                        )
                
        # dialect    
        elif k=='dialect':
            
            # An object property that provides a single dialect description. 
            # If provided, dialect provides hints to processors about how to 
            # parse the referenced files to create tabular data models for
            # the tables in the group. 
            # This may be provided as an embedded object or as a URL reference. 
            # See section 5.9 Dialect Descriptions for more details.
            
            validate_object_property(
                metadata_table_group_dict,
                k,
                v,
                )
            
            referenced_url=\
                normalize_object_property(
                    metadata_table_group_dict,
                    k,
                    v,
                    base_url
                    )
            
            validate_and_normalize_metadata_dialect_dict(
                    v,
                    referenced_url,
                    base_url,
                    default_language
                    )
             
            
        # notes
        elif k=='notes':
            
            # An array property that provides an array of objects representing 
            # arbitrary annotations on the annotated group of tables. 
            # The value of this property becomes the value of the notes 
            # annotation for the group of tables. 
            # The properties on these objects are interpreted equivalently 
            # to common properties as described in section 5.8 Common Properties.

            # NOTE
            # The Web Annotation Working Group is developing a vocabulary 
            # for expressing annotations. In future versions of this 
            # specification, we anticipate referencing that vocabulary.
            
            validate_array_property(
                metadata_table_group_dict,
                k,
                v,
                None
                )
            
            normalize_common_property_or_notes(
                metadata_table_group_dict,
                k,
                v
                )
            
        # tableDirection
        elif k=='tableDirection':
            
            # An atomic property that must have a single string value that 
            # is one of "rtl", "ltr", or "auto". 
            # Indicates whether the tables in the group should be displayed 
            # with the first column on the right, on the left, or based on 
            # the first character in the table that has a specific direction. 
            # The value of this property becomes the value of the table 
            # direction annotation for all the tables in the table group. 
            # See Bidirectional Tables in [tabular-data-model] for details. 
            # The default value for this property is "auto".
        
            validate_atomic_property(
                metadata_table_group_dict,
                k,
                v,
                expected_types=[str],
                expected_values=['rtl','ltr','auto'],
                default_value='auto',
                )
            
            normalize_atomic_property(
                metadata_table_group_dict,
                k,
                v
                )
            
        # tableSchema
        elif k=='tableSchema':
            
            # An object property that provides a single schema description as 
            # described in section 5.5 Schemas, used as the default for all 
            # the tables in the group. 
            # This may be provided as an embedded object within the JSON 
            # metadata or as a URL reference to a separate JSON object that is 
            # a schema description.
            
            validate_object_property(
                metadata_table_group_dict,
                k,
                v,
                )
            
            referenced_url=\
                normalize_object_property(
                    metadata_table_group_dict,
                    k,
                    v,
                    base_url
                    )
            
            validate_and_normalize_metadata_schema_dict(
                v,
                referenced_url,
                base_url,
                default_language
                )
            
        
        # transformations
        elif k=='transformations':
        
            # An array property of transformation definitions that provide 
            # mechanisms to transform the tabular data into other formats. 
            # The value of this property becomes the value of the 
            # transformations annotation for all the tables in the table 
            # group.        
        
            validate_array_property(
                metadata_table_group_dict,
                k,
                v,
                [dict]
                )
            
            # loop through transormations
            for metadata_transformation_dict in v:
                
                validate_and_normalize_metadata_transformation_dict(
                        metadata_transformation_dict,
                        base_url,
                        default_language,
                        )
            
        # @id
        elif k=='@id':
    
            # If included, @id is a link property that identifies the 
            # group of tables, as defined by [tabular-data-model], 
            # described by this table group description. 
            # It must not start with _:. 
            # The value of this property becomes the value of the id 
            # annotation for the group of tables.
            
            validate_link_property(
                metadata_table_group_dict,
                k,
                v,
                )
            
            normalize_link_property(
                metadata_table_group_dict,
                k,
                v,
                base_url
                )
                
            if v.startswith('_:'):
                
                message='Property "@id" must not start with "_:". '
                
                raise ValueError(message)
                
            
        # @type
        elif k=='@type':
            
            # If included, @type is an atomic property that must be set 
            # to "TableGroup". 
            # Publishers may include this to provide additional information 
            # to JSON-LD based toolchains.
            
            validate_atomic_property(
                metadata_table_group_dict,
                k,
                v,
                required_values=['TableGroup']
                )
            
            normalize_atomic_property(
                metadata_table_group_dict,
                k,
                v
                )
            
        # inherited properties
        elif k in ['aboutUrl','datatype','default','lang','null','ordered',
                   'propertyUrl','required','separator','textDirection',
                   'valueUrl']:
            
            validate_and_normalize_inherited_property(
                metadata_table_group_dict,
                k,
                v,
                base_url,
                default_language
                )
            
        # common properties
        else:
            
            validate_and_normalize_common_property(
                metadata_table_group_dict,
                k,
                v,
                base_url,
                default_language
                )
            
    return base_url, default_language
        
        
#%% 5.4 - Tables

def validate_and_normalize_metadata_table_dict(
        metadata_table_dict,
        base_url,
        default_language,
        ):
    """
    """
    # A table description is a JSON object that describes a table within a CSV file.    
    

    # required properties
    if not 'url' in metadata_table_dict:
        
        message='Property "url" is a required property.'
        
        raise KeyError(message)
    
    
    # loop through items
    for k, v in metadata_table_dict.items():
        
        # url
        if k=='url':
        
            # This link property gives the single URL of the CSV file 
            # that the table is held in, relative to the location of the 
            # metadata document. 
            # The value of this property is the value of the url 
            # annotation for the annotated table this table description describes.
        
            validate_link_property(
                metadata_table_dict,
                k,
                v,
                )
            
            normalize_link_property(
                metadata_table_dict,
                k,
                v,
                base_url
                )
        
        # dialect
        elif k=='dialect':
            
            # As defined for table groups.
            
            validate_object_property(
                metadata_table_dict,
                k,
                v,
                )
            
            referenced_url=\
                normalize_object_property(
                    metadata_table_dict,
                    k,
                    v,
                    base_url
                    )
            
            validate_and_normalize_metadata_dialect_dict(
                v,
                referenced_url,
                base_url,
                default_language
                )
                
                
        # notes
        elif k=='notes':
            
            # An array property that provides an array of objects 
            # representing arbitrary annotations on the annotated tabular 
            # data model. 
            # The value of this property becomes the value of the notes 
            # annotation for the table. 
            # The properties on these objects are interpreted equivalently 
            # to common properties as described in section 5.8 Common Properties.

            # NOTE
            # The Web Annotation Working Group is developing a vocabulary 
            # for expressing annotations. 
            # In future versions of this specification, 
            # we anticipate referencing that vocabulary.
                    
            validate_array_property(
                metadata_table_dict,
                k,
                v,
                None
                )
            
            normalize_common_property_or_notes(
                metadata_table_dict,
                k,
                v
                )
        
        
        # suppressOutput
        elif k=='suppressOutput':
            
            # A boolean atomic property. 
            # If true, suppresses any output that would be generated when 
            # converting this table. 
            # The value of this property becomes the value of the suppress 
            # output annotation for this table. 
            # The default is false.
            
            validate_atomic_property(
                metadata_table_dict,
                k,
                v,
                expected_types=[bool],
                default_value=False,
                )
            
            normalize_atomic_property(
                metadata_table_dict,
                k,
                v
                )
        
        # tableDirection
        elif k=='tableDirection':
            
            # As defined for table groups. 
            # The value of this property becomes the value of the table 
            # direction annotation for this table.
        
            validate_atomic_property(
                metadata_table_dict,
                k,
                v,
                expected_types=[str],
                expected_values=['rtl','ltr','auto'],
                default_value='auto',
                )
            
            normalize_atomic_property(
                metadata_table_dict,
                k,
                v
                )
        
        # tableSchema
        elif k=='tableSchema':
            
            # An object property that provides a single schema description 
            # as described in section 5.5 Schemas. 
            # This may be provided as an embedded object within the JSON 
            # metadata or as a URL reference to a separate JSON schema document. 
            # If a table description is within a table group description, 
            # the tableSchema from that table group acts as the default for 
            # this property.

            # If a tableSchema is not declared in table description, 
            # it may be declared on the table group description, which is 
            # then used as the schema for this table description.
            
            # The @id property of the tableSchema, if there is one, becomes 
            # the value of the schema annotation for this table.
            
            # NOTE
            # When a schema is referenced by URL, this URL becomes the 
            # value of the @id property in the normalized schema description, 
            # and thus the value of the schema annotation on the table.
        
            validate_object_property(
                metadata_table_dict,
                k,
                v,
                )
            
            referenced_url=\
                normalize_object_property(
                    metadata_table_dict,
                    k,
                    v,
                    base_url
                    )
            
            validate_and_normalize_metadata_schema_dict(
                v,
                referenced_url,
                base_url,
                default_language
                )
        
        
        # transformations
        elif k=='transformations':
            
            # As defined for table groups. 
            # The value of this property becomes the value of the 
            # transformations annotation for this table.
            
            validate_array_property(
                metadata_table_dict,
                k,
                v,
                [dict]
                )
            
            # loop through transormations
            for metadata_transformation_dict in v:
                
                validate_and_normalize_metadata_transformation_dict(
                        metadata_transformation_dict,
                        base_url,
                        default_language,
                        )
            
        
        # @id
        elif k=='@id':
    
            # If included, @id is a link property that identifies the table, 
            # as defined in [tabular-data-model], described by this table 
            # description. 
            # It must not start with _:. 
            # The value of this property becomes the value of the id 
            # annotation for this table.
            
            validate_link_property(
                metadata_table_dict,
                k,
                v,
                )
            
            normalize_link_property(
                metadata_table_dict,
                k,
                v,
                base_url
                )
                
            if v.startswith('_:'):
                
                message='Property "@id" must not start with "_:". '
                
                raise ValueError(message)
        
        # @type
        elif k=='@type':
            
            # If included, @type is an atomic property that must be set 
            # to "Table". 
            # Publishers may include this to provide additional information 
            # to JSON-LD based toolchains.
            
            validate_atomic_property(
                metadata_table_dict,
                k,
                v,
                required_values=['Table']
                )
            
            normalize_atomic_property(
                metadata_table_dict,
                k,
                v
                )
        
        
        # inherited properties
        elif k in ['aboutUrl','datatype','default','lang','null','ordered',
                   'propertyUrl','required','separator','textDirection',
                   'valueUrl']:
            
            validate_and_normalize_inherited_property(
                metadata_table_dict,
                k,
                v,
                base_url,
                default_language
                )
            
            
        # common properties
        else:
            
            validate_and_normalize_common_property(
                metadata_table_dict,
                k,
                v,
                base_url,
                default_language
                )


#%% 5.4.3 - Table Description Compatibility

def compare_table_descriptions(
        TM,  # table_dict
        EM,  # embedded dict
        validate=False
        ):
    """
    """
    # Two table descriptions are compatible if they have equivalent 
    # normalized url properties, and have compatible schemas as defined 
    # in section 5.5.1 Schema Compatibility.
    
    if not TM['url']==EM['url']:
        raise Exception  # NEEDS FUTHER WORK
        
    compare_schema_descriptions(
        TM['tableSchema'],
        EM['tableSchema']
        )
        

        
            
#%% 5.5 - Schemas

def validate_and_normalize_metadata_schema_dict(
        metadata_schema_dict,
        referenced_url,
        base_url,
        default_language
        ):
    """
    """
    # A schema is a definition of a tabular format that may be common to 
    # multiple tables. 
    # For example, multiple tables from different sources may have the 
    # same columns and be designed such that they can be aggregated together.

    # A schema description is a JSON object that encodes the information 
    # about a schema, which describes the structure of a table. 
    # All the properties of a schema description are optional.

    if not referenced_url is None:
        
        # top-level properties
        base_url, default_language=\
            validate_top_level_properties(
                metadata_schema_dict,
                metadata_document_location=referenced_url
                )
            
    for k,v in metadata_schema_dict.items():
        
        if k=='@context':
            
            pass
        
        # columns
        elif k=='@id':
            
            raise NotImplementedError
            
        # foreignKeys
        elif k=='@id':
            
            raise NotImplementedError
            
        # primaryKey
        elif k=='@id':
            
            raise NotImplementedError
            
        # rowTitles
        elif k=='@id':
            
            raise NotImplementedError
            
        # @id
        elif k=='@id':
        
            # If included, @id is a link property that identifies the schema, 
            # as defined in [tabular-data-model], described by this schema 
            # description. 
            # It must not start with _:. 
            
            validate_link_property(
                metadata_schema_dict,
                k,
                v,
                )
            
            normalize_link_property(
                metadata_schema_dict,
                k,
                v,
                base_url
                )
                
            if v.startswith('_:'):
                
                message='Property "@id" must not start with "_:". '
                
                raise ValueError(message)
        
        # @type
        elif k=='@type':
            
            # If included, @type is an atomic property that must be set 
            # to "Schema". 
            # Publishers may include this to provide additional information 
            # to JSON-LD based toolchains.
            
            validate_atomic_property(
                metadata_schema_dict,
                k,
                v,
                required_values=['Table']
                )
            
            normalize_atomic_property(
                metadata_schema_dict,
                k,
                v
                )
        
        # inherited properties
        elif k in ['aboutUrl','datatype','default','lang','null','ordered',
                   'propertyUrl','required','separator','textDirection',
                   'valueUrl']:
            
            validate_and_normalize_inherited_property(
                metadata_schema_dict,
                k,
                v,
                base_url,
                default_language
                )
            
            
        # common properties
        else:
            
            validate_and_normalize_common_property(
                metadata_schema_dict,
                k,
                v,
                base_url,
                default_language
                )
        
    
    
    
    if not referenced_url is None:
        
        metadata_schema_dict.remove('@context')
        
        if not '@id' in metadata_schema_dict:
            
            metadata_schema_dict['@id']=referenced_url
   
#%% 5.5.1 Schema Compatibility

def compare_schema_descriptions(
        TM_schema,
        EM_schema,
        validate=False
        ):
    """Section 5.5.1.
    """
    #print(TM_schema)
    #print(EM_schema)
    
    # Two schemas are compatible if they have the same number of non-virtual 
    # column descriptions, and the non-virtual column descriptions at the 
    # same index within each are compatible with each other. 
    TM_non_virtual_columns=[x for x in TM_schema.get('columns',[]) 
                            if x.get('virtual',False)==False]
    EM_non_virtual_columns=[x for x in TM_schema.get('columns',[]) 
                            if x.get('virtual',False)==False]
    
    if not len(TM_non_virtual_columns)==len(EM_non_virtual_columns):
        raise Exception  # TO DO
        
    for i in range(len(TM_non_virtual_columns)):
        
        TM_column=TM_non_virtual_columns[i]
        EM_column=EM_non_virtual_columns[i]
    
        compare_column_descriptions(
            TM_column,
            EM_column,
            validate=validate
            )
        
        
def compare_column_descriptions(
        TM_column,
        EM_column,
        validate
        ):
    """
    """
    # Column descriptions are compatible under the following conditions:

    # If either column description has neither name nor titles properties.
    
    if not 'name' in TM_column and not 'titles' in TM_column:
        return
    
    if not 'name' in EM_column and not 'titles' in EM_column:
        return
    
    
    # If there is a case-sensitive match between the name properties of the columns.
    if 'name' in TM_column and 'name' in EM_column:
        if TM_column['name']==EM_column['name']:
            return
    
    # If there is a non-empty case-sensitive intersection between the 
    # titles values, where matches must have a matching language; und 
    # matches any language, and languages match if they are equal when 
    # truncated, as defined in [BCP47], to the length of the shortest language tag.
    
    
    intersection=False
    
    for TM_lang_tag,TM_titles in TM_column.get('titles',{}).items():
        
        for EM_lang_tag,EM_titles in EM_column.get('titles',{}).items():
            
            if TM_lang_tag=='und' or EM_lang_tag=='und' \
                or langcodes.standardize_tag(TM_lang_tag)== \
                    langcodes.standardize_tag(EM_lang_tag):
                        
                for title in TM_titles:
                    if title in EM_titles:
                        intersection=True
                        break
          
            if intersection:
                break
          
        if intersection:
            break
          
    if intersection:
        return
          
         
    
    # intersection=False
    # for TM_lang_tag,TM_titles in TM_column.get('titles',{}).items():
        
    #     if TM_lang_tag=='und':
            
    #         for EM_titles in EM_column.get('titles',{}).values():
            
    #             for title in TM_titles:
    #                 if title in EM_titles:
    #                     intersection=True
    #                     break
            
    #     else:
    #         raise NotImplementedError # TO DO
            
    
    
    # If not validating, and one schema has a name property but not a 
    # titles property, and the other has a titles property but not a name property.

    if ('name' in TM_column 
        and not 'titles' in TM_column
        and not 'name' in EM_column 
        and 'titles' in EM_column
        ):
        if validate==False:
            return
    
    if (not 'name' in TM_column 
        and 'titles' in TM_column
        and 'name' in EM_column 
        and not 'titles' in EM_column
        ):
        if validate==False:
            return

    raise Exception  # i.e. NOT COMPATIBLE - NEED TO FIX IF VALIDATING OR NOT

    
            
#%% 5.7 - Inherited Properties

def validate_and_normalize_inherited_property(
        metadata_obj_dict,
        property_name,
        property_value,
        base_url,
        default_language
        ):
    """
    """
    raise NotImplementedError


#%% 5.8 - Common Properties            
            
def validate_and_normalize_common_property(
        metadata_obj_dict,
        property_name,
        property_value,
        base_url,
        default_language
        ):
    """
    """
    
    # Descriptions of groups of tables, tables, schemas and columns may 
    # contain any common properties whose names are either absolute URLs 
    # or prefixed names. 
    # For example, a table description may contain dc:description, 
    # dcat:keyword, or schema:copyrightHolder properties to provide a 
    # description, keywords, or the name of the copyright holder, as 
    # defined in Dublin Core Terms, DCAT, or schema.org.
    
    validate_common_property_name(
        property_name
        )
    
    validate_common_property_value(
        property_name,
        property_value
        )
    
    normalize_common_property_or_notes(
        metadata_obj_dict,
        property_name,
        property_value,
        base_url,
        default_language
        )
    
    
#%% 5.8.1 - Names of Common Properties
    
def validate_common_property_name(
        property_name
        ):
    """
    """
    
    
    # The names of common properties are prefixed names, in the 
    # syntax prefix:name.
    
    # Prefixed names can be expanded to provide a URI, by replacing 
    # the prefix and following colon with the URI that the prefix 
    # is associated with. 
    # Expansion is intended to be entirely consistent with Section 6.3 
    # IRI Expansion in [JSON-LD-API] and implementations may use a 
    # JSON-LD processor for performing prefixed name and IRI expansion.
    
    # The prefixes that are recognized are those defined for [rdfa-core] 
    # within the RDFa 1.1 Initial Context and other prefixes defined 
    # within [csvw-context] and these must not be overridden. 
    # These prefixes are periodically extended; refer to [csvw-context] 
    # for details. 
    # Properties from other vocabularies must be named using absolute URLs.
    
    # NOTE
    # Forbidding the declaration of new prefixes ensures consistent 
    # processing between JSON-LD-aware and non-JSON-LD-aware processors.
    
    # This specification does not define how common properties are 
    # interpreted by implementations. 
    # Implementations should treat the prefixed names for common 
    # properties and the URLs that they expand into in the same way. 
    # For example, if an implementation recognises and displays the 
    # value of the dc:description property, it should also recognise 
    # and display the value of the http://purl.org/dc/terms/description 
    # property in the same way.
    
    # test prefixed names
    if ':' in property_name:
        x=property_name.split(':')
    else:
        x=property_name.split('%3A')  # this is the percent encoded version
        
    if len(x)==2:
        
        if not x[0] in prefixes:
            
            message=f'Property "{property_name}" is a common property but '
            message+='has a name with a prefix which is not valid. '
            message+='Only certain prefixes as defined in the CSVW standard are recognized.'
            
            raise ValueError(message)
            
    # test absolute URLs 
    url = hyperlink.parse(property_name)
    
    if not url.absolute:
        
        message=f'Property "{property_name}" is a common property but '
        message+='has name which is not a prefixed name and is not an absolute URL. '
        
        raise ValueError(message)
    

#%% 5.8.2 - Values of Common Properties
    
def validate_common_property_value(
        property_name,
        property_value
        ):
    
    def validate_type_property_value(
        value
        ):
        ""
        # If a @type property is used on an object without a @value property, 
        # its value must be one of:
        
        # - one of the legitimate values for @type as defined for any of the 
        #   description objects in this specification.
        # - a prefixed name using any of the pre-defined prefixes as described 
        #   in section 5.8.1 Names of Common Properties.
        # - a string, which must be interpreted as an absolute URL.
        
        if isinstance(value,str):
        
            # legitimate value 
            if value in ['TableGroup','Table','Schema','Column',
                         'Dialect','Template','Datatype']:
                
                return
            
            # prefixed name
            if ':' in value:
                x=value.split(':')
            else:
                x=value.split('%3A')  # this is the percent encoded version
                
            if len(x)==2:
                
                if x[0] in prefixes:
                    
                    return
                
            # absolute URL
            url = hyperlink.parse(property_name)
            
            if url.absolute:
                
                return
            
        message=f'Property "@type" with value "{value}" ({type(value).__name__}) '
        message+='is not valid'
            
        raise (message)
        
    
    # Common properties can take any JSON value, so long as any objects 
    # within the value (for example as items of an array or values of 
    # properties on other objects) adhere to the following restrictions, 
    # which are designed to ensure compatibility between JSON-LD-aware 
    # and non-JSON-LD-aware processors:
    
    if isinstance(property_value,dict):
        
        if '@value' in property_value:
            
            value=property_value['@value']
            
            # If a @value property is used on an object, that object must not have 
            # any other properties aside from either @type or @language, and must 
            # not have both @type and @language as properties. 
            
            if list(property_value)==['@value','@type']:
                
                pass
            
            elif list(property_value)==['@value','@language']:
                
                pass
            
            else:
                
                message=f'Property "{property_name}" with value "{property_value}" '
                message+='has invalid object properties. '
                message+='As a "@value" property is present, there should '
                message+='only be either a "@type" property or a "@language" property. '
                
                raise ValueError(message)
            
            # The value of the @value property must be a string, number, or 
            # boolean value.
            
            if isinstance(value,str) or isinstance(value,int) \
                or isinstance(value,float) or isinstance(value,bool):
                    
                pass
            
            else:
                
                message=f'Property "{property_name}" with '
                message+='value "{property_value}" ({type(property_value).__name__}) '
                message+='has invalid value of its "@value" object property. '
                message+='This should be of type string, number or boolean'
                
                raise ValueError
            
            # If @type is also used, its value must be one of:
            if '@type' in property_value:
                
                type_=property_value['@type']
                
                is_valid_flag=False
            
                # - a built-in datatype, as defined in section 5.11.1 Built-in Datatypes.
                if type_ in datatypes:
                    
                    is_valid_flag=True
                
                # - a prefixed name using any of the pre-defined prefixes as described in 
                #   section 5.8.1 Names of Common Properties.
                if ':' in type_:
                    x=type_.split(':')
                else:
                    x=type_.split('%3A')  # this is the percent encoded version
                    
                if len(x)==2:
                    
                    if x[0] in prefixes:
                        
                        is_valid_flag=True
                
                # - a string, which must be interpreted as an absolute URL.
                if isinstance(type_,str):
                    
                    url = hyperlink.parse(property_name)
                    
                    if url.absolute:
                        
                        is_valid_flag=True
                
                if not is_valid_flag:
                    
                    message='Property "@type" with '
                    message+='value "{type_}" ({type(type_).__name__}) '
                    message+='is invalid. '
                    
                    raise ValueError(message)
                
            # If a @language property is used, it must have a string value that 
            # adheres to the syntax defined in [BCP47], or be null.
            if '@language' in property_value:
                
                language=property_value['@language']
                
                if isinstance(language,str) and langcodes.tag_is_valid(language):
                    
                    pass
                
                elif language is None:
                    
                    pass
                
                else:
                    
                    message='Property "@language" with '
                    message+='value "{language}" ({type(language).__name__}) '
                    message+='is invalid. '
                    
                    raise ValueError(message)
                
                
        else:  # no @value property present
            
            for k,v in property_value.items():
                
                if k=='@type':
                    
                    # If a @type property is used on an object without a @value property, 
                    # its value must be one of:
                    
                    # - one of the legitimate values for @type as defined for any of the 
                    #   description objects in this specification.
                    # - a prefixed name using any of the pre-defined prefixes as described 
                    #   in section 5.8.1 Names of Common Properties.
                    # - a string, which must be interpreted as an absolute URL.
                    
                    # A @type property can also have a value that is an array of such values.
                    
                    if not isinstance(v,list):
                        
                        validate_type_property_value(v)
                    
                    else:
                        
                        for item in v:
                            
                            validate_type_property_value(item)
            
        
                if k=='@id':
                    
                    # The values of @id properties are link properties and are treated as URLs. 
                    #  During normalization, as described in section 6. Normalization, 
                    # they will have any prefix expanded and the result resolved against 
                    # the base URL. 
                    # Therefore, if an @id property is used on an object, it must have a 
                    # value that is a string and that string must not start with _:.
                    
                    if not isinstance(v,str):
                        
                        message='Property "@id" with value "{v}" ({type(v).__name__}) '
                        message+='is invalid. Expected type is string.'
                        
                        raise TypeError(message)
                        
                    if v.startswith('_:'):
                        
                        message='Property "@id" with value "{v}" ({type(v).__name__}) '
                        message+='is invalid. The value must not start with "-:".'
                        
                        raise ValueError(message)
        
                    
                if k=='@language':
                        
                    # A @language property must not be used on an object unless it also 
                    # has a @value property.
                    
                    message='Property "@language" with value "{v}" ({type(v).__name__}) '
                    message+='is invalid. A @language property must not be '
                    message+='used on an object unless it also has a @value property.'
                    
                    raise ValueError(message)
                
                else:
                    
                    # Aside from @value, @type, @language, and @id, the properties used 
                    # on an object must not start with @.
                    
                    if k.startswith('@'):
                        
                        message='Property "{k}" with value "{v}" ({type(v).__name__}) '
                        message+='is invalid. Aside from @value, @type, @language, ' 
                        message+='and @id, the properties used on an object must not start with "@".'
                        
                        raise ValueError(message)
                    
                    # These restrictions are also described in section A. JSON-LD Dialect, 
                    # from the perspective of a processor that otherwise supports JSON-LD. 
                    # Examples of common property values and the impact of normalization 
                    # are given in section 6.1 Examples.
        
    
    elif isinstance(property_value,list):
        
        for item in property_value:
            
            validate_common_property_value(
                item
                )
        
    else:
        
        pass
        
    
        
    # normalize_common_property_or_notes(
    #     metadata_obj_dict,
    #     property_name,
    #     property_value,
    #     base_url,
    #     default_language
    #     )



#%% 5.9 - Dialect Descriptions

def validate_and_normalize_metadata_dialect_dict(
        metadata_dialect_dict,
        has_top_level_properties=True
        ):
    """
    """
    raise NotImplementedError


#%% 5.10 - Transformation Definitions

def validate_and_normalize_metadata_transformation_dict(
        metadata_transformation_dict,
        base_url,
        default_language,
        has_top_level_property=False
        ):
    """
    """
    raise NotImplementedError
        
        
#%% Section 6 - Normalization

def normalize_common_property_or_notes(
        metadata_obj_dict,
        property_name,
        property_value,
        base_url,
        default_language
        ):
    """
    """

    # 1. If the property is a common property or notes the value must be 
    #    normalized as follows:

    # 1.1 If the value is an array, each value within the array is normalized 
    #     in place as described here.
    if isinstance(property_value,list):
        
        result=[]
        
        for x in property_value:
            
            y={property_name:property_value}
        
            normalize_common_property_or_notes(
                y,
                property_name,
                x,
                base_url,
                default_language
                )
            
            result.append(x)
    
        property_value=result
    
    
    # 1.2 If the value is a string, replace it with an object with a @value 
    #  property whose value is that string. If a default language is specified, 
    #  add a @language property whose value is that default language.
    elif isinstance(property_value,str):
        
        if default_language=='und':
        
            property_value={
                '@value': property_value
                }
    
        else:
            
            property_value={
                '@value': property_value,
                '@language': default_language
                }
    
    
    # 1.3 If the value is an object with a @value property, it remains as is.
    elif isinstance(property_value,dict) and '@value' in property_value:
        
        return
    
    
    # 1.4 If the value is any other object, normalize each property of that 
    #  object as follows:
    elif isinstance(property_value,dict):
        
        for k, v in property_value.items():
        
            # 1.4.1 If the property is @id, expand any prefixed names and resolve 
            #  its value against the base URL.
            if k=='@id':
                
                # expand name
                if ':' in v:
                    
                    x=v.split(':')

                else:

                    x=v.split('%3A')  # this is the percent encoded version
                    
                if len(x)==2:

                    if x[0] in prefixes:

                        expanded_url=prefixes[x[0]]+x[1]

                    else:

                        expanded_url=v
                    
                else:
                    expanded_url=v
                
                # resolve url                
                resolved_url=\
                    urllib.parse.urljoin(
                        base_url,
                        expanded_url
                        )
                
                property_value[k]=resolved_url
        
            # 1.4.2 If the property is @type, then its value remains as is.
            elif k=='@type':
                
                pass
            
            # 1.4.3 Otherwise, normalize the value of the property as if it were 
            #  a common property, according to this algorithm.
            else:
                
                y={k:v}
                
                normalize_common_property_or_notes(
                    y,
                    k,
                    v,
                    base_url,
                    default_language     
                    )
            
                property_value[k]=v
    
            
    # 1.5 Otherwise, the value remains as is.
    else:
        
        return
    
    #
    metadata_obj_dict[property_name]=property_value


def normalize_link_property(
        metadata_obj_dict,
        property_name,
        property_value,
        base_url
        ):
    """
    """
    
    # 3 If the property is a link property the value is turned into an 
    #  absolute URL using the base URL and normalized as described in 
    #  URL Normalization [tabular-data-model].

    property_value=\
        urllib.parse.urljoin(
            base_url,
            property_value
            )
    
    property_value=hyperlink.parse(property_value).normalize()
        
    metadata_obj_dict[property_name]=property_value


def normalize_object_property(
        metadata_obj_dict,
        property_name,
        property_value,
        base_url
        ):
    """
    """
    # 4. If the property is an object property with a string value, the 
    #    string is a URL referencing a JSON document containing a single 
    #    object. 
    #    Fetch this URL to retrieve an object, which may have a local 
    #    @context. 
    #    Normalize each property in the resulting object recursively using 
    #    this algorithm and with its local @context then remove the local 
    #    @context property. 
    #    If the resulting object does not have an @id property, add an 
    #    @id whose value is the original URL. 
    #    This object becomes the value of the original object property.
    
    if isinstance(property_value,str):
        
        url=property_value
            
        try:
            
            with open(url) as f:
                
                referenced_metadata_obj_dict=json.load(f)
                
        except FileNotFoundError:
            
            try:
            
                response=requests.get(url)
                text=response.text
                referenced_metadata_obj_dict=json.loads(text)
            
            except (requests.MissingSchema, requests.ConnectionError):
                
                message=f'Property "{property_name}" ' 
                message+=f'with value "{property_value}" '
                message+='does not refer to a local or remote file.'    
                
        raise ValueError(message)
        
        property_value=referenced_metadata_obj_dict
        
        metadata_obj_dict[property_name]=property_value
    
        
    else:
        
        url=None
        
    return url
        
        
def normalize_atomic_property(
        metadata_obj_dict,
        property_name,
        property_value
        ):
    """
    """
    # 7 If the property is an atomic property that can be a string or an 
    #  object, normalize to the object form as described for that property.
    if property_name=='format' and isinstance(property_value,str):
        
        property_value={'pattern':property_value}
    
    elif property_name=='datatype' and isinstance(property_value,str):
        
        property_value={'base':property_value}
    
    metadata_obj_dict[property_name]=property_value
        
        
        
        
        
        
        
        
        
        
        
        
        
    
    
    
    