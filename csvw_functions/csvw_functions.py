# -*- coding: utf-8 -*-


import json
import pkg_resources
import urllib.parse
import os
import requests
import csv
import re
import datetime
import langcodes
import uritemplate
import warnings


#%% read metadata schemas

schemas={}

metadata_schema_files=[
    'column_description.schema.json', 
    'common_properties.schema.json', 
    'datatype_description.schema.json', 
    'dialect_description.schema.json', 
    'foreign_key_definition.schema.json', 
    'foreign_key_reference.schema.json', 
    'inherited_properties.schema.json', 
    'number_format.schema.json', 
    'schema_description.schema.json', 
    'table_description.schema.json', 
    'table_group_description.schema.json', 
    'top_level_properties.schema.json', 
    'transformation_definition.schema.json'
    ]

for schema_file in metadata_schema_files:
    resource_package = __name__
    resource_path = '/'.join(('metadata_schema_files', schema_file))  
    data = pkg_resources.resource_string(resource_package, resource_path)
    json_dict=json.loads(data)
    schemas[schema_file]=json_dict
    
    
#%% identify metadata schema properties
    
top_level_properties=schemas['top_level_properties.schema.json']['properties']
inherited_properties=schemas['inherited_properties.schema.json']['properties']

all_optional_and_required_properties={}
for schema_name,schema in schemas.items():
    if not schema_name in ['top_level_properties.schema.json',
                           'inherited_properties.schema.json',
                           'common_properties.schema.json']:
        all_optional_and_required_properties.update(schema['properties'])

all_properties={
    **top_level_properties,
    **inherited_properties,
    **all_optional_and_required_properties
    }
    
#%% read annotated schemas

annotated_schema_files=[
    'annotated_cell.schema.json', 
    'annotated_column.schema.json', 
    'annotated_datatype.schema.json', 
    'annotated_row.schema.json', 
    'annotated_table.schema.json', 
    'annotated_table_group.schema.json', 
    ]

for schema_file in annotated_schema_files:
    resource_package = __name__
    resource_path = '/'.join(('model_schema_files', schema_file))  
    data = pkg_resources.resource_string(resource_package, resource_path)
    json_dict=json.loads(data)
    schemas[schema_file]=json_dict
    


#%% schema prefixes

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


#%% datatypes

# Metadata Section 5.11.1
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


# lists of datatype collections

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




#%% custom exceptions and warnings

class ValidationError(Exception):
    ""
    
    
class ValidationWarning(Warning):
    ""



#%% FUNCTIONS - Top Level Functions

def get_embedded_metadata_from_csv(
        csv_file_path_or_url,
        comment_prefix=None,
        delimiter=None,
        escape_character=None,
        header_row_count=None,
        line_terminators=None,
        quote_character=None,
        skip_blank_rows=None,
        skip_columns=None,
        skip_rows=None,
        trim=None
        ):
    """Returns the embedded metadata from a CSV file.
    
    :param csv_file_path_or_url: Location of the CSV file.
        Either a) a relative local file path; b) 
        an absolute local file path; or c) a full URL
    :type csv_file_path_or_url: str
    
    :param comment_prefix: A string that, when it appears at the beginning of 
        a row, indicates that the row is a comment that should be associated 
        as a rdfs:comment annotation to the table. This is set by the 
        commentPrefix property of a dialect description. The default 
        is null, which means no rows are treated as comments. A value 
        other than null may mean that the source numbers of rows are 
        different from their numbers.
    :type comment_prefix: str
        
    :param delimiter: The separator between cells, set by the delimiter 
        property of a dialect description. The default is ,.
    :type delimiter: str
    
    :param escape_character: The string that is used to escape the quote 
        character within escaped cells, or null, set by the doubleQuote 
        property of a dialect description. The default is " (such that "" 
        is used to escape " within an escaped cell).
    :type escape_character: str
    
    :param header_row_count: The number of header rows (following the 
        skipped rows) in the file, set by the header or headerRowCount 
        property of a dialect description. The default is 1. A value other 
        than 0 will mean that the source numbers of rows will be different 
        from their numbers.
    :type header_row_count: int
    
    :param line_terminators: The strings that can be used at the end of a 
        row, set by the lineTerminators property of a dialect description. 
        The default is [CRLF, LF].
    :type line_terminators: list
    
    :param quote_character: The string that is used around escaped 
        cells, or null, set by the quoteChar property of a dialect 
        description. The default is ".
    :type quote_character: str
    
    :param skip_blank_rows: Indicates whether to ignore wholly empty 
        rows (i.e. rows in which all the cells are empty), set by the 
        skipBlankRows property of a dialect description. The default is 
        false. A value other than false may mean that the source numbers 
        of rows are different from their numbers.
    :type skip_blank_rows: bool
        
    :param skip_columns: The number of columns to skip at the beginning 
        of each row, set by the skipColumns property of a dialect 
        description. The default is 0. A value other than 0 will mean 
        that the source numbers of columns will be different from their 
        numbers.
    :type skip_columns: int
        
    :param skip_rows: The number of rows to skip at the beginning of the 
        file, before a header row or tabular data, set by the skipRows 
        property of a dialect description. The default is 0. A value 
        greater than 0 will mean that the source numbers of rows will be 
        different from their numbers.
    :type skip_rows: int
    
    :param trim: Indicates whether to trim whitespace around cells; may 
        be true, false, start, or end, set by the skipInitialSpace or 
        trim property of a dialect description. The default is true.
    :type trim: str or bool
    
    :returns: A CSVW metadata.json file as a Python dictionary.
    :rtype: dict
    
    """
    dialect_flags=dict(
        commentPrefix=comment_prefix,
        delimiter=delimiter,
        escapeCharacter=escape_character,
        headerRowCount=header_row_count,
        lineTerminators=line_terminators,
        quoteCharacter=quote_character,
        skipBlankRows=skip_blank_rows,
        skipColumns=skip_columns,
        skipRows=skip_rows
        )
    dialect_flags={k:v for k,v in dialect_flags.items() if v}
    if len(dialect_flags)==0:
        dialect_flags=None
    
    embedded_metadata=\
        create_annotated_tables_from_csv_file_path_or_url(
            csv_file_path_or_url,
            overriding_metadata_file_path_or_url=None,
            validate=False,
            return_embedded_metadata=True,
            dialect_flags=dialect_flags
            )
    return embedded_metadata
    


def validate_metadata(
        metadata_file_path_or_url
        ):
    """Validates a metadata.json file.
    
    :param metadata_file_path_or_url: Location of the metadata.json file.
        Either a) a relative local file path; b) 
        an absolute local file path; or c) a full URL
    :type overriding_metadata_file_path_or_url: str
    
    
    
    :returns: True if the metadata document is valid; False if not valid
    :rtype: bool
    
    """
    result=True
    
    metadata_file_path, metadata_file_url=\
        get_path_and_url_from_file_location(
            metadata_file_path_or_url
            )
        
    if not metadata_file_path is None:
        
        metadata_file_text=\
            get_text_from_file_path(
                metadata_file_path)
        headers=None
            
    
    elif not metadata_file_url is None:
    
        metadata_file_text, headers=\
            get_text_and_headers_from_file_url(
                metadata_file_url)
        
    
    # TO DO
    
    
    return result
    
    
    
    
def validate_csv(
        csv_file_path_or_url,
        metadata_file_path_or_url
        ):
    """
    
    :param file_path_or_url: CSV file or metadata.json file
    :type file_path_or_url: str
    
    """
    
    # TO DO
    
    
def get_annotated_table_group_from_csv(
        csv_file_path_or_url,
        overriding_metadata_file_path_or_url=None,
        comment_prefix=None,
        delimiter=None,
        escape_character=None,
        header_row_count=None,
        line_terminators=None,
        quote_character=None,
        skip_blank_rows=None,
        skip_columns=None,
        skip_rows=None,
        ):
    """Returns an annotated table group derived from a CSV file.
    
    :param csv_file_path_or_url: Location of the CSV file.
        Either a) a relative local file path; b) 
        an absolute local file path; or c) a full URL
    :type csv_file_path_or_url: str
    
    :param overriding_metadata_file_path_or_url: Location of a metadata.json
        file to be used as "overriding metadata".
        Either a) a relative local file path; b) 
        an absolute local file path; or c) a full URL
    :type overriding_metadata_file_path_or_url: str
    
    :param comment_prefix: A string that, when it appears at the beginning of 
        a row, indicates that the row is a comment that should be associated 
        as a rdfs:comment annotation to the table. This is set by the 
        commentPrefix property of a dialect description. The default 
        is null, which means no rows are treated as comments. A value 
        other than null may mean that the source numbers of rows are 
        different from their numbers.
    :type comment_prefix: str
        
    :param delimiter: The separator between cells, set by the delimiter 
        property of a dialect description. The default is ,.
    :type delimiter: str
    
    :param escape_character: The string that is used to escape the quote 
        character within escaped cells, or null, set by the doubleQuote 
        property of a dialect description. The default is " (such that "" 
        is used to escape " within an escaped cell).
    :type escape_character: str
    
    :param header_row_count: The number of header rows (following the 
        skipped rows) in the file, set by the header or headerRowCount 
        property of a dialect description. The default is 1. A value other 
        than 0 will mean that the source numbers of rows will be different 
        from their numbers.
    :type header_row_count: int
    
    :param line_terminators: The strings that can be used at the end of a 
        row, set by the lineTerminators property of a dialect description. 
        The default is [CRLF, LF].
    :type line_terminators: list
    
    :param quote_character: The string that is used around escaped 
        cells, or null, set by the quoteChar property of a dialect 
        description. The default is ".
    :type quote_character: str
    
    :param skip_blank_rows: Indicates whether to ignore wholly empty 
        rows (i.e. rows in which all the cells are empty), set by the 
        skipBlankRows property of a dialect description. The default is 
        false. A value other than false may mean that the source numbers 
        of rows are different from their numbers.
    :type skip_blank_rows: bool
        
    :param skip_columns: The number of columns to skip at the beginning 
        of each row, set by the skipColumns property of a dialect 
        description. The default is 0. A value other than 0 will mean 
        that the source numbers of columns will be different from their 
        numbers.
    :type skip_columns: int
        
    :param skip_rows: The number of rows to skip at the beginning of the 
        file, before a header row or tabular data, set by the skipRows 
        property of a dialect description. The default is 0. A value 
        greater than 0 will mean that the source numbers of rows will be 
        different from their numbers.
    :type skip_rows: int
    
    :param trim: Indicates whether to trim whitespace around cells; may 
        be true, false, start, or end, set by the skipInitialSpace or 
        trim property of a dialect description. The default is true.
    :type trim: str or bool
    
    :returns: A CSVW annotated table group object, as a Python dictionary.
    :rtype: dict
    
    """
    # dialect_flags
    dialect_flags=dict(
        commentPrefix=comment_prefix,
        delimiter=delimiter,
        escapeCharacter=escape_character,
        headerRowCount=header_row_count,
        lineTerminators=line_terminators,
        quoteCharacter=quote_character,
        skipBlankRows=skip_blank_rows,
        skipColumns=skip_columns,
        skipRows=skip_rows
        )
    dialect_flags={k:v for k,v in dialect_flags.items() if v}
    if len(dialect_flags)==0:
        dialect_flags=None
    
    # get annotated table group dict
    annotated_table_group_dict=\
        create_annotated_tables_from_csv_file_path_or_url(
            csv_file_path_or_url,
            overriding_metadata_file_path_or_url=overriding_metadata_file_path_or_url,
            validate=False,
            return_embedded_metadata=False,
            dialect_flags=dialect_flags
            )

    return annotated_table_group_dict


def get_annotated_table_group_from_metadata(
        metadata_file_path_or_url
        ):
    """Returns an annotated table group derived from a metadata.json file.
    
    :param metadata_file_path_or_url: Location of the metadata.json file.
        Either a) a relative local file path; b) 
        an absolute local file path; or c) a full URL
    :type overriding_metadata_file_path_or_url: str
    
    :returns: A CSVW annotated table group object, as a Python dictionary.
    :rtype: dict
    
    """
    annotated_table_group_dict=\
        create_annotated_tables_from_metadata_file_path_or_url(
                metadata_file_path_or_url
                )
        
    return annotated_table_group_dict
    

def get_json_ld_from_annotated_table_group(
        annotated_table_group_dict,
        mode='standard'
        ):
    """
    
    :param mode: Either 'standard' or 'minimal'
    
    """
    




def convert_annotated_table_group_to_rdf(
        nnotated_table_group_dict
        ):
    """
    """
    

    
    
#%% FUNCTIONS - Model for Tabular Data and Metadata

#%% Section 5 - Locating Metadata

# def get_embedded_metadata_from_csv_file(
#         csv_file_path_or_url
#         ):
#     """
#     """
#     csv_file_path, csv_file_url=\
#         get_path_and_url_from_file_location(
#             csv_file_path_or_url
#             )
    
#     csv_text_line_generator=get_text_line_generator_from_path_or_url(
#         csv_file_path, 
#         csv_file_url
#         )  
    
#     column_titles=get_column_titles_of_csv_file_text_line_generator(
#         csv_text_line_generator
#         )
#     column_description_objects=[{'titles':[column_title]}
#                                 for column_title in column_titles]
    
#     schema_description_object={
#         'columns': column_description_objects
#         }
    
#     table_description_object={
#         '@context': "http://www.w3.org/ns/csvw",
#         '@type': 'Table', 
#         'url': csv_file_path or csv_file_url,
#         'tableSchema': schema_description_object
#         }
    
#     return table_description_object
    

#%% Section 6.1 - Creating Annotated Tables


def create_annotated_tables_from_csv_file_path_or_url(
        csv_file_path_or_url,
        overriding_metadata_file_path_or_url=None,
        validate=False,
        return_embedded_metadata=False,
        dialect_flags=None
        ):
    """
    
    :param csv_file_path_or_url: Either a) a relative local file path; b) 
        an absolute local file path; or c) a full URL
    :type csv_file_path_or_url: str
    
    """
    csv_file_absolute_path, csv_file_url=\
        get_path_and_url_from_file_location(
            csv_file_path_or_url
            )
    
    # 1 Retrieve the tabular data file.
    
    # 2 Retrieve the first metadata file (FM) as described in section 5. Locating Metadata:
        
    # 2.1 metadata supplied by the user (see section 5.1 Overriding Metadata).
    
    if not overriding_metadata_file_path_or_url is None:
    
        metadata_file_path, metadata_file_url=\
            get_path_and_url_from_file_location(
                overriding_metadata_file_path_or_url
                )
            
        if not metadata_file_path is None:
            
            metadata_file_text=\
                get_text_from_file_path(
                    metadata_file_path)
            
        elif not metadata_file_url is None:
        
            metadata_file_text, headers=\
                get_text_and_headers_from_file_url(
                    metadata_file_url)
        
        else:
            raise Exception
        
        metadata_root_obj_dict=json.loads(metadata_file_text)
        
        metadata_type=\
            get_type_of_metadata_object(
                metadata_root_obj_dict
                )
        
        if metadata_type=='TableGroup':
            table_dict=metadata_root_obj_dict['tables'][0]  # NOTE assumed first table present is used...?
            
        elif metadata_type=='Table':
            table_dict=metadata_root_obj_dict
        
        # include csv file path or url in metadata
        table_dict['url']=csv_file_absolute_path or csv_file_url
        
        # add dialect_flags
        if not dialect_flags is None:
            
            x=table_dict.setdefault('dialect',{})
            x.update(dialect_flags)
        
    
        return create_annotated_tables_from_metadata_root_object(
            metadata_root_obj_dict,
            metadata_file_path,
            metadata_file_url,
            validate=validate,
            from_csv=True,
            return_embedded_metadata=return_embedded_metadata,
            )

    # 2.2 metadata referenced from a Link Header that may be returned when 
    #     retrieving the tabular data file (see section 5.2 Link Header).
        
    # TO DO
    
    if not csv_file_url is None:
        
        pass
    
    # 2.3 metadata retrieved through a site-wide location configuration 
    #     (see section 5.3 Default Locations and Site-wide Location Configuration).

    # TO DO
    
    # 2.4 embedded metadata as defined in section 5.4 Embedded Metadata with 
    #     a single tables entry where the url property is set from that of the 
    #     tabular data file.
    
    metadata_root_obj_dict={
        'url': csv_file_absolute_path or csv_file_url,
        '@type': 'Table',
        'tableSchema':{}
        }
    if dialect_flags:
        metadata_root_obj_dict['dialect']=dialect_flags
    
    #print(metadata_root_obj_dict)
    
    return create_annotated_tables_from_metadata_root_object(
        metadata_root_obj_dict=metadata_root_obj_dict,
        metadata_file_path='.',
        metadata_file_url=None,
        validate=validate,
        embedded_metadata=True,
        from_csv=True,
        return_embedded_metadata=return_embedded_metadata
        )



def create_annotated_tables_from_metadata_file_path_or_url(
        metadata_file_path_or_url
        ):
    """
    """
    metadata_file_path, metadata_file_url=\
        get_path_and_url_from_file_location(
            metadata_file_path_or_url
            )
        
    if not metadata_file_path is None:
        
        metadata_file_text=\
            get_text_from_file_path(
                metadata_file_path)
        
        metadata_root_obj_dict=json.loads(metadata_file_text)
        
        annotated_tables_dict=\
            create_annotated_tables_from_metadata_root_object(
                    metadata_root_obj_dict,
                    metadata_file_path=metadata_file_path,
                    metadata_file_url=None
                    )
            
        
    elif not metadata_file_url is None:
    
        metadata_file_text, headers=\
            get_text_and_headers_from_file_url(
                metadata_file_url)
        
        metadata_root_obj_dict=json.loads(metadata_file_text)
        
        annotated_tables_dict=\
            create_annotated_tables_from_metadata_root_object(
                    metadata_root_obj_dict,
                    metadata_file_path=None,
                    metadata_file_url=metadata_file_url
                    )
    
    else:
        
        raise Exception
        
    return annotated_tables_dict


def create_annotated_tables_from_metadata_root_object(
        metadata_root_obj_dict=None,
        metadata_file_path=None,
        metadata_file_url=None,
        validate=False,
        embedded_metadata=False,
        from_csv=False,
        return_embedded_metadata=False
        ):
    """
    
    Model for Tabular Data and Metadata, Section 6.1.
    
    """
    
    annotated_table_group_dict={
        'tables':[]
        }
    
    # After locating metadata, metadata is normalized and coerced into a 
    # single table group description. When starting with a metadata file, 
    # this involves normalizing the provided metadata file and verifying 
    # that the embedded metadata for each tabular data file referenced 
    # from the metadata is compatible with the metadata. 
    
    # 1 Retrieve the metadata file yielding the metadata UM (which is 
    #   treated as overriding metadata, see section 5.1 Overriding Metadata).
    # AND
    # 2 Normalize UM using the process defined in Normalization in 
    #   [tabular-metadata], coercing UM into a table group description, 
    #   if necessary.
    
    
    
    # get and normalize metadata file
    normalized_metadata_obj_dict=\
        normalize_metadata_root_object(
            metadata_root_obj_dict,
            metadata_file_path,
            metadata_file_url
            )
        
    # get base path & url
    base_path, base_url=\
        get_base_path_and_url_of_metadata_object(
            metadata_root_obj_dict,
            metadata_file_path,
            metadata_file_url
            )
        
    # get default language
    default_language=\
        get_default_language_of_metadata_object(
            metadata_root_obj_dict
            )
    
    # convert to TableGroup if needed
    metadata_type=\
        get_type_of_metadata_object(
            normalized_metadata_obj_dict
            )
    if metadata_type=='TableGroup':
        metadata_table_group_obj_dict=normalized_metadata_obj_dict
        
    elif metadata_type=='Table':
        metadata_table_group_obj_dict={
            '@context': "http://www.w3.org/ns/csvw",
            '@type': 'TableGroup',
            'tables': [normalized_metadata_obj_dict]
            }
        metadata_table_group_obj_dict['tables'][0].pop('@context', None)
        
    else:
        raise Exception
                 
    # 3 For each table (TM) in UM in order, create one or more annotated tables:
    
    for table_index,metadata_table_obj_dict in \
        enumerate(metadata_table_group_obj_dict['tables']):
            
        # 3.1 Extract the dialect description (DD) from UM for the table 
        #     associated with the tabular data file. If there is no such 
        #     dialect description, extract the first available dialect 
        #     description from a group of tables in which the tabular data 
        #     file is described. Otherwise use the default dialect description.

        default_dialect_flag=False

        dialect_description_obj_dict=\
            metadata_table_obj_dict.get('dialect',None)
            
        # gets the first dialect description in the group of tables
        if dialect_description_obj_dict is None:
            for metadata_table_obj_dict in \
                metadata_table_group_obj_dict['tables']:
                    if 'dialect' in metadata_table_obj_dict:
                        dialect_description_obj_dict=\
                            metadata_table_obj_dict['dialect']
                        break

        # gets the default dialect description
        if dialect_description_obj_dict is None:
            
            default_dialect_flag=True
            
            dialect_description_schema=\
                get_schema_from_schema_name(
                        'dialect_description.schema.json'
                        )
            
            dialect_description_obj_dict={}
            for k,v in dialect_description_schema['properties'].items():
                if 'default' in v:
                    dialect_description_obj_dict[k]=v['default']
                    
        encoding=dialect_description_obj_dict.get('encoding',None)
        
        # load table
        tabular_data_file_path_or_url=metadata_table_obj_dict['url']    
        
        tabular_data_file_path, tabular_data_file_url=\
            get_path_and_url_from_file_location(
                tabular_data_file_path_or_url
                )
            
        if not tabular_data_file_path is None:
            
            tabular_data_text=\
                get_text_from_file_path(
                    tabular_data_file_path,
                    encoding=encoding
                    )
            headers={}
            
        elif not tabular_data_file_url is None:
        
            tabular_data_text, headers=\
                get_text_and_headers_from_file_url(
                    tabular_data_file_url,
                    encoding=encoding
                    )
        
        else:
            
            raise Exception
        
        #print(tabular_data_text)
        
        # 3.2 If using the default dialect description, override default values 
        #     in DD based on HTTP headers found when retrieving the tabular data file:
        #     - If the media type from the Content-Type header is text/tab-separated-values, 
        #       set delimiter to TAB in DD.
        #     - If the Content-Type header includes the header parameter with a 
        #       value of absent, set header to false in DD.
        #     - If the Content-Type header includes the charset parameter, set 
        #       encoding to this value in DD.
        
        if default_dialect_flag:
        
            content_type=headers.get('Content-Type',None)
            if not content_type is None:
                if 'text/tab-separated-values' in content_type:  # NEEDS TESTING
                    dialect_description_obj_dict['delimter']='\t'
                if 'header=absent' in content_type:  # NEEDS TESTING
                    dialect_description_obj_dict['header']=False
                if 'charset' in content_type:  # NEEDS TESTING
                    charset_value=\
                        content_type.split('charset')[1].split(';')[0].strip()[1:]  # NEEDS TESTING
                    dialect_description_obj_dict['encoding']=charset_value

        # 3.3 Parse the tabular data file, using DD as a guide, to create a 
        #     basic tabular data model (T) and extract embedded metadata (EM), 
        #     for example from the header line.

        table_dict, embedded_metadata_dict=\
            parse_tabular_data_from_text(
                tabular_data_text,
                tabular_data_file_path_or_url,
                dialect_description_obj_dict
                )
            
        if return_embedded_metadata:  # function only returns the embedded metadata
            return embedded_metadata_dict
            
        # if called with embedded_metadata=True
        if embedded_metadata:
            metadata_table_obj_dict=embedded_metadata_dict
            metadata_table_obj_dict.pop('@context')
            metadata_table_group_obj_dict['tables'][table_index]=metadata_table_obj_dict
            #print(metadata_table_group_obj_dict)
        
        # 3.4 If a Content-Language HTTP header was found when retrieving the 
        #     tabular data file, and the value provides a single language, set 
        #     the lang inherited property to this value in TM, unless TM 
        #     already has a lang inherited property.
        content_language=headers.get('Content-Language',None)
        if not content_language is None:
            if not 'lang' in metadata_table_obj_dict:
                metadata_table_obj_dict['lang']=content_language  # NEEDS TESTING
    
        # 3.5 Verify that TM is compatible with EM using the procedure defined 
        #     in Table Description Compatibility in [tabular-metadata]; if TM 
        #     is not compatible with EM validators must raise an error, other 
        #     processors must generate a warning and continue processing.
        compare_table_descriptions(
            metadata_table_obj_dict,
            embedded_metadata_dict,
            validate=validate
            )
        
        annotated_table_group_dict['tables'].append(table_dict)
        
        # Not directly in this section of the standard, but Section 8.2.1.1
        # suggests that the metadata_table_obj_dict is merged with the embedded metadata
        # Here this is done for certain properties which are present in the 
        # embedded metadata but not present in the metadata_table_obj_dict
        # NOTE - may need improving, might not work for all properties and cases
        # at present
        
        if from_csv:        
        
            metadata_table_obj_dict=\
                merge_metadata_objs(
                    metadata_table_obj_dict,
                    embedded_metadata_dict
                    )
        
        
    # 3.6 Use the metadata TM to add annotations to the tabular data model 
    #     T as described in Section 2 Annotating Tables in [tabular-metadata].
    
    annotated_table_group_dict=\
        annotate_table_group(
            annotated_table_group_dict,
            metadata_table_group_obj_dict,
            default_language
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
                        separator=annotated_column_dict['separator']
                        )
                    
                annotated_cell_dict['value']=value
                annotated_cell_dict['errors'].append(errors)
                #print(annotated_column_dict['aboutURL'])
                
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
                    annotated_cell_dict['valueURL']=\
                        get_URI_from_URI_template(
                            annotated_cell_dict['valueURL'],
                            annotated_cell_dict,
                            tabular_data_file_path, 
                            tabular_data_file_url
                            )  
     
             
     
 
 
        
    #print(metadata_table_group_obj_dict)
        
    return annotated_table_group_dict
     

#%% Section 6.4 - Parsing Cells

def parse_cell(
        string_value,
        datatype,
        default,
        lang,
        null,
        required,
        separator,
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
            raise NotImplementedError
            
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
                
                cell_value={'@value':json_value,
                            '@type':datatypes[type_]}
                
                if not language is None:
                    cell_value['@language']:lang
                
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
        
    # dates and times
    elif datatype['base']in datatypes_dates_and_times:
        
        json_value,type_,errors=parse_date_and_time(
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


#%% Section 6.4.2 Formats for numeric type

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
    
    



#%% Section 6.4.4. Formats for dates and times

def parse_date_and_time(
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
        
        json_value=string_value
        
        return json_value, type_, errors

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

    if datatype_base=='date':

        if datatype_format in [
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
                ]:
            
            x=datatype_format
            x=x.replace('yyyy','%Y')
            x=x.replace('MM','%m')
            x=x.replace('M','%m')
            x=x.replace('dd','%d')
            x=x.replace('d','%d')
            
            dt=datetime.datetime.strptime(string_value, x)  # NEEDS CHECKING
            
            json_value=dt.date().isoformat()
            
            return json_value, type_, errors
        
    else:
        
        raise Exception
        
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

    




#%% Section 8 - Parsing Tabular Data

def parse_tabular_data_from_text(
        tabular_data_text,
        tabular_data_file_path_or_url,
        dialect_description_obj_dict
        ):
    """
    """
    # 8. Parsing Tabular Data
    
    # ... hard coded the defaults here...
    
    #print(tabular_data_text)
    
    #print(dialect_description_obj_dict)
    
    comment_prefix=dialect_description_obj_dict.get('commentPrefix',None)
    delimiter=dialect_description_obj_dict.get('delimiter',',')
    escape_character=dialect_description_obj_dict.get('escapeCharacter','"')
    header_row_count=dialect_description_obj_dict.get('headerRowCount',1)
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
        notes=False,
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

    metadata_dict={
        "@context": "http://www.w3.org/ns/csvw",
        "rdfs:comment": [],
        "tableSchema": {
        "columns": [],
        "@type": 'Table'
      }
    }

    # 3 If the URL of the tabular data file being parsed is known, set the 
    # url property on M to that URL.
    
    if not tabular_data_file_path_or_url is None:
        metadata_dict['url']=tabular_data_file_path_or_url

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
                
            metadata_dict['rdfs:comment'].append(
                row_content[len(comment_prefix):]
                )
        
        # 6.3 Otherwise, if the row content is not an empty string, add the 
        # row content to the M.rdfs:comment array.
        elif not row_content=='':
            metadata_dict['rdfs:comment'].append(
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
                
            metadata_dict['rdfs:comment'].append(
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
            if len(metadata_dict['tableSchema']['columns'])==0:
                metadata_dict['tableSchema']['columns']=\
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
                metadata_dict['tableSchema']['columns'][i]['titles'].append(
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
        
        metadata_dict['tableSchema']['columns']=\
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
                
            metadata_dict['rdfs:comment'].append(
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
                    table=table_dict, #table_name, 
                    number=row_number,
                    sourceNumber=source_row_number,
                    primaryKey=[],
                    referencedRows=[],
                    cells=[]
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
    if len(metadata_dict['rdfs:comment'])==0:
        metadata_dict.pop('rdfs:comment')
    
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
    # To read a quoted value to provide a quoted value, perform the following steps:
        
    # 1 Set the quoted value to an empty string.
    quoted_value=''
    
    # 2 Read the initial quote character and add a quote character to the quoted value.
    initial_quote_character=characters[0]
    quoted_value+=initial_quote_character
    
    # 3 Read initial characters and process as follows:
    i=1
    
    while True:
    
        # 3.1 If the string starts with the escape character followed by the quote 
        # character, append both strings to the quoted value, and move on to 
        # process the string following the quote character.
        if characters[i]==escape_character \
            and characters[i+1]==quote_character:
            quoted_value+=escape_character+quote_character
            i+=2
        
        # 3.2 Otherwise, if string starts with the escape character and the escape 
        # character is not the same as the quote character, append the escape 
        # character and the character following it to the quoted value and move 
        # on to process the string following that character.
        elif characters[i]==escape_character and \
            escape_character!=quote_character:
            quoted_value+=characters[i:i+2]
            i+=2
        
        # 3.3 Otherwise, if the string starts with the quote character, return 
        # the quoted value.
        elif characters[i]==quote_character:
            i+=1
            break
        
        # 3.4 Otherwise, append the first character to the quoted value and move 
        # on to process the string following that character.
        else:
            quoted_value+=characters[i]
            i+=1
            
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
    
        #print(list_of_cell_values)    
    
        # 3.1 If the string starts with the escape character followed by the 
        # quote character, append the quote character to the current cell 
        # value, and move on to process the string following the quote character.
        if characters[i]==escape_character \
            and characters[i+1]==quote_character:
            current_cell_value+=quote_character
            i+=2
        
        # 3.2 Otherwise, if the string starts with the escape character and 
        # the escape character is not the same as the quote character, append 
        # the character following the escape character to the current cell 
        # value and move on to process the string following that character.
        elif characters[i]==escape_character and \
            escape_character!=quote_character:
            current_cell_value+=characters[i+1]
            i+=2
        
        # 3.3 Otherwise, if the string starts with the quote character then:
        elif characters[i]==quote_character:
            
            # 3.3.1 If quoted is false, set the quoted flag to true, and move on 
            # to process the remaining string. If the current cell value is not 
            # an empty string, raise an error.
            if quoted_flag==False:
                quoted_flag==True
                if not current_cell_value=='':
                    raise Exception
                i+=1
        
            # 3.3.2 Otherwise, set quoted to false, and move on to process the 
            # remaining string. If the remaining string does not start with the 
            # delimiter, raise an error.
            else:
                quoted_flag=False
                if not characters[i+1]==delimiter:
                    raise Exception
                i+=1
        
        # 3.4 Otherwise, if the string starts with the delimiter, then:
        elif characters[i]==delimiter:
        
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
                list_of_cell_values.append(trimmed_cell_value)
                current_cell_value=''
                i+=1
        
        # 3.5 Otherwise, append the first character to the current cell value 
        # and move on to process the remaining string.
        else:
            current_cell_value+=characters[i]
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


def get_column_titles_of_csv_file_text_line_generator(
        text_line_generator
        ):
    """
    """
    reader=csv.reader(text_line_generator)
    first_row=next(reader)
    return first_row
     
    
    



#%% FUNCTIONS - Metadata Vocabulary for Tabular Data

#%% Section 4- Annotating Tables

def check_metadata_document(
        metadata_obj_dict
        ):
    """
    """
    # All compliant applications must generate errors and stop processing if 
    # a metadata document:
    # - does not use valid JSON syntax defined by [RFC7159].
    # - uses any JSON outside of the restrictions defined in section A. JSON-LD Dialect.
    # - does not specify a property that it is required to specify.
    
    # NEEDS COMPLETING


def apply_metadata_default_values(
        metadata_obj_dict
        ):
    """
    """
    
    # If a property has a value that is not permitted by this specification, 
    # then if a default value is provided for that property, compliant 
    # applications must generate a warning and use that default value. 
    # If no default value is provided for that property, compliant 
    # applications must generate a warning and behave as if the property 
    # had not been specified. Additionally, including:
    # - properties (aside from common properties) which are not defined in 
    #   this specification, and
    # - properties having invalid values for a given property.

    # NEEDS COMPLETING


def annotate_table_group(
        annotated_table_group_dict,
        metadata_table_group_obj_dict,
        default_language,
        validate=False
        ):
    """
    """
    
    #print(metadata_table_group_obj_dict)
    
    # All compliant applications must generate errors and stop processing if 
    # a metadata document:
    # - does not use valid JSON syntax defined by [RFC7159].
    # - uses any JSON outside of the restrictions defined in section A. JSON-LD Dialect.
    # - does not specify a property that it is required to specify.
    check_metadata_document(
            metadata_table_group_obj_dict
            )
    
    # Compliant applications must ignore properties (aside from common 
    # properties) which are not defined in this specification and must 
    # generate a warning when they are encoutered.
    
    # NEEDS COMPLETING
    
    # If a property has a value that is not permitted by this specification, 
    # then if a default value is provided for that property, compliant 
    # applications must generate a warning and use that default value. 
    # If no default value is provided for that property, compliant 
    # applications must generate a warning and behave as if the property 
    # had not been specified. Additionally, including:
    # - properties (aside from common properties) which are not defined in 
    #   this specification, and
    # - properties having invalid values for a given property.
    apply_metadata_default_values(
        metadata_table_group_obj_dict
        )
    
    # Metadata documents contain descriptions of groups of tables, tables, 
    # columns, rows, and cells, which are used to create annotations on an 
    # annotated tabular data model. A description object is a JSON object 
    # that describes a component of the annotated tabular data model (a group 
    # of tables, a table or a column) and has one or more properties that 
    # are mapped into properties on that component. There are two types of 
    # description objects:
    # - descriptions of particular groups of tables or tables within a single 
    #   tabular data file — these are used for notes or flags on particular data.
    # - descriptions of columns that appear within a schema, and that may 
    #   apply across multiple tabular data files — these are used to describe 
    #   the general structure of a tabular data file.
    
    # NEEDS COMPLETING
    
    # The description objects contain a number of properties. These are:
    # - properties that are used to identify the table or column that the 
    #   annotations should appear on; these match up to core annotations on 
    #   those objects in the annotated tabular data model defined in 
    #   [tabular-data-model] and do not create new annotations.
    # - properties that are used to create annotations that appear directly 
    #   on the group of tables, table or column whose description they appear 
    #   on, such as the name of a column or the dc:provenance of a table.
    # - properties that are inherited, for example by being specified on 
    #   the description of a group of tables and being inherited by each 
    #   table in that group, or being specified for a table or column but 
    #   being used to create or affect the values of annotations on each of 
    #   the cells that appear in that table or column.
    
    # NEEDS COMPLETING
    
    
    
    
    
    # inherited properties
    inherited_properties={}
    for name in get_inherited_properties_from_type('TableGroup'):
        if name in metadata_table_group_obj_dict:
            inherited_properties[name]=metadata_table_group_obj_dict[name]
    
    # annotate this table group
    #?? Initially, use this simple version
    for k,v in metadata_table_group_obj_dict.items():
        
        if k=='tables':
            
            tables=annotated_table_group_dict['tables']
            
            for i in range(len(tables)):
                tables[i]=annotate_table(
                    tables[i],
                    v[i],
                    default_language,
                    inherited_properties,
                    validate=validate
                    )
        
        elif k=='notes':
            
            notes=annotated_table_group_dict['notes']
            
            if isinstance(v,list):
                
                notes.extend(v)
            
            else:
                
                notes.append(v)
        
        else:
            
            annotated_table_group_dict[k]=v
        
    

    return annotated_table_group_dict



def annotate_table(
        annotated_table_dict,
        metadata_table_obj_dict,
        default_language,
        inherited_properties,
        validate=False
        ):
    """
    """
    
    # add inherited properties that were passed
    for k,v in inherited_properties.items():
        if k=='aboutUrl': k='aboutURL'
        if k=='propertyUrl': k='propertyURL'
        if k=='valueUrl': k='valueURL'
        if k in annotated_table_dict:
            annotated_table_dict[k]=v
            
    # include new inherited properties from metadata
    for name in get_inherited_properties_from_type('Table'):
        if name in metadata_table_obj_dict:
            inherited_properties[name]=metadata_table_obj_dict[name]
    
    # annotate this table
    #?? Initially, use this simple version
    for k,v in metadata_table_obj_dict.items():
        
        if k=='tableSchema':
            
            # include new inherited properties from metadata
            for name in get_inherited_properties_from_type('Schema'):
                if name in v:
                    inherited_properties[name]=v[name]
            
            #print(inherited_properties)
            
            for k1,v1 in v.items():
                
                if k1=='columns':
                    columns=annotated_table_dict['columns']
                    for i in range(len(columns)):
                        columns[i]=annotate_column(
                            columns[i],
                            v['columns'][i],
                            default_language,
                            inherited_properties=inherited_properties
                            )
                
                elif k1=='primaryKey':
                    pk=v1
                    if not isinstance(pk,list):
                        pk=[pk]
                    column_indexes=[]
                    for x in pk:
                        for i,column in enumerate(annotated_table_dict['columns']):
                            if x==columns[i]['name']:
                                column_indexes.append(i)
                    for row in annotated_table_dict['rows']:
                        for column_index in column_indexes:
                            row['primaryKey'].append(row['cells'][column_index])
                
        
        elif k=='notes':
            
            notes=annotated_table_dict['notes']
            
            if isinstance(v,list):
                
                notes.extend(v)
            
            else:
                
                notes.append(v)
        
        else:
            
            annotated_table_dict[k]=v
    
    return annotated_table_dict
    
    
    
def annotate_column(
        annotated_column_dict,
        metadata_column_obj_dict,
        default_language,
        inherited_properties,
        ):
    """
    """
    # add inherited properties that were passed
    for k,v in inherited_properties.items():
        if k=='aboutUrl': k='aboutURL'
        if k=='propertyUrl': k='propertyURL'
        if k=='valueUrl': k='valueURL'
        if k in annotated_column_dict:
            annotated_column_dict[k]=v
    
    # include new inherited properties from metadata
    for name in get_inherited_properties_from_type('Column'):
        if name in metadata_column_obj_dict:
            inherited_properties[name]=metadata_column_obj_dict[name]
    
    # annotate this column
    for k,v in metadata_column_obj_dict.items():
        
        if k=='titles':
            x=[]
            for lang_code,titles in v.items():  # lang_code, list of titles
                for title in titles:
                    x.append(
                        {'@value':title,
                         '@language':lang_code
                            }
                        )
        
            annotated_column_dict[k]=x
        
        else:
        
            annotated_column_dict[k]=v
        
    # annotate cells
    cells=annotated_column_dict['cells']
    for i in range(len(cells)):
        cells[i]=annotate_cell(
            cells[i],
            inherited_properties
            )
    
    # If there is no name property defined on this column, the first titles 
    # value having the same language tag as default language, or und or if 
    # no default language is specified, becomes the name annotation for the 
    # described column. 
    # This annotation must be percent-encoded as necessary to conform to 
    # the syntactic requirements defined in [RFC3986].
    if annotated_column_dict['name'] is None:
        title=metadata_column_obj_dict['titles'][default_language][0]
        title=urllib.parse.quote(title.encode('utf8'))
        annotated_column_dict['name']=title
    
    
    return annotated_column_dict
    
    
def annotate_cell(
        annotated_cell_dict,
        inherited_properties
        ):
    """
    """    
    # add inherited properties that were passed
    for k,v in inherited_properties.items():
        if k=='aboutUrl': k='aboutURL'
        if k=='propertyUrl': k='propertyURL'
        if k=='valueUrl': k='valueURL'
        if k in annotated_cell_dict:
            annotated_cell_dict[k]=v
    
    
    
    return annotated_cell_dict
    


#%% Section 5.1.3 - URI Template Properties

def get_URI_from_URI_template(
        uri_template_string,
        annotated_cell_dict,
        table_path,
        table_url
        ):
    """
    """
    
    # If the supplied value of a URI template property is not a string 
    # (e.g. if it is an integer), compliant applications must issue a 
    # warning and proceed as if the property had been supplied with an empty string.
    
    # TO DO
    
    # URI template properties contain a [URI-TEMPLATE] which can be 
    # used to generate a URI.
    # These URI templates are expanded in the context of each row by 
    # combining the template with a set of variables with values as 
    # defined in [URI-TEMPLATE]. 
    
    # The following variables are set:
    variables={}
    
    # column names
    # a variable is set for each column within the schema; the name of the 
    # variable is the column name of the column from the annotated table 
    # and the value is derived from the value of the cell in that column 
    # in the row that is currently being processed, namely one of:
        # null,
        # the canonical representation of the value of the cell, based on 
        #  its datatype as defined in [xmlschema11-2], if it has a single value, or
        # a list of canonical representations of the values of the cell, 
        #  if it has a sequence value.
    
    for cell in annotated_cell_dict['row']['cells']:
        name=cell['column']['name']
        if not name is None:
            value=cell['value']
            if value is None:
                pass
            elif isinstance(value,list):
                value=[x['@value'] for x in value]
            else:
                value=value['@value'],
            variables[name]=value
            
    # _column
    # _column is set to the column number of the column from the annotated 
    # table that is currently being processed.
    variables['_column']=annotated_cell_dict['column']['number']
    
    # _sourceColumn
    #_sourceColumn is set to the source number of the column that is currently 
    # being processed; this usually varies from _column by skip columns.
    variables['_sourceColumn']=annotated_cell_dict['column']['sourceNumber']

    #_row
    #_row is set to the row number of the row from the annotated table that is 
    # currently being processed.
    variables['_row']=annotated_cell_dict['row']['number']

    #_sourceRow
    #_sourceRow is set to the source number of the row that is currently 
    # being processed; this usually varies from _row by skip rows and header rows.
    variables['_sourceRow']=annotated_cell_dict['row']['sourceNumber']

    #_name
    #_name is set to the URI decoded column name annotation, as defined 
    # in [tabular-data-model], for the column that is currently being 
    # processed. (Percent-decoding is necessary as name may have been encoded 
    # if taken from titles; this prevents double percent-encoding.)
    variables['_name']=urllib.parse.unquote(annotated_cell_dict['column']['name'])        
        
    # The annotation value is the result of:
        
    # 1 applying the template against the cell in that column in the row that 
    # is currently being processed.
    uri=uritemplate.expand(uri_template_string,
                           variables)
    
    # 2 expanding any prefixes as if the value were the name of a common 
    # property, as described in section 5.8 Common Properties.
    uri=get_expanded_prefixed_name(uri)
    
    # 3 resolving the resulting URL against the base URL of the table url if not null.
    
    # if absolute path
    if os.path.isabs(uri):
        return uri
    
    # if absolute url
    elif bool(urllib.parse.urlparse(uri).netloc): 
        return uri
    
    else: # if relative path or url, resolve against table path or base table
    
        if not table_path is None:
            
            return table_path + uri
        
        elif not table_url is None:

            # remove fragments            
            x=urllib.parse.urljoin(table_url, urllib.parse.urlparse(table_url).path) 

            return urllib.parse.urljoin(x,uri)  # NEED CHECKING
    
    
            
    

    
    
    

#%% Section 5.4.3 - Table Description Compatibility

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
    
        
    
#%% Section 5.5.1 - Schema Compatibility
    
def compare_schema_descriptions(
        TM_schema,
        EM_schema,
        validate=False
        ):
    """
    """
    #print(TM_schema)
    #print(EM_schema)
    
    # Two schemas are compatible if they have the same number of non-virtual 
    # column descriptions, and the non-virtual column descriptions at the 
    # same index within each are compatible with each other. 
    TM_columns=TM_schema.get('columns',[])
    EM_columns=EM_schema.get('columns',[])
    
    if not len(TM_columns)==len(EM_columns):
        raise Exception  # TO DO
        
    for i in range(len(TM_columns)):
        TM_column=TM_columns[i]
        EM_column=EM_columns[i]
    
        # Column descriptions are compatible under the following conditions:

        # If either column description has neither name nor titles properties.
        
        if not 'name' in TM_column and not 'titles' in TM_column:
            continue
        
        if not 'name' in EM_column and not 'titles' in EM_column:
            continue
        
        
        # If there is a case-sensitive match between the name properties of the columns.
        if 'name' in TM_column and 'name' in EM_column:
            if TM_column['name']==EM_column['name']:
                continue
        
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
            continue
              
                
        
        
        
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
                continue
        
        if (not 'name' in TM_column 
            and 'titles' in TM_column
            and 'name' in EM_column 
            and not 'titles' in EM_column
            ):
            if validate==False:
                continue

        raise Exception  # i.e. NOT COMPATIBLE - NEED TO FIX IF VALIDATING OR NOT

    
    
#%% Section 6 - (Metadata) Normalization

def normalize_metadata_from_file_path_or_url(
        metadata_file_path_or_url
        ):
    """
    """
    metadata_file_path, metadata_file_url=\
        get_path_and_url_from_file_location(
            metadata_file_path_or_url
            )
        
    if not metadata_file_path is None:
        
        metadata_file_text=\
            get_text_from_file_path(
                metadata_file_path)
        
        metadata_root_obj_dict=json.loads(metadata_file_text)
        
        normalized_metadata_object=\
            normalize_metadata_root_object(
                    metadata_root_obj_dict=metadata_root_obj_dict,
                    metadata_file_path=metadata_file_path, 
                    metadata_file_url=None
                    )
            
        headers=None
        
    elif not metadata_file_url is None:
    
        metadata_file_text, headers=\
            get_text_and_headers_from_file_url(
                metadata_file_url)
        
        metadata_root_obj_dict=json.loads(metadata_file_text)
        
        normalized_metadata_object=\
            normalize_metadata_root_object(
                    metadata_root_obj_dict=metadata_root_obj_dict,
                    metadata_file_path=None, 
                    metadata_file_url=metadata_file_url
                    )
    
    else:
        
        raise Exception
        
    return normalized_metadata_object, headers
    
    
    
def normalize_metadata_root_object(
        metadata_root_obj_dict,
        metadata_file_path, 
        metadata_file_url
        ):
    """Normalizes a CSVW metadata file.
    
    :param metadata_file_path_or_url: 
    :type metadata_file_path_or_url: str
    
    :returns: A normalized copy of the CSVW metadata.json file.
    :rtype: dict
    
    """
    
    base_path, base_url=\
        get_base_path_and_url_of_metadata_object(
            metadata_root_obj_dict,
            metadata_file_path,
            metadata_file_url
            )
        
    default_language=\
        get_default_language_of_metadata_object(
            metadata_root_obj_dict
            )
    
    return normalize_metadata_object(
        metadata_root_obj_dict,
        base_path,
        base_url,
        default_language
        )


def normalize_metadata_object(
        obj_dict,
        base_path,
        base_url,
        default_language
        ):
    """Normalizes a CSVW metadata object.
    
    This follows the procedure given in Section 6 of the 
    'Metadata Vacabulary for Tabular Data' W3C recomendation 
    https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/.
    
    :type obj_dict: dict
    
    :returns: A normalized copy of the CSVW metadata.json file.
    :rtype: dict
    """
    d={}
    
    for property_name, property_value in obj_dict.items():
        
        property_family=\
            get_property_family(property_name)
        property_type=\
            get_property_type(property_name)
            
        #print(property_name, property_family, property_type)
        
        normalized_value=\
            normalize_metadata_property(
                property_name,
                property_value,
                property_family,
                property_type,
                base_path,
                base_url,
                default_language       
                )
                
        d[property_name]=normalized_value
    
    return d
    

def normalize_metadata_property(
        property_name,
        property_value,
        property_family,
        property_type,
        base_path,
        base_url,
        default_language       
        ):
    """
    """
    
     # Following this normalization process, the @base and @language 
     #  properties within the @context are no longer relevant; the normalized 
     #  metadata can have its @context set to http://www.w3.org/ns/csvw.
    if property_name=='@context':
        
        normalized_value='http://www.w3.org/ns/csvw'
    
    
    # 1 If the property is a common property or notes the value must be 
    #  normalized as follows:
    elif property_family=='common property' or property_name=='notes':
        
        normalized_value=normalize_common_property(
            property_name,
            property_value,
            property_family,
            property_type,
            base_path,
            base_url,
            default_language     
            )
        
    # 2 If the property is an array property each element of the value is 
    #  normalized using this algorithm.
    elif property_type=='array property':
        
        normalized_value=[]
        for x in property_value:
            if isinstance(x,dict):
                normalized_value.append(
                    normalize_metadata_object(
                        x,
                        base_path,
                        base_url,
                        default_language
                        )
                    )
            else:
                raise Exception # what to do if not an object??
        
    # 3 If the property is a link property the value is turned into an 
    #  absolute URL using the base URL and normalized as described in 
    #  URL Normalization [tabular-data-model].
    elif property_type=='link property':
        
        normalized_value=\
            get_resolved_path_or_url_from_link_string(
                    property_value,
                    base_path,
                    base_url
                    )
        
    # 4 If the property is an object property with a string value, 
    #  the string is a URL referencing a JSON document containing a single 
    #  object. Fetch this URL to retrieve an object, which may have a 
    #  local @context. Normalize each property in the resulting object 
    #  recursively using this algorithm and with its local @context 
    #  then remove the local @context property. If the resulting object 
    #  does not have an @id property, add an @id whose value is the 
    #  original URL. This object becomes the value of the original 
    #  object property.
    elif property_type=='object property' and isinstance(property_value,str): 
    
        resolved_url=\
            get_resolved_path_or_url_from_link_string(
                    property_value,
                    base_path,
                    base_url
                    )
        
        # get normalised version of file at the resolved url
        obj_dict=\
            normalize_metadata_from_file_path_or_url(
                resolved_url
                )[0]
        
        # remove @context if it exists
        obj_dict.pop('@context',None)
        
        # add @id if it does not exist
        if not '@id' in obj_dict:
            obj_dict['@id']=property_value
            
        normalized_value=obj_dict
        
    
    # 5 If the property is an object property with an object value, 
    #  normalize each property recursively using this algorithm.
    elif property_type=='object property':
    
        normalized_value=\
            normalize_metadata_object(
                property_value,
                base_path,
                base_url,
                default_language
                )
    
    # 6 If the property is a natural language property and the value is 
    #  not already an object, it is turned into an object whose properties 
    #  are language codes and where the values of those properties are arrays. 
    #  The suitable language code for the values is determined through the 
    #  default language; if it can't be determined the language code und 
    #  must be used.
    elif property_type=='natural language property' and not isinstance(property_value,dict):
        
        if isinstance(property_value,str):
            x=[property_value]
        else:  # i.e. property value is an array
            x=property_value
        
        normalized_value={default_language: x}
    
    # 7 If the property is an atomic property that can be a string or an 
    #  object, normalize to the object form as described for that property.
    elif property_name=='format' and isinstance(property_value,str):
        normalized_value={'pattern':property_value}
    
    elif property_name=='datatype' and isinstance(property_value,str):
        normalized_value={'base':property_value}
    
    
    # otherwise...
    else:
        
        normalized_value=property_value
        
    return normalized_value

            
def normalize_common_property(
        property_name,
        property_value,
        property_family,
        property_type,
        base_path,
        base_url,
        default_language     
        ):
    """
    """
    
    # 1.1 If the value is an array, each value within the array is normalized 
    #  in place as described here.
    if isinstance(property_value,list):
        
        normalized_value=[normalize_common_property(
                    property_name,
                    x,
                    property_family,
                    property_type,
                    base_path,
                    base_url,
                    default_language     
                    ) 
                for x in property_value]
    
    # 1.2 If the value is a string, replace it with an object with a @value 
    #  property whose value is that string. If a default language is specified, 
    #  add a @language property whose value is that default language.
    elif isinstance(property_value,str):
        
        if default_language=='und':
        
            normalized_value={
                '@value': property_value
                }
    
        else:
            
            normalized_value={
                '@value': property_value,
                '@language': default_language
                }
    
    
    # 1.3 If the value is an object with a @value property, it remains as is.
    elif isinstance(property_value,dict) and '@value' in property_value:
        
        normalized_value=property_value
    
    # 1.4 If the value is any other object, normalize each property of that 
    #  object as follows:
    elif isinstance(property_value,dict):
        
        d={}
        
        for p1_name, p1_value in property_value.items():
        
            # 1.4.1 If the property is @id, expand any prefixed names and resolve 
            #  its value against the base URL.
            if p1_name=='@id':
                
                
                x=get_expanded_prefixed_name(
                    p1_value
                    )
                
                x=get_resolved_path_or_url_from_link_string(
                    x,
                    base_path,
                    base_url
                    )
        
            # 1.4.2 If the property is @type, then its value remains as is.
            elif p1_name=='@type':
                
                x=p1_value
            
            # 1.4.3 Otherwise, normalize the value of the property as if it were 
            #  a common property, according to this algorithm.
            else:
                
                x=normalize_common_property(
                    p1_name,
                    p1_value,
                    property_family,
                    property_type,
                    base_path,
                    base_url,
                    default_language     
                    )
            
            d[p1_name]=x
            
        normalized_value=d
    
    
    # 1.5 Otherwise, the value remains as is.
    else:
        
        normalized_value=property_value
    
    return normalized_value
    


#%% FUNCTIONS - Generating JSON from Tabular Data on the Web

#%% Section 4.2 Generating JSON

def get_minimal_json_from_annotated_table_group(
        annotated_table_group_dict
        ):
    """
    """
    # The steps in the algorithm defined here apply to minimal mode.
    
    # 1 Insert an empty array A into the JSON output. 
    #   The objects containing the name-value pairs associated with the cell 
    #   values will be subsequently inserted into this array.
    
    output=[]
    
    # 2 Each table is processed sequentially in the order they are referenced 
    #   in the group of tables. 
    #   For each table where the suppress output annotation is false:
    for annotated_table_dict in annotated_table_group_dict['tables']:
        
        if annotated_table_dict['supress_output']==False:
            
            # 2.1 Each row within the table is processed sequentially in order. 
            #     For each row in the current table:
            
            for annotated_row_dict in annotated_table_dict['rows']:
                
                # 2.1.1 Generate a sequence of objects, S1 to Sn, each of 
                #       which corresponds to a subject described by the 
                #       current row, as described in 4.3 Generating Objects.
                
                sequence_of_objects=generate_objects(
                    annotated_row_dict
                    )
                
                # 2.1.2 As described in 4.4 Generating Nested Objects, 
                # process the sequence of objects, S1 to Sn, to produce a 
                # new sequence of root objects, SR1 to SRm, that may 
                # include nested objects.
                
                sequence_of_root_objects=\
                    generate_nested_objects(
                        annotated_row_dict,
                        sequence_of_objects 
                        )
                
                output.append(sequence_of_root_objects)
                
    return output
        
#%% Section 4.3 Generating Objects

def generate_objects(
        annotated_row_dict
        ):
    """
    """
    
    sequence_of_objects=[]
    
    # The steps in the algorithm defined here apply to both standard and 
    # minimal modes.
        
    # This algorithm generates a sequence of objects, S1 to Sn, each of which 
    # corresponds to a subject described by the current row. 
    # The algorithm inserts name-value pairs into Si depending on the cell 
    # values as outlined in the following steps.
        
    # 1 Determine the unique subjects for the current row. 
    #   The subject(s) described by each row are determined according to the 
    #   about URL annotation for each cell in the current row. 
    #   A default subject for the row is used for any cells where about URL 
    #   is undefined.
        
    subjects=[]
    
    for annotated_cell_dict in annotated_row_dict['cells']:
        
        subject=annotated_cell_dict['aboutURL']
        if not subject in subjects:
            subjects.append(subject)
        
    # 2 For each subject that the current row describes where at least one 
    #   of the cells that refers to that subject has a value or value URL 
    #   that is not null, and is associated with a column where suppress 
    #   output annotation is false:
    
    for subject in subjects:
        
        for annotated_cell_dict in annotated_row_dict['cells']:
            
            cell_subject=annotated_cell_dict['aboutURL']
            
            if not annotated_cell_dict['value'] is None:
                cell_value_or_valueURL=annotated_cell_dict['value']
            else:
                cell_value_or_valueURL=annotated_cell_dict['valueURL']
            
            column_suppress_output=annotated_cell_dict['column']['supress_output']
            
            if (cell_subject==subject
                and cell_value_or_valueURL is not None
                and column_suppress_output==False):
                    
                # 2.1 Create an empty object Si to represent the subject i.
                
                #     (i is the index number with values from 1 to n, 
                #     where n is the number of subjects for the row)

                #     Subject i is identified according to the about URL 
                #     annotation of its associated cells: IS. 
                #     For a default subject where about URL is not 
                #     specified by its cells, IS is null.
        
                object_={}
                Is=cell_subject
                
                # 2.2 If the identifier for subject i, IS, is not null, 
                #     then insert the following name-value pair into object Si:
                #     name:@id; value:IS
                
                if not Is is None:
                    object_['@id']=Is
                    
                sequence_of_objects.append(object_)        
        
                break
    
    # 2.3 Each cell referring to subject i is then processed sequentially 
    # according to the order of the columns.
        
    for object_ in sequence_of_objects:
      
        for annotated_cell_dict in annotated_row_dict['cells']:
        
            cell_subject=annotated_cell_dict['aboutURL']
            
            column_suppress_output=annotated_cell_dict['column']['supress_output']

            # For each cell referring to subject i, where the suppress output 
            # annotation for the column associated with that cell is false, 
            # insert a name-value pair into object Si as described below:
            
            if (object_.get('@id',None)==cell_subject
                and column_suppress_output==False):
                
                # 2.3.1 If the value of property URL for the cell is not null, 
                #       then name N takes the value of property URL compacted 
                #       according to the rules as defined in URL Compaction 
                #       in [tabular-metadata].

                #       Else, name N takes the URI decoded value of the name 
                #       annotation for the column associated with the cell. 
                #       (URI decoding is necessary as name may have been 
                #       encoded if it was taken from a supplied title.)
                
                property_url=annotated_cell_dict['propertyURL']
                
                if not property_url is None:
                    
                    object_name=property_url
                    
                    raise Exception  # NEED TO COMPACT THIS
                    
                else:
                    
                    object_name=annotated_cell_dict['column']['name']
                    
                    raise Exception  # NEED TO DECODE THIS
                    
                # 2.3.2 If the value URL for the current cell is not null, 
                #       then insert the following name-value pair into 
                #       object Si:
                #       name:N, value:Vurl
                #
                # where Vurl is the value of value URL annotation for the 
                # current cell expressed as a string in the JSON output. 
                # If N is @type, compact Vurl according to the rules as 
                # defined in URL Compaction in [tabular-metadata].
                
                value_url=annotated_cell_dict['valueURL']
                cell_value=annotated_cell_dict['value']
                
                
                if not value_url is None:
                
                    if object_name=='@type':
                        
                        object_value=value_url
                        
                        raise Exception  # NEED TO DECODE THIS
                        
                    else:
                    
                        object_value=value_url
        
                
                # 2.3.3 Else, if the cell value is a list that is not empty, 
                #       then the cell value provides a sequence of values for 
                #       inclusion within the JSON output; insert an array Av 
                #       containing each value V of the sequence into object Si:
                #       name:N, value:Av
                #       Each of the values V derived from the sequence must 
                #       be expressed in the JSON output according to the 
                #       datatype of V as defined below in section 4.5 
                #       Interpreting datatypes.           
        
                elif isinstance(cell_value,list) and len(cell_value)>0:
                    
                    object_value=[x for x in cell_value]
                        
                        
                # 2.3.4 Else, if the cell value is not null, then the cell 
                #       value provides a single value V for inclusion within the 
                #       JSON output; insert the following name-value pair into 
                #       object Si:
                #       name:N,value:V
                #       Value V derived from the cell values must be expressed 
                #       in the JSON output according to the datatype of the 
                #       value as defined in section 4.5 Interpreting datatypes.
                        
                elif not cell_value is None:
                    
                    object_value=cell_value
                    
                # 2.4 If name N occurs more than once within object Si, 
                #     the name-value pairs from each occurrence of name N 
                #     must be compacted to form a single name-value pair with 
                #     name N and whose value is an array containing all values 
                #     from each of those name-value pairs. Where the value 
                #     from one or more contributing name-value pairs is of 
                #     type array, the values from contributing arrays are 
                #     included directly to the resulting array (i.e. arrays 
                #     of values are flattened).
                
                if object_name in object_:
                    
                    if not isinstance(object_[object_name],list):
                        
                        object_[object_name]=[object_[object_name]]
                    
                    if isinstance(object_value,list):
                    
                        object_[object_name].extend(object_value)
                    
                    else:
                    
                        object_[object_name].append(object_value)
                    
                else:
                
                    object_[object_name]=object_value
               
    return sequence_of_objects 
  

#%% Section 4.4 Generating Nested Objects

def generate_nested_objects(
        annotated_row_dict,
        sequence_of_objects 
        ):
    """
    """
    # The steps in the algorithm defined herein apply to both standard and 
    # minimal modes.
    
    # Where the current row describes multiple subjects, it may be possible 
    # to organize the objects associated with those subjects such that some 
    # objects are nested within others; e.g. where the value URL annotation 
    # for one cell matches the about URL annotation for another cell in the 
    # same row. 
    # This algorithm considers a sequence of objects generated according to
    # 4.3 Generating Objects, S1 to Sn, each of which corresponds to a 
    # subject described by the current row. 
    # It generates a new sequence of root objects, SR1 to SRm, that may 
    # include nested objects.

    # Where the current row describes only a single subject, this algorithm 
    # may be bypassed as no nesting is possible. 
    # In such a case, the root object SR1 is identical to the original 
    # object S1.
    
    if len(sequence_of_objects)==1:
        return sequence_of_objects
    
    # This nesting algorithm is based on the interrelationships between 
    # subjects described within a given row that are specified using the 
    # value URL annotation. 
    # Cell values expressing the identity of a subject in the current 
    # row (i.e., as a simple literal) will be ignored by this algorithm.
    
    # The nesting algorithm is defined as follows:
        
    # 1 For all cells in the current row, determine the value URLs, Vurl, 
    # that occur only once. 
    # The list of these uniquely occurring value URLs is referred to as 
    # the URL-list.
    
    url_list=[]
    cache=[]
    
    for annotated_cell_dict in annotated_row_dict['cells']:
        
        value_url=annotated_cell_dict['valueURL']
        
        if not value_url is None and not value_url in cache:
            
            if value_url in url_list:
                cache.append(value_url)
                url_list.remove(value_url)
            
            else:
                url_list.append(value_url)
        
    # 2 Create an empty forest F. Vertices in the trees of this forest 
    # represent the subjects described by the current row.
    forest=[]
    
    # 3 For each object Si in the sequence S1 to Sn:
        
    for object_ in sequence_of_objects:
        
        # 3.1 Determine the identity of object Si: IS. 
        #     If present in object Si, the name-value pair with name 
        #     @id provides the value of IS. 
        #     Else, object Si is not explicitly identified and IS is null.
    
        Is=object_.get('@id',None)
        
        # 3.2 Check whether there is a vertex N in forest F that represents 
        #     object Si. 
        
        N=None
        forest_nodes=[node for tree in forest for node in tree['nodes']]
        for node in forest_nodes:
            if node['object_']==object_:
                N=node
                break
                    
        #     If none of the existing vertices in forest F represent 
        #     object Si, then insert a new tree into forest F whose 
        #     root is a vertex N that represents object Si and has 
        #     identity IS.
        
        if N is None:
        
            tree=dict(
                nodes=[],
                edges=[]
                )
            N=dict(
                id_=Is,
                object_=object_,
                root=True,
                tree=tree,
                )
            tree['nodes'].append(N)
            forest.append(tree)
            
        # 3.3 For all cells associated with the current object Si (e.g. 
        #     whose about URL annotation matches IS):
        
        annotated_cell_dicts=[x for x in annotated_row_dict['cells']
                              if x['aboutURL']==Is]
        
        for annotated_cell_dict in annotated_cell_dicts:
            
            # 3.3.1 If the value URL annotation of the current cell is 
            #       defined and its value, Vurl, appears in the URL-list, 
            #       then check each of the other objects in the sequence S1 
            #       to Sn to determine if Vurl identifies one of those objects.
        
            value_url=annotated_cell_dict['valueURL']
            
            if not value_url is None and value_url in url_list:
                
                for object2_ in sequence_of_objects:
                    
                    if object2_==object_:
                        
                        continue
                
                    # For object Sj, if the name-value pair with name @id is 
                    # present and its value matches Vurl, then:
                
                    if '@id' in object2_ and object2_['@id']==value_url:
                        
                        # 3.3.1.1 If the root of the tree containing vertex N 
                        #         is a vertex that represents object Sj, then 
                        #         object Si is already a descendant of object 
                        #         Sj; no further action should be taken for 
                        #         this instance of Vurl.
        
                        tree=N['tree']
                        root=[x for x in tree['nodes'] if x['root']==True][0]
                        
                        if root['object_']==object2_:
                            
                            break 
                            
                        # 3.3.1.2 Else, if there is a root vertex M in forest 
                        #         F that represents object Sj, then set vertex 
                        #         M as a child of vertex N and remove vertex 
                        #         M from the list of roots in forest F (i.e., 
                        #         the tree rooted by M becomes a sub-tree of N).
                            
                        else:
                            
                            roots=[node for node in tree for tree in forest
                                   if node['root']==True]
                            Ms=[node for node in roots if 
                                node['object_']==object2_]
                            
                            if len(Ms)>0:
                                
                                M=Ms[0]
                                
                                M['root']=False
                                M['tree']=tree
                                tree['nodes'].append(M)
                                tree['edges'].append((N,M))
                                forest.remove(M['tree'])
                                
                            else:
                            
                                # 3.3.1.3 Else, create a new vertex M that 
                                #         represents object Sj as a child of 
                                #         vertex N.
    
                                M=dict(
                                    id_=object2_.get('@id',None),
                                    object_=object2_,
                                    root=False,
                                    tree=tree,
                                    )
                                tree['nodes'].append(M)
                                tree['edges'].append((N,M))
                                
    # 4 Each vertex in forest F represents an object in the original 
    #   sequence of objects S1 to Sn and is associated with a subject 
    #   described by the current row. 
    #   Rearrange objects S1 to Sn such that they mirror the structure 
    #   of the trees in forest F as follows: 
    #   - If vertex M, representing object Si, is a child of vertex N, 
    #     representing object Sj, then the name-value pair in object Sj 
    #     associated with the edge relating M and N must be modified 
    #     such that the (literal) value, Vurl, from that name-value pair 
    #     is replaced by object Si thus creating a nested object.
    
    forest_nodes=[node for tree in forest for node in tree['nodes']]
    forest_edges=[edge for tree in forest for edge in tree['edges']]
    
    for object_ in sequence_of_objects:
        
        M=[node for node in forest_nodes if node['object_']==object_][0]
        
        NM_edges=[edge for edge in forest_edges if edge[1]==M]
    
        if len(NM_edges)>0:
            
            NM_edge=NM_edges[0]
            
            N=NM_edge[0]
            
            N['object_']['valueURL']=M['object_']
            
    # 5 Return the sequence of root objects, SR1 to SRm.
    
    sequence_of_root_objects=[]
    
    roots=[node for node in forest_nodes if node['root']==True]
    
    for root in roots:
        
        sequence_of_root_objects.append(root['object_'])
        
    return sequence_of_root_objects
    
    
                        
#%% Section 4.5 Interpreting datatypes
   
# This is already done in the annotated table dictionary
# Cell values are stored as JSON objects there.


def interpret_datatype(
        datatype
        ):
    """
    """
    
    
    
    
    
    
    
    
    
#%% FUNCTIONS - General


def get_base_path_and_url_of_metadata_object(
        obj_dict,
        metadata_file_path,
        metadata_file_url
        ):
    """Returns the default language of the metadata object.
    
    :param url: The URL indicating the location of the metadata file.
    
    :raises ValueError: If no base url is present.
    
    :rtype: str
    
    """
    try:
        base_url_property_value=obj_dict['@context'][1]['@base']
    except (KeyError,IndexError,TypeError):
        base_url_property_value=None
    
    # check if absolute
    if not base_url_property_value is None:
        # if absolute path
        if os.path.isabs(base_url_property_value):
            return base_url_property_value, None
        # if absolute url
        if bool(urllib.parse.urlparse(base_url_property_value).netloc): 
            return None, base_url_property_value
    
    if not metadata_file_path is None:
        base_url=None
        metadata_file_dir=os.path.dirname(metadata_file_path)
        if not base_url_property_value is None:
            base_path=os.path.join(metadata_file_dir,
                                   base_url_property_value)
        else:
            base_path=metadata_file_dir
        
    elif not metadata_file_url is None:
        base_path=None
        if not base_url_property_value is None:
            base_url=urllib.parse.urljoin(metadata_file_url,
                                          base_url) # should this be base_url_property_value??
        else:
            base_url=urllib.parse.urljoin(metadata_file_url,
                                          '.')  
            
    else:
        base_path=None
        base_url=None
            
    return base_path, base_url
    
    
def get_common_properties_of_metadata_object(
        json_dict
        ):
    """Returns a list of the common properties in the object.
    
    :param json_dict: The metadata object. This has to include a '@type' property.
    :type json_dict: dict
    
    :raises KeyError: If '@type' property is not present.
    
    :rtype: list
    
    """
    d=json_dict
    type_=d['@type']
    top_level_properties=get_top_level_properties_from_type(type_)
    inherited_properties=get_inherited_properties_from_type(type_)
    required_properties=get_required_properties_from_type(type_)
    optional_properties=get_optional_properties_from_type(type_)
    
    all_properties=(
        top_level_properties
        +inherited_properties
        +required_properties
        +optional_properties
        )
    
    return [x for x in d if not x in all_properties]


def get_default_language_of_metadata_object(
        metadata_dict
        ):
    """Returns the default language of the metadata object.
    
    :returns: The language code as specified in the '@context' property.
        If not present then 'und' is returned.
    :rtype: str
    
    """
    try:
        return metadata_dict['@context'][1]['@language']
    except (KeyError,IndexError,TypeError):
        return 'und'

    
def get_expanded_prefixed_name(
        name
        ):
    """Returns the full, expanded version of a prefixed uri.
    
    :param name: The prefixed name.
    :type name: str
    
    :rtype: str
    
    """
    if ':' in name:
        x=name.split(':')
    else:
        x=name.split('%3A')  # this is the percent encoded version
        
    if len(x)==2:
        if x[0] in prefixes:
            return prefixes[x[0]]+x[1]
        else:
            return name
        
    else:
        return name
        

def get_inherited_properties_from_type(
        type_
        ):
    """Returns a list of inherited properties based on the schema
    
    :param type_: The @type property of a metadata object, i.e. 'TableGroup'
    :type type_: str
    
    :rtype: list
    
    """
    schema_name=get_schema_name_from_type(type_)
    schema=get_schema_from_schema_name(schema_name)
    
    for x in schema['allOf']:
        if x['$ref'].endswith('inherited_properties.schema.json'):
            return list(get_schema_from_schema_name(
                'inherited_properties.schema.json'
                )['properties'])
        
    return []


def get_optional_properties_from_type(
        type_
        ):
    """Returns a list of optional properties based on the schema
    
    :param type_: The @type property of a metadata object, i.e. 'TableGroup'
    :type type_: str
    
    :rtype: list
    
    """
    schema_name=get_schema_name_from_type(type_)
    schema=get_schema_from_schema_name(schema_name)
    
    required_properties=schema.get('required',[])
    
    x=[]
    for p in schema.get('properties',[]):
        if not p in required_properties:
            x.append(p)
    
    return x
    

def get_path_and_url_from_file_location(
        file_path_or_url
        ):
    """Returns separate absolute path and url from the supplied path or url .
    
    :param file_path_or_url: Either a) a relative local file path; b) 
        an absolute local file path; or c) a full URL
    :param file_path_or_url: str
    
    :returns: A tuple of (file_absolute_path, file_url).
        file_absolute_path is the absolute local file path, 
        or None if supplied value is a URL.
        file_url is the full URL, or None if supplied value is a local file path.
    
    :rtype: tuple
    
    """
    # is argument a local path or a url?
    try:
        
        with open(file_path_or_url):
            
            file_absolute_path=os.path.abspath(file_path_or_url)
            file_url=None
            
    except (OSError,FileNotFoundError):
        
        file_absolute_path=None
        file_url=file_path_or_url

    return file_absolute_path, file_url


def get_property_family(
        property_name,
        ):
    """
    """
    if property_name in top_level_properties:
        return 'top level property'
    elif property_name in inherited_properties:
        return 'inherited property'
    elif property_name in all_optional_and_required_properties:
        return 'optional or required property'
    else:
        return 'common property'
    
    
def get_property_type(
        property_name,
        ):
    """
    """
    try:
        return all_properties[property_name]['$comment']
    except KeyError:
        return None
    

def get_required_properties_from_type(
        type_
        ):
    """Returns a list of required properties based on the schema
    
    :param type_: The @type property of a metadata object, i.e. 'TableGroup'
    :type type_: str
    
    :rtype: list
    
    """
    schema_name=get_schema_name_from_type(type_)
    schema=get_schema_from_schema_name(schema_name)
    
    return schema.get('required',[])
    

def get_resolved_path_or_url_from_link_string(
        link_string,
        base_path,
        base_url
        ):
    """
    """
    
    # if absolute path, return the original link string
    if os.path.isabs(link_string):
        return link_string
    
    # if absolute url, return the original link string
    elif bool(urllib.parse.urlparse(link_string).netloc): 
        return link_string
    
    else: # if relative path or url, resolve against base path or base url
    
        if not base_path is None:
            return os.path.join(base_path,link_string)
        elif not base_url is None:
            return urllib.parse.urljoin(base_url,link_string)
    
    
def get_schema_from_schema_name(
        schema_name
        ):
    """Returns a csvw metatdata json schema.
    
    :param schema_name: The file name for the schema i.e. 'table_description.schema.json'
    :type schema_name: str
    
    :returns: The json schema as a Python dictionary.
    :rtype: dict
    
    """
    return schemas[schema_name]
    
    
def get_schema_name_from_type(
        type_
        ):
    """Returns the json schema name (i.e. file name) for a given description type.
    
    :param type_: The @type property of a metadata object, i.e. 'TableGroup'
    :type type_: str
    
    :raises KeyError: If type_ is not a recognised csvw metadata @type.
    
    :rtype: str
    
    """
    d={
       'TableGroup':'table_group_description.schema.json',
       'Table':'table_description.schema.json',
       'Schema':'schema_description.schema.json',
       'Column':'column_description.schema.json',
       'Dialect':'dialect_description.schema.json',
       'Template':'transformation_definition.schema.json',
       'Datatype':'datatype_description.schema.json'
       }
    return d[type_]


def get_text_from_file_path(
        file_path,
        encoding=None
        ):
    """
    """
    with open(file_path, encoding=None) as f:
        return f.read()
    

def get_text_and_headers_from_file_url(
        file_url,
        encoding=None
        ):
    """
    
    """
    response = requests.get(file_url, stream=True)
    
    # apply encoding if specified
    if not encoding is None:
        response.encoding = encoding  # NEEDS TESTING
    
    return response.text, response.headers


def get_text_line_generator_from_path_or_url(
        file_path, 
        file_url,
        encoding=None
        ):
    """
    """
    if not file_path is None:
        
        for line in open(file_path, 
                         encoding=encoding):
            yield line
            
        
    elif not file_url is None:
        
        response = requests.get(file_url, stream=True)
        
        # apply encoding if specified
        if not encoding is None:
            response.encoding = encoding  # NEEDS TESTING
        
        for line in response.iter_lines():
            yield line.decode()
        
    else:
        raise Exception
     
    

    


def get_top_level_properties_from_type(
        type_
        ):
    """Returns a list of top level properties based on the schema
    
    :param type_: The @type property of a metadata object, i.e. 'TableGroup'
    :type type_: str
    
    :rtype: list
    
    """
    schema_name=get_schema_name_from_type(type_)
    schema=get_schema_from_schema_name(schema_name)
    
    for x in schema['allOf']:
        if x['$ref'].endswith('top_level_properties.schema.json'):
            return list(get_schema_from_schema_name(
                'top_level_properties.schema.json'
                )['properties'])
        
    return []

    
def get_type_of_metadata_object(
        json_dict
        ):
    """Returns the type of the metadata object (TableGroup, Table etc.)
    
    This is inferred from the properties of the object.
    
    Only works if the object has either a @type property or if the object
        can be recognised through a required property.
    
    :param json_dict: The metadata object
    :type json_dict: dict
    
    :raises ValueError: If the type cannot be inferred from the object.    
    
    :rtype: str
    
    """
    d=json_dict
    
    # If object already contains the @type property
    
    if '@type' in d:
        
        return d['@type']
    
    # TableGroup
    
    if 'tables' in d:
        
        return 'TableGroup'
    
    elif 'url' in d:
        
        return 'Table'
    
    else:
        
        raise ValueError
    

def merge_metadata_objs(
        obj1,        
        obj2
        ):
    """
    """
    # not sure if this will handle all cases
    # i.e. an existing titles list, to whcih further (embedded) titles are added
    
    if isinstance(obj2,dict):
        
        if not isinstance(obj1,dict):
            
            return obj1
        
        else:
            
            for k, v in obj2.items():
                
                if k in obj1:
                    
                    obj1[k]=merge_metadata_objs(
                        obj1[k],
                        v
                        )
        
                else:
                    
                    obj1[k]=v
                    
            return obj1
        
    elif isinstance(obj2,list):
        
        if isinstance(obj1,list):
            
            x=[]            
            
            for i in range(len(obj2)):
                
                x.append(merge_metadata_objs(
                    obj1[i],
                    obj2[i]
                    ))
                
            return x
                
        else:
            
            return obj1
    
    else:
        
        return obj1
        
    
    
    

