# -*- coding: utf-8 -*-


import json
import pkg_resources
import urllib.parse
import os
import requests
import csv


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
        validate=False
        ):
    """
    """
    
    check_metadata_document(
            metadata_table_group_obj_dict
            )
    
    
    # Compliant applications must ignore properties (aside from common 
    # properties) which are not defined in this specification and must 
    # generate a warning when they are encoutered.
    
    # NEEDS COMPLETING
    
   
    
    
    
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
    
    
    
    # 
    
    
    
    
    
    # inherited properties
    
    
    # annotate each table
    x=[]
    for i in range(len(annotated_table_group_dict['tables'])):
        y=annotate_table(
            annotated_table_group_dict['tables'][i],
            metadata_table_group_dict['tables'][i]
            )
        x.append(y)
    annotated_table_group_dict['tables']=x

    return annotated_table_group_dict



def annotate_table(
        annotated_table_dict,
        metadata_table_obj_dict,
        validate=False
        ):
    """
    """
    
    
def annotate_column(
        annotated_column_dict,
        metadata_column_obj_dict
        ):
    """
    """
    
    
def annotate_row(
        ):
    """
    """
    
    
def annotate_cell(
        ):
    """
    """    
    

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
        
        elif not 'name' in EM_column and not 'titles' in EM_column:
            continue
        
        
        # If there is a case-sensitive match between the name properties of the columns.
        elif 'name' in TM_column and 'name' in EM_column:
            if TM_column['name']==EM_column['name']:
                continue
        
        # If there is a non-empty case-sensitive intersection between the 
        # titles values, where matches must have a matching language; und 
        # matches any language, and languages match if they are equal when 
        # truncated, as defined in [BCP47], to the length of the shortest language tag.
        
        # NOT YET IMPLEMENTED
        
        # If not validating, and one schema has a name property but not a 
        # titles property, and the other has a titles property but not a name property.

        # NOT YET IMPLEMENED

        else:
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
        
        normalized_metadata_object=\
            normalize_metadata_from_file_path(
                metadata_file_path
                )
        headers=None
        
    elif not metadata_file_url is None:
    
        normalized_metadata_object, headers=\
            normalize_metadata_from_file_url(
                metadata_file_url
                )
    
    else:
        
        raise Exception
        
    return normalized_metadata_object, headers
    
    
    

def normalize_metadata_from_file_path(
        metadata_file_path
        ):
    """
    """
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

    return normalized_metadata_object


def normalize_metadata_from_file_url(
        metadata_file_url
        ):
    """
    """
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
    
    return normalized_metadata_object, headers
    
    
def normalize_metadata_root_object(
        metadata_root_obj_dict,
        metadata_file_path, 
        metadata_file_url
        ):
    """Normalizes a CSVW metadata file.
    
    This follows the procedure given in Section 6 of the 
    'Metadata Vacabulary for Tabular Data' W3C recomendation 
    https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/.
    
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
    

#%% FUNCTIONS - Model for Tabular Data and Metadata

#%% Section 5 - Locating Metadata

def get_embedded_metadata_from_csv_file(
        csv_file_path_or_url
        ):
    """
    """
    csv_file_path, csv_file_url=\
        get_path_and_url_from_file_location(
            csv_file_path_or_url
            )
    
    csv_text_line_generator=get_text_line_generator_from_path_or_url(
        csv_file_path, 
        csv_file_url
        )  
    
    column_titles=get_column_titles_of_csv_file_text_line_generator(
        csv_text_line_generator
        )
    column_description_objects=[{'titles':[column_title]}
                                for column_title in column_titles]
    
    schema_description_object={
        'columns': column_description_objects
        }
    
    table_description_object={
        '@context': "http://www.w3.org/ns/csvw",
        '@type': 'Table', 
        'url': csv_file_path or csv_file_url,
        'tableSchema': schema_description_object
        }
    
    return table_description_object
    

#%% Section 6.1 - Creating Annotated Tables

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
        
        annotated_tables_dict=\
            create_annotated_tables_from_metadata_file_path(
                metadata_file_path
                )
        
    elif not metadata_file_url is None:
    
        annotated_tables_dict=\
            create_annotated_tables_from_metadata_file_url(
                metadata_file_url
                )
    
    else:
        
        raise Exception
        
    return annotated_tables_dict


def create_annotated_tables_from_metadata_file_path(
        metadata_file_path
        ):
    """
    """
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
        
    return annotated_tables_dict
    
    
def create_annotated_tables_from_metadata_file_url(
        metadata_file_url
        ):
    """
    """
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
        
    return annotated_tables_dict


def create_annotated_tables_from_metadata_root_object(
        metadata_root_obj_dict,
        metadata_file_path,
        metadata_file_url,
        validate=False
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
    #print(metadata_table_group_dict)
    
    # 3 For each table (TM) in UM in order, create one or more annotated tables:
    
    for table_index,metadata_table_obj_dict in \
        enumerate(metadata_table_group_obj_dict['tables']):
            
        table_name=f'TG1T{table_index+1}'
        
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
                dialect_description_obj_dict,
                table_name
                )
        
        
        
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
        
        
    # 3.6 Use the metadata TM to add annotations to the tabular data model 
    #     T as described in Section 2 Annotating Tables in [tabular-metadata].
    
    annotated_table_group_dict=\
        annotate_table_group(
            annotated_table_group_dict,
            metadata_table_group_obj_dict
            )
        
        
    return annotated_table_group_dict
     

#%% Section 6.4 - Parsing Cells

def parse_cell(
        cell_string_value
        ):
    """
    """
    
    # The process of parsing the string value into a single value or a list of values is as follows:
        
    # 1 unless the datatype base is string, json, xml, html or anyAtomicType, replace all carriage return (#xD), line feed (#xA), and tab (#x9) characters with space characters.
    
    # etc.








#%% Section 8 - Parsing Tabular Data

def parse_tabular_data_from_text(
        tabular_data_text,
        tabular_data_file_path_or_url,
        dialect_description_obj_dict,
        table_name):
    """
    """
    # 8. Parsing Tabular Data
    
    # ... hard coded the defaults here...
    
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


    # # splits the text according to the supplied line_terminators
    # x=tabular_data_text
    # for terminator in line_terminators:
    #     x=x.replace(terminator,'\n')
    # tabular_data_lines=x.split('\n')
    
    
    # # create a generator
    # tabular_data_lines_generator=(x for x in tabular_data_lines)
    
    
    # reads the lines using csv.reader
    # csv_reader=csv.reader(tabular_data_lines,
    #                       delimiter=delimiter,
    #                       doublequote=True,  # the default
    #                       escapechar=escape_character,
    #                       quoting=csv.QUOTE_MINIMAL,  # the default
    #                       quotechar=quote_character,
    #                       skipinitialspace=False,  # the default
    #                       strict=False ## the default
    #                       )


    # 6 Repeat the following the number of times indicated by skip rows:
    
    for _ in range(skip_rows): 
        
        # 6.1 Read a row to provide the row content.
        character_index, row_content=\
            get_row_content(
                tabular_data_text,
                character_index,
                escape_character,
                quote_character,
                line_terminators
                )
        
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
            metadata_dict['tableSchema']['columns']=\
                [{'title':[],
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
                # cell values to the array at M.tableSchema.columns[i].title.
                metadata_dict['tableSchema']['columns'][i]['title'].append(
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
                    table=table_name, 
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
                        table=table_name,
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
                    table=table_name,
                    column=f'{table_name}C{i+1}',
                    row=f'{table_name}R{row_number}',
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
        
    return table_dict, metadata_dict
    
    

def get_row_content(
        tabular_data_text,
        i,
        escape_character,
        quote_character,
        line_terminators
        ):
    """
    """
    # print(tabular_data_text)
    # print(i)
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
    


    
# def read_row(
#         csv_reader,
#         trim
#         ):
#     """
#     """
#     row_list=next(csv_reader)
    
#     if trim==True:
#         row_list=[x.strip() for x in row_list]
#     elif trim==False:
#         pass
#     elif trim=='true':
#         row_list=[x.strip() for x in row_list]
#     elif trim=='false':
#         pass
#     elif trim=='start':
#         row_list=[x.lstrip() for x in row_list]
#     elif trim=='end':
#         row_list=[x.rstrip() for x in row_list]
    
#     return row_list
    

#%% FUNCTIONS - General

def add_types_to_metadata(
        json_dict
        ):
    """Adds '@type' properties to the object and its child objects.
    
    For example, adds the name:value pair `"@type": "Column"` to any Column objects.
    
    :param json_dict: The metadata object
    :type json_dict: dict
    
    """
    
    d=json_dict
    
    if isinstance(d,dict):
        
        if not '@type' in d:
            try:
                d['@type']=get_type_of_metadata_object(d)
            except ValueError:
                pass
        
        d1={}
        
        for k,v in d.items():
            
            if k=='tables':
                
                for x in v:
                    x['@type']='Table'
                
            elif k=='dialect':
                v['@type']='Dialect'
                
            elif k=='transformations':
                v['@type']='Template'
                
            elif k=='tableSchema':
                v['@type']='Schema'
                
            elif k=='columns':
                for x in v:
                    x['@type']='Column'
                    
            elif k=='datatype':
                v['@type']='Datatype'
                
                
            d1[k]=add_types_to_metadata(d[k])
            
        return d1
        
    elif isinstance(d,list):
        
        return list(add_types_to_metadata(x) for x in d)
        
    else:
    
        return d
    
    
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
                                          base_url)
        else:
            base_url=urllib.parse.urljoin(metadata_file_url,
                                          '.')  
            
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
     
    
def get_default_language_of_metadata_object(
        obj_dict
        ):
    """Returns the default language of the metadata object.
    
    :rtype: str
    
    """
    try:
        return obj_dict['@context'][1]['@language']
    except (KeyError,IndexError,TypeError):
        return 'und'
    
    

    

    
    
def get_expanded_prefixed_name(
        name
        ):
    """
    """
    x=name.split(':')
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






# def get_obj_dict_from_path_or_url(
#         metadata_file_path, 
#         metadata_file_url
#         ):
#     """
#     """
#     text=get_text_from_path_or_url(
#         metadata_file_path, 
#         metadata_file_url
#         )
    
#     return json.loads(text)
    
    

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
    """
    """
    
    # is argument a local path or a url?
    try:
        with open(file_path_or_url):
            file_path=os.path.abspath(file_path_or_url)
            #metadata_file_dir=os.path.dirname(metadata_file_path)
            file_url=None
    except (OSError,FileNotFoundError):
        file_path=None
        file_url=file_path_or_url

    return file_path, file_url


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
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

