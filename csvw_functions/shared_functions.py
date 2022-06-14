# -*- coding: utf-8 -*-


import json
import pkg_resources
import urllib.parse
import os
import requests
import csv


schema_files=[
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


schemas={}
for schema_file in schema_files:
    resource_package = __name__
    resource_path = '/'.join(('metadata_schema_files', schema_file))  
    data = pkg_resources.resource_string(resource_package, resource_path)
    json_dict=json.loads(data)
    schemas[schema_file]=json_dict
    
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
   


def get_text_from_path_or_url(
        file_path, 
        file_url
        ):
    """
    """
    if not file_path is None:
        
        with open(file_path) as f:
            return f.read()
    
    elif not file_url is None:
        
        response = requests.get(file_url)
        return response.text
        
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
    
    
def get_embedded_metadata_from_csv_file(
        csv_file_path_or_url
        ):
    """
    """
    csv_file_path, csv_file_url=\
        get_path_and_url_from_file_location(
            csv_file_path_or_url
            )
    
    csv_text=get_text_from_path_or_url(
        csv_file_path, 
        csv_file_url
        )  # this will load all the csv text which might slow things down...
    
    column_titles=get_column_titles_of_csv_file_text(
        csv_text
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
    
    
    
    
def get_column_titles_of_csv_file_text(
        text
        ):
    """
    """
    reader=csv.reader(text.split('\n'))
    first_row=next(reader)
    return first_row
    
    
    
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


def get_obj_dict_from_path_or_url(
        metadata_file_path, 
        metadata_file_url
        ):
    """
    """
    text=get_text_from_path_or_url(
        metadata_file_path, 
        metadata_file_url
        )
    
    return json.loads(text)
    
    
    
    


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
    except FileNotFoundError:
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
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

