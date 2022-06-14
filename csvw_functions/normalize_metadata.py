# -*- coding: utf-8 -*-


from . import shared_functions


def normalize_metadata_file(
        metadata_file_path_or_url
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
    
    metadata_file_path, metadata_file_url=\
        shared_functions.get_path_and_url_from_file_location(
            metadata_file_path_or_url
            )
    
    obj_dict=\
        shared_functions.get_obj_dict_from_path_or_url(
            metadata_file_path, 
            metadata_file_url
            )
    
    base_path, base_url=\
        shared_functions.get_base_path_and_url_of_metadata_object(
            obj_dict,
            metadata_file_path,
            metadata_file_url
            )
    
    default_language=\
        shared_functions.get_default_language_of_metadata_object(obj_dict)
    
    return normalize_metadata_object(
        obj_dict,
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
            shared_functions.get_property_family(property_name)
        property_type=\
            shared_functions.get_property_type(property_name)
            
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
            shared_functions.get_resolved_path_or_url_from_link_string(
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
            shared_functions.get_resolved_path_or_url_from_link_string(
                    property_value,
                    base_path,
                    base_url
                    )
        
        # get normalised version of file at the resolved url
        obj_dict=\
            normalize_metadata_file(
                resolved_url
                )
        
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
                
                
                x=shared_functions.get_expanded_prefixed_name(
                    p1_value
                    )
                
                x=shared_functions.get_resolved_path_or_url_from_link_string(
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
    
    
    
    
    
    
    
    
    
    
    
