# csvw_functions

Python implementation of the CSV on the Web (CSVW) standards.

## Contents

[About](#about) | 
[Installation](#installation) | 
[Issues, Questions?](#issues-questions) | 
[Quick Start](#quick-start) | 
[API](#api) | 
[Developer Notes](#developer-notes)

## About

This is a Python package which implements the following W3C standards:

- [Model for Tabular Data and Metadata on the Web](https://www.w3.org/TR/2015/REC-tabular-data-model-20151217/)
- [Metadata Vocabulary for Tabular Data](https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/)
- [Generating JSON from Tabular Data on the Web](https://www.w3.org/TR/2015/REC-csv2json-20151217/)
- [Generating RDF from Tabular Data on the Web](https://www.w3.org/TR/2015/REC-csv2rdf-20151217/)

These standards together comprise the CSV on the Web (CSVW) standards.

Further information on CSVW is available from:

- [CSV on the Web: A Primer](https://www.w3.org/TR/tabular-data-primer/)
- [csvw.org](https://csvw.org/)
- [www.stevenfirth.com/tag/csvw/](https://www.stevenfirth.com/tag/csvw/)

The package is written as pure Python and passes all the required tests in the [CSVW Test Suite](https://w3c.github.io/csvw/tests/):

- CSVW JSON tests: passes 270 / 270
- CSVW RDF tests: passes 270 / 270
- CSVW Validation tests: passes 282 / 282

## Installation

Source code available on GitHub here: https://github.com/stevenkfirth/csvw_functions

Install from GitHub using command: `pip install git+https://github.com/stevenkfirth/csvw_functions` 

The csvw_functions package uses the following external packages: *requests, json, os, urllib, warnings, hyperlink, uritemplate, langcodes, datetime, re, uuid, base64, dateutil, copy*. These packages are available as part of the Python standard library, part of the Anaconda distribution (recommended) and / or will need to be installed separately. 

## Issues, Questions?

The CSVW standards represent a complex set of operations and there are likely to be a 
number of situations where things don't work as expected. When this happens...

Raise an issue on GitHub: https://github.com/stevenkfirth/csvw_functions/issues

Email the author: [Steven Firth](https://www.lboro.ac.uk/departments/abce/staff/steven-firth/), s.k.firth@lboro.ac.uk


## Quick Start

### Access embedded metadata from CSV file

Let's say we have a CSV file with the contents...

```
# countries.csv
"country","country group","name (en)","name (fr)","name (de)","latitude","longitude"
"at","eu","Austria","Autriche","Österreich","47.6965545","13.34598005"
"be","eu","Belgium","Belgique","Belgien","50.501045","4.47667405"
"bg","eu","Bulgaria","Bulgarie","Bulgarien","42.72567375","25.4823218"
```

...and we'd like to extract information from the column headers in the form of a CSVW metadata JSON object. We would use the [`get_embedded_metadata`](#get_embedded_metadata) function:

```python
>>> import csvw_functions
>>> embedded_metadata = csvw_functions.get_embedded_metadata(
        'countries.csv',
        relative_path=True  # this means that the `url` property will contain a relative file path
        )
>>> print(embedded_metadata)
```
```json
{
    "@context": "http://www.w3.org/ns/csvw",
    "tableSchema": {
        "columns": [
            {
                "titles": {
                    "und": [
                        "country"
                    ]
                },
                "name": "country"
            },
            {
                "titles": {
                    "und": [
                        "country group"
                    ]
                },
                "name": "country%20group"
            },
            {
                "titles": {
                    "und": [
                        "name (en)"
                    ]
                },
                "name": "name%20%28en%29"
            },
            {
                "titles": {
                    "und": [
                        "name (fr)"
                    ]
                },
                "name": "name%20%28fr%29"
            },
            {
                "titles": {
                    "und": [
                        "name (de)"
                    ]
                },
                "name": "name%20%28de%29"
            },
            {
                "titles": {
                    "und": [
                        "latitude"
                    ]
                },
                "name": "latitude"
            },
            {
                "titles": {
                    "und": [
                        "longitude"
                    ]
                },
                "name": "longitude"
            }
        ]
    },
    "url": "countries.csv"
}
```

(This example is taken from Section 1.3 of the CSVW Primer: https://www.w3.org/TR/tabular-data-primer/#column-info. Note the differences here including the addition of the `name` property and the `titles` property given as a list of undefined ('und') language strings.) 


### Convert CSVW file to JSON-LD

Let's say we have a CSVW metadata JSON file which references the countries.csv file...

```json
{
  "@context": "http://www.w3.org/ns/csvw",
  "url": "countries.csv",
  "tableSchema": {
    "columns": [{
      "titles": "country"
    },{
      "titles": "country group"
    },{
      "titles": "name (en)",
      "lang": "en"
    },{
      "titles": "name (fr)",
      "lang": "fr"
    },{
      "titles": "name (de)",
      "lang": "de"
    },{
      "titles": "latitude",
      "datatype": "number"
    },{
      "titles": "longitude",
      "datatype": "number"
    }]
  }
}

```

... and we'd like to convert this to a dictionary in the form of JSON-LD data. Here we would use the [`create_annotated_table_group`](#create_annotated_table_group) and [`create_json_ld`](#create_json_ld) functions:

```python
>>> import csvw_functions
>>> annotated_table_group_dict = csvw_functions.create_annotated_table_group(
        input_file_path_or_url = 'countries-metadata.json'
        )
>>> json_ld = csvw_functions.create_json_ld(
        annotated_table_group_dict,
        mode='minimal'
        )
>>> print(json_ld)
```
```json
[
    {
        "country": "at",
        "country group": "eu",
        "name (en)": "Austria",
        "name (fr)": "Autriche",
        "name (de)": "\u00d6sterreich",
        "latitude": 47.6965545,
        "longitude": 13.34598005
    },
    {
        "country": "be",
        "country group": "eu",
        "name (en)": "Belgium",
        "name (fr)": "Belgique",
        "name (de)": "Belgien",
        "latitude": 50.501045,
        "longitude": 4.47667405
    },
    {
        "country": "bg",
        "country group": "eu",
        "name (en)": "Bulgaria",
        "name (fr)": "Bulgarie",
        "name (de)": "Bulgarien",
        "latitude": 42.72567375,
        "longitude": 25.4823218
    }
]
```

(This example is taken from Section 4.2 of the CSVW Primer: https://www.w3.org/TR/tabular-data-primer/#transformation-values. Note here that the 'Ö' character is replaced by its Unicode equivalent.)

### Convert CSVW file to RDF

Let's say we have the CSVW metadata JSON file and CSV file from the previous example, and we'd like to convert these to RDF data in Turtle notation. Now we would use the [`create_annotated_table_group`](#create_annotated_table_group) and [`create_rdf`](#create_rdf) functions:

```python
>>> import csvw_functions
>>> from rdflib import Graph
>>> annotated_table_group_dict = csvw_functions.create_annotated_table_group(
        input_file_path_or_url = 'countries-metadata.json'
        )
>>> rdf_ntriples = csvw_functions.create_rdf( 
        annotated_table_group_dict,
        mode = 'minimal',
        local_path_replacement_url='http://example.org'  # use in place of the local file path.
        )
>>> rdf_ttl = Graph().parse(data = rdf_ntriples, format='ntriples').serialize(format = "ttl")
>>> print(rdf_ttl)  
```
```
@prefix ns1: <http://example.org/countries.csv#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

[] ns1:country "bg"^^xsd:string ;
    ns1:country%20group "eu"^^xsd:string ;
    ns1:latitude 4.272567e+01 ;
    ns1:longitude 2.548232e+01 ;
    ns1:name%20%28de%29 "Bulgarien"@de ;
    ns1:name%20%28en%29 "Bulgaria"@en ;
    ns1:name%20%28fr%29 "Bulgarie"@fr .

[] ns1:country "be"^^xsd:string ;
    ns1:country%20group "eu"^^xsd:string ;
    ns1:latitude 5.050104e+01 ;
    ns1:longitude 4.476674e+00 ;
    ns1:name%20%28de%29 "Belgien"@de ;
    ns1:name%20%28en%29 "Belgium"@en ;
    ns1:name%20%28fr%29 "Belgique"@fr .

[] ns1:country "at"^^xsd:string ;
    ns1:country%20group "eu"^^xsd:string ;
    ns1:latitude 4.769655e+01 ;
    ns1:longitude 1.334598e+01 ;
    ns1:name%20%28de%29 "Österreich"@de ;
    ns1:name%20%28en%29 "Austria"@en ;
    ns1:name%20%28fr%29 "Autriche"@fr .
```

(This example is taken from Section 4.2 of the CSVW Primer: https://www.w3.org/TR/tabular-data-primer/#transformation-values. Note here that 'http://example.org' is used as a sample namespace for the predicates.)

## API 

### get_embedded_metadata

Description: This function reads a CSV file and returns any embedded metadata extracted from the CSV file. This is a useful thing to do if you only have a CSV file and want to create an initial version of its CSVW metadata JSON object. The standard approach to extracting metadata is described in [Section 8. Parsing Tabular Data](https://www.w3.org/TR/2015/REC-tabular-data-model-20151217/#parsing) of the *Model for Tabular Data and Metadata on the Web* standard.

Call signature:

```python
csvw_functions.get_embedded_metadata(
        input_file_path_or_url,
        relative_path=False,
        nrows=None,
        parse_tabular_data_function=parse_tabular_data_from_text_non_normative_definition,
        )
```

Arguments:

- **input_file_path_or_url** *(str)*: This argument is passed to the [`create_annotated_table_group`](#create_annotated_table_group) function.
- **relative_path** *(bool)*: OPTIONAL. If `True`, then any absolute file paths in the returned dictionary are replaced by local file paths. Only applicable if the CSV file is a file path (not a url). Default is `False`.
- **nrows** *(int or None)*: OPTIONAL. This argument is passed to the [`create_annotated_table_group`](#create_annotated_table_group) function.
- **parse_tabular_data_function** *(Python function)*: OPTIONAL. This argument is passed to the [`create_annotated_table_group`](#create_annotated_table_group) function.

Returns: The embedded metadata of a CSV file in the form of a CSVW metadata JSON object.

Return type: dict


### create_annotated_table_group

```python
csvw_functions.create_annotated_table_group(
        input_file_path_or_url,
        overriding_metadata_file_path_or_url=None,
        validate=False,
        parse_tabular_data_function=parse_tabular_data_from_text_non_normative_definition,
        _link_header=None,  
        _well_known_text=None,  
        _save_intermediate_and_final_outputs_to_file=False,  
        _print_intermediate_outputs=False  
        )
```
Description: This function reads either a CSVW metadata file or a CSV file with no metadata and converts it to an Annotated Tablular Data Model as defined in [Section 4. Tabular Data Models](https://www.w3.org/TR/2015/REC-tabular-data-model-20151217/#model) of the the *Model for Tabular Data and Metadata on the Web* standard. In essence this function combines the data from the CSVW metadata file and the CSV file into a single object (here as a Python dictionary) and checks for errors as this is done.

Arguments:
- **input_file_path_or_url** *(str)*: The relative file path, absolute file path or url to either a CSVW metadata document or a CSV file.
- **overriding_metadata_file_path_or_url** *(str)*: OPTIONAL. The relative file path, absolute file path or url to a metadata.json file to be used as Overriding Metadata as described in  [Section 5.1: Overriding Metadata](https://www.w3.org/TR/2015/REC-tabular-data-model-20151217/#overriding-metadata) of the *Model for Tabular Data and Metadata on the Web* standard.
- **validate** *(bool)*: OPTIONAL. If `True` then the process is run as a [validator](https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/#dfn-validator) and any validation errors will be raised. 
- **parse_tabular_data_function** *(Python function)*: OPTIONAL. This is the Python function which is used to parse the CSV file. In the csvw_functions package, the method described in [Section 8. Parsing Tabular Data](https://www.w3.org/TR/2015/REC-tabular-data-model-20151217/#parsing) is implemented as a Python function named *parse_tabular_data_from_text_non_normative_definition*. This is used as the default method. However users could create their own parsing functions, say for an unusually formed CSV file format, and pass this function in this keyword argument instead.
- **_link_header** *(str)*: USED FOR TESTING. Provides link header text which would normally be provided through a HTTP request.
- **_well_known_text** *(str)*: USED FOR TESTING. Provides well known text which would normally be provided through a HTTP request. 
- **_save_intermediate_and_final_outputs_to_file** *(bool)*: USED FOR TESTING. Writes a number of files which are generated during the process, such as the embedded metadata file, the normalised metadata file etc.
- **_print_intermediate_outputs** *(bool)*: USED FOR TESTING. Prints intermediate outputs which occur during the prodess.

Returns: A Python dictionary containing the annotated table group with a structure following the definition in [Section 4. Tabular Data Models](https://www.w3.org/TR/2015/REC-tabular-data-model-20151217/#model) of the the *Model for Tabular Data and Metadata on the Web* standard. Note that this dictionary can be difficult to view using standard methods, so please use the [`display_annotated_table_group_dict`](#display_annotated_table_group_dict) function. The reason for this is that the annotated table group dictionary is self-referring and potentially recursive when viewed, because for example the 'table' item in a 'column' points back to the entire table which the column belongs to (which in turn contains the original column...). The use of self-referal in the output dictionary is useful when navigating 'up or down' the various items but makes it difficult to print out.

Return type: dict

### display_annotated_table_group_dict

```python
csvw_functions.display_annotated_table_group_dict(
        annotated_table_group_dict
        )
```

Description: This function returns a version of an annotated_table_group_dict dictionary which has the self-referring removed and can then be easily viewed and/or printed.

Arguments:
- **annotated_table_group_dict** *(dict)*: A dictionary returned by the [`create_annotated_table_group`](#create_annotated_table_group) function.

Returns: See description.

Return type: dict


### get_errors

```python
csvw_functions.get_errors(
        annotated_table_group_dict
        )
```

Description: This function returns a list of the cell errors present in a annotated_table_group_dict dictionary.

Arguments:
- **annotated_table_group_dict** *(dict)*: A dictionary returned by the [`create_annotated_table_group`](#create_annotated_table_group) function.

Returns: See description.

Return type: list


### create_json_ld

Description: This function converts an annotated table group object to JSON-LD format. This follow the approach as given in the [Generating JSON from Tabular Data on the Web](https://www.w3.org/TR/2015/REC-csv2json-20151217/) standard.

Call signature:

```python
csvw_functions.create_json_ld(
        annotated_table_group_dict,
        mode='standard',
        local_path_replacement_url=None,
        _replace_strings=None  
        )
```

Arguments:

- **annotated_table_group_dict** *(dict)*: This is the output of the [`create_annotated_table_group`](#create_annotated_table_group) function.
- **mode** *(str)*: If 'standard' then the conversion is run in standard mode. If 'minimal' then the conversion is run in minimal mode. See [here](https://www.w3.org/TR/2015/REC-csv2json-20151217/#intro) for details of standard vs. minimal mode. If neither 'standard' nor 'minimal' then an error is raised.
- **local_path_replacement_url** *(str or None)*: If not `None` then any local file paths are converted to the string provided. This is useful for testing purposes.
- **_replace_strings** *(list)*: USED FOR TESTING. A list of 2-item tuples of string replacements for the output json object.

Returns: The result of the conversion to the JSON-LD format. This is a dictionary and can be saved to a file using the [json](https://docs.python.org/3/library/json.html) library.

Return type: dict


### create_rdf

Description: This function converts an annotated table group object to JSON-LD format. This follow the approach as given in the [Generating RDF from Tabular Data on the Web](https://www.w3.org/TR/2015/REC-csv2rdf-20151217/) standard.

Call signature:

```python
csvw_functions.create_rdf(
        annotated_table_group_dict,
        mode='standard',
        local_path_replacement_url=None
        )
```

Arguments:

- **annotated_table_group_dict** *(dict)*: This is the output of the [`create_annotated_table_group`](#create_annotated_table_group) function.
- **mode** *(str)*: If 'standard' then the conversion is run in standard mode. If 'minimal' then the conversion is run in minimal mode. See [here](https://www.w3.org/TR/2015/REC-csv2rdf-20151217/#intro) for details of standard vs. minimal mode. If neither 'standard' nor 'minimal' then an error is raised. 
- **local_path_replacement_url** *(str or None)*: If not `None` then any local file paths are converted to the string provided. This is useful for testing purposes.

Returns: The result of the conversion to the RDF format. This is a string of RDF [N-Triples](https://www.w3.org/TR/n-triples/). This string can be saved to a file as needed. To convert the N-Triples to another format (such as [Turtle](https://www.w3.org/TR/turtle/)) this can be done using a dedicated RDF package such as [RDFLib](https://rdflib.readthedocs.io/en/stable/). 

Return type: str

### CVSWError

An exception, likely raised for  major error or a validation error if running in validation mode.

### CSVWWarning

A warning, likely raised for a validation error if not running in validation mode.

## Developer Notes

- The package is written as a series of functions rather than classes to promote reuse and because the CSVW standards are largely about transferring files from one format to another.
- The code is all contained in a single file 'csvw_functions.py'. This is a large file of 14,000+ lines so needs a suitable IDE to navigate it. I use Spyder (part of the Anaconda distribution) which provides an automated outline view (like a table of contents) to enable navigating between different sections of the code.
 - The tests are also in a single file 'test_csvw_functions.py'. To run the tests, the CSVW Test Suite will need to be downloaded separately. This isn't included on GitHub due to its size.
 


