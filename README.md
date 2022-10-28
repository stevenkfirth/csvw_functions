# csvw_functions
Python implementation of the CSVW standards

**UNDER DEVELOPMENT - NOT YET FULLY RELEASED**

## Contents

[About](#about)

[Installation](#installation)

[Issues, Questions?](#issues-questions)

[Quick Start](#quick-start)

[- Access embedded metadata from CSV file](#access-embedded-metadata-from-csv-file)

[- Convert CSVW file to JSON-LD](#convert-csvw-file-to-json-ld)

[- Convert CSVW file to RDF](#convert-csvw-file-to-rdf)

[API](#api)

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

The package is written as pure Python and passes all the tests in the [CSVW Test Suite](https://w3c.github.io/csvw/tests/).

## Installation

Source code available on GitHub here: https://github.com/stevenkfirth/csvw_functions

Available on PyPi here: https://pypi.org/project/csvw_functions (LINK NOT YET ACTIVE)

Install from PyPi using command: `pip install csvw_functions`


## Issues, Questions?

Raise an issue on GitHub: https://github.com/stevenkfirth/csvw_functions/issues

Email the author: s.k.firth@lboro.ac.uk


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

...and we'd like to extract information from the column headers as a Python dictionary in the form of a CSVW metadata JSON file:

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

... and we'd like to convert this to a dictionary in the form of JSON-LD data:

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

(This example is taken from Section 4.2 of the CSVW Primer: https://www.w3.org/TR/tabular-data-primer/#transformation-values. Note here that the 'Ö' character is replaced by it's Unicode equivalent.)

### Convert CSVW file to RDF

Let's say we have the CSVW metadata JSON file and CSV file from the previous example, and we'd like to convert these to RDF data in Turtle notation:

```python
>>> import csvw_functions
>>> from rdflib import Graph
>>> annotated_table_group_dict = csvw_functions.create_annotated_table_group(
        input_file_path_or_url = 'countries-metadata.json'
        )
>>> rdf_ntriples = csvw_functions.create_rdf(  ... CHECK
        annotated_table_group_dict,
        mode = 'minimal',
        convert_local_path_to_example_dot_org = True  # uses 'http://example.org' in place of the local file path.
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

csvw_functions.get_embedded_metadata(



)

### create_annotated_table_group

Function signature:

```python
def create_annotated_table_group(
        input_file_path_or_url,
        overriding_metadata_file_path_or_url=None,
        validate=False,
        parse_tabular_data_function=parse_tabular_data_from_text_non_normative_definition,
        _link_header=None,  # for testing link headers,
        _well_known_text=None,  # for testing well known paths
        _save_intermediate_and_final_outputs_to_file=False,  # for testing, to see metadata objects etc.
        _print_intermediate_outputs=False  # for testing, to see intermediate outputs as the code is running
        ):
```
Description: 


Arguments:


Returns:

Return type: Python dictionary


### create_json_ld

### create_rdf








