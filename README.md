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

The package is written as pure Python and passes all the test in the [CSVW Test Suite](https://w3c.github.io/csvw/tests/).

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
>>> embedded_metadata = csvw_functions.get_embedded_metadata('countries.csv')
>>> print(embedded_metadata)
```
```json
{
  "@context": "http://www.w3.org/ns/csvw",
  "url": "countries.csv"
  "tableSchema": {
    "columns": [{
      "titles": "country"
    },{
      "titles": "country group"
    },{
      "titles": "name (en)"
    },{
      "titles": "name (fr)"
    },{
      "titles": "name (de)"
    },{
      "titles": "latitude"
    },{
      "titles": "longitude"
    }]
  }
}
```

(This example is taken from Section 1.3 of the CSVW Primer: https://www.w3.org/TR/tabular-data-primer/#column-info) 


### Convert CSVW file to JSON-LD

Let's say we have a CSVW metadata JSON file whichh references the countries.csv file...

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
>>> annotated_table_group_dict=csvw_functions2.create_annotated_table_group(
        input_file_path_or_url='countries-metadata.json'
        )
>>> json_ld=csvw_functions2.create_json_ld(
        annotated_table_group_dict,
        mode='minimal'
        )
>>> print(json_ld)
```
```json
[{
  "country": "at",
  "country group": "eu",
  "name (en)": "Austria",
  "name (fr)": "Autriche",
  "name (de)": "Österreich",
  "latitude": 47.6965545,
  "longitude": 13.34598005
},{
  "country": "be",
  "country group": "eu",
  "name (en)": "Belgium",
  "name (fr)": "Belgique",
  "name (de)": "Belgien",
  "latitude": 50.501045,
  "longitude": 4.47667405
},{
  "country": "bg",
  "country group": "eu",
  "name (en)": "Bulgaria",
  "name (fr)": "Bulgarie",
  "name (de)": "Bulgarien",
  "latitude": 42.72567375,
  "longitude": 25.4823218
}]
```

(This example is taken from Section 4.2 of the CSVW Primer: https://www.w3.org/TR/tabular-data-primer/#transformation-values)

### Convert CSVW file to RDF

Let's say we have the CSVW metadata JSON file and CSV file from the previous example, and we'd like to convert these to RDF data in Turtle notation:

```python
>>> import csvw_functions
>>> annotated_table_group_dict=csvw_functions2.create_annotated_table_group(
        input_file_path_or_url='countries-metadata.json'
        )
>>> rdf_text=csvw_functions2.create_rdf(  ... CHECK
        annotated_table_group_dict,
        mode='minimal'
        )
>>> print(rdf)  ... TO DO ... need to use rdflib
```
```
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

[
  <#country> "at";
  <#country%20group> "eu";
  <#latitude> 4.76965545e1;
  <#longitude> 1.334598005e1;
  <#name%20%28de%29> "Österreich"@de;
  <#name%20%28en%29> "Austria"@en;
  <#name%20%28fr%29> "Autriche"@fr
] .

[
  <#country> "be";
  <#country%20group> "eu";
  <#latitude> 5.0501045e1;
  <#longitude> 4.47667405e0;
  <#name%20%28de%29> "Belgien"@de;
  <#name%20%28en%29> "Belgium"@en;
  <#name%20%28fr%29> "Belgique"@fr
] .

[
  <#country> "bg";
  <#country%20group> "eu";
  <#latitude> 4.272567375e1;
  <#longitude> 2.54823218e1;
  <#name%20%28de%29> "Bulgarien"@de;
  <#name%20%28en%29> "Bulgaria"@en;
  <#name%20%28fr%29> "Bulgarie"@fr
] .
```

(This example is taken from Section 4.2 of the CSVW Primer: https://www.w3.org/TR/tabular-data-primer/#transformation-values)

## API

### get_embedded_metadata

csvw_functions.get_embedded_metadata(



)

### create_annotated_table_group

Function call:

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








