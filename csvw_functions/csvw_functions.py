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
import re
from uuid import uuid4
import base64
import dateutil.parser
import copy


#%% ---Module Level Variables---
#
# This section sets up a number of variables at the module level
# which are used by the functions in the package.

#%% Schema Prefixes

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

#%% CSVW Namespace Terms

# see https://www.w3.org/ns/csvw#term-definitions

csvw_terms={
    "as": "https://www.w3.org/ns/activitystreams#",
    "cc": "http://creativecommons.org/ns#",
    "csvw": "http://www.w3.org/ns/csvw#",
    "ctag": "http://commontag.org/ns#",
    "dc": "http://purl.org/dc/terms/",
    "dc11": "http://purl.org/dc/elements/1.1/",
    "dcat": "http://www.w3.org/ns/dcat#",
    "dcterms": "http://purl.org/dc/terms/",
    "dctypes": "http://purl.org/dc/dcmitype/",
    "dqv": "http://www.w3.org/ns/dqv#",
    "duv": "https://www.w3.org/TR/vocab-duv#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "gr": "http://purl.org/goodrelations/v1#",
    "grddl": "http://www.w3.org/2003/g/data-view#",
    "ical": "http://www.w3.org/2002/12/cal/icaltzd#",
    "ldp": "http://www.w3.org/ns/ldp#",
    "ma": "http://www.w3.org/ns/ma-ont#",
    "oa": "http://www.w3.org/ns/oa#",
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
    "v": "http://rdf.data-vocabulary.org/#",
    "vcard": "http://www.w3.org/2006/vcard/ns#",
    "void": "http://rdfs.org/ns/void#",
    "wdr": "http://www.w3.org/2007/05/powder#",
    "wrds": "http://www.w3.org/2007/05/powder-s#",
    "xhv": "http://www.w3.org/1999/xhtml/vocab#",
    "xml": "rdf:XMLLiteral",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "json": "csvw:JSON",
    "any": "xsd:anyAtomicType",
    "anyAtomicType": "xsd:anyAtomicType",
    "binary": "xsd:base64Binary",
    "datetime": "xsd:dateTime",
    "describedby": "wrds:describedby",
    "html": "rdf:HTML",
    "license": "xhv:license",
    "maximum": "csvw:maxInclusive",
    "minimum": "csvw:minInclusive",
    "number": "xsd:double",
    "role": "xhv:role",
    "anyURI": "xsd:anyURI",
    "base64Binary": "xsd:base64Binary",
    "boolean": "xsd:boolean",
    "byte": "xsd:byte",
    "date": "xsd:date",
    "dateTime": "xsd:dateTime",
    "dayTimeDuration": "xsd:dayTimeDuration",
    "dateTimeStamp": "xsd:dateTimeStamp",
    "decimal": "xsd:decimal",
    "double": "xsd:double",
    "duration": "xsd:duration",
    "float": "xsd:float",
    "gDay": "xsd:gDay",
    "gMonth": "xsd:gMonth",
    "gMonthDay": "xsd:gMonthDay",
    "gYear": "xsd:gYear",
    "gYearMonth": "xsd:gYearMonth",
    "hexBinary": "xsd:hexBinary",
    "int": "xsd:int",
    "integer": "xsd:integer",
    "language": "xsd:language",
    "long": "xsd:long",
    "Name": "xsd:Name",
    "NCName": "xsd:NCName",
    "NMTOKEN": "xsd:NMTOKEN",
    "negativeInteger": "xsd:negativeInteger",
    "nonNegativeInteger": "xsd:nonNegativeInteger",
    "nonPositiveInteger": "xsd:nonPositiveInteger",
    "normalizedString": "xsd:normalizedString",
    "positiveInteger": "xsd:positiveInteger",
    "QName": "xsd:QName",
    "short": "xsd:short",
    "string": "xsd:string",
    "time": "xsd:time",
    "token": "xsd:token",
    "unsignedByte": "xsd:unsignedByte",
    "unsignedInt": "xsd:unsignedInt",
    "unsignedLong": "xsd:unsignedLong",
    "unsignedShort": "xsd:unsignedShort",
    "yearMonthDuration": "xsd:yearMonthDuration",
    "Cell": "csvw:Cell",
    "Column": "csvw:Column",
    "Datatype": "csvw:Datatype",
    "Dialect": "csvw:Dialect",
    "Direction": "csvw:Direction",
    "ForeignKey": "csvw:ForeignKey",
    "NumericFormat": "csvw:NumericFormat",
    "Row": "csvw:Row",
    "Schema": "csvw:Schema",
    "Table": "csvw:Table",
    "TableGroup": "csvw:TableGroup",
    "TableReference": "csvw:TableReference",
    "Transformation": "csvw:Transformation"
    }



#%% Datatypes - Metadata Section 5.11.1

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
    'positiveInteger':'http://www.w3.org/2001/XMLSchema#positiveInteger',
    'unsignedLong':'http://www.w3.org/2001/XMLSchema#unsignedLong',
    'unsignedInt':'http://www.w3.org/2001/XMLSchema#unsignedInt',
    'unsignedShort':'http://www.w3.org/2001/XMLSchema#unsignedShort',
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
    'gMonthDay':'http://www.w3.org/2001/XMLSchema#gMonthDay',
    'gYear':'http://www.w3.org/2001/XMLSchema#gYear',
    'gYearMonth':'http://www.w3.org/2001/XMLSchema#gYearMonth',
    'hexBinary':'http://www.w3.org/2001/XMLSchema#hexBinary',
    'QName':'http://www.w3.org/2001/XMLSchema#QName',
    'string':'http://www.w3.org/2001/XMLSchema#string',
    'normalizedString':'http://www.w3.org/2001/XMLSchema#normalizedString',
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
                               'unsignedInt',
                               'unsignedShort',
                               'unsignedByte'
                               ]
datatypes_nonPositiveIntegers=['nonPositiveInteger',
                              'negativeInteger']
datatypes_integers=(['integer']
                    +datatypes_longs
                    +datatypes_nonNegativeIntegers
                    +datatypes_nonPositiveIntegers)
datatypes_decimals=(['decimal']
                    +datatypes_integers)
datatypes_numbers=['double','number','float']+datatypes_decimals

datatypes_dates_and_times=['date','dateTime','datetime','dateTimeStamp','time']

datatypes_durations=['duration','dayTimeDuration','yearMonthDuration']

datatypes_binary=['base64Binary','binary','hexBinary']


#%% Encodings

encodings=[
  {
    "encodings": [
      {
        "labels": [
          "unicode-1-1-utf-8",
          "unicode11utf8",
          "unicode20utf8",
          "utf-8",
          "utf8",
          "x-unicode20utf8"
        ],
        "name": "UTF-8"
      }
    ],
    "heading": "The Encoding"
  },
  {
    "encodings": [
      {
        "labels": [
          "866",
          "cp866",
          "csibm866",
          "ibm866"
        ],
        "name": "IBM866"
      },
      {
        "labels": [
          "csisolatin2",
          "iso-8859-2",
          "iso-ir-101",
          "iso8859-2",
          "iso88592",
          "iso_8859-2",
          "iso_8859-2:1987",
          "l2",
          "latin2"
        ],
        "name": "ISO-8859-2"
      },
      {
        "labels": [
          "csisolatin3",
          "iso-8859-3",
          "iso-ir-109",
          "iso8859-3",
          "iso88593",
          "iso_8859-3",
          "iso_8859-3:1988",
          "l3",
          "latin3"
        ],
        "name": "ISO-8859-3"
      },
      {
        "labels": [
          "csisolatin4",
          "iso-8859-4",
          "iso-ir-110",
          "iso8859-4",
          "iso88594",
          "iso_8859-4",
          "iso_8859-4:1988",
          "l4",
          "latin4"
        ],
        "name": "ISO-8859-4"
      },
      {
        "labels": [
          "csisolatincyrillic",
          "cyrillic",
          "iso-8859-5",
          "iso-ir-144",
          "iso8859-5",
          "iso88595",
          "iso_8859-5",
          "iso_8859-5:1988"
        ],
        "name": "ISO-8859-5"
      },
      {
        "labels": [
          "arabic",
          "asmo-708",
          "csiso88596e",
          "csiso88596i",
          "csisolatinarabic",
          "ecma-114",
          "iso-8859-6",
          "iso-8859-6-e",
          "iso-8859-6-i",
          "iso-ir-127",
          "iso8859-6",
          "iso88596",
          "iso_8859-6",
          "iso_8859-6:1987"
        ],
        "name": "ISO-8859-6"
      },
      {
        "labels": [
          "csisolatingreek",
          "ecma-118",
          "elot_928",
          "greek",
          "greek8",
          "iso-8859-7",
          "iso-ir-126",
          "iso8859-7",
          "iso88597",
          "iso_8859-7",
          "iso_8859-7:1987",
          "sun_eu_greek"
        ],
        "name": "ISO-8859-7"
      },
      {
        "labels": [
          "csiso88598e",
          "csisolatinhebrew",
          "hebrew",
          "iso-8859-8",
          "iso-8859-8-e",
          "iso-ir-138",
          "iso8859-8",
          "iso88598",
          "iso_8859-8",
          "iso_8859-8:1988",
          "visual"
        ],
        "name": "ISO-8859-8"
      },
      {
        "labels": [
          "csiso88598i",
          "iso-8859-8-i",
          "logical"
        ],
        "name": "ISO-8859-8-I"
      },
      {
        "labels": [
          "csisolatin6",
          "iso-8859-10",
          "iso-ir-157",
          "iso8859-10",
          "iso885910",
          "l6",
          "latin6"
        ],
        "name": "ISO-8859-10"
      },
      {
        "labels": [
          "iso-8859-13",
          "iso8859-13",
          "iso885913"
        ],
        "name": "ISO-8859-13"
      },
      {
        "labels": [
          "iso-8859-14",
          "iso8859-14",
          "iso885914"
        ],
        "name": "ISO-8859-14"
      },
      {
        "labels": [
          "csisolatin9",
          "iso-8859-15",
          "iso8859-15",
          "iso885915",
          "iso_8859-15",
          "l9"
        ],
        "name": "ISO-8859-15"
      },
      {
        "labels": [
          "iso-8859-16"
        ],
        "name": "ISO-8859-16"
      },
      {
        "labels": [
          "cskoi8r",
          "koi",
          "koi8",
          "koi8-r",
          "koi8_r"
        ],
        "name": "KOI8-R"
      },
      {
        "labels": [
          "koi8-ru",
          "koi8-u"
        ],
        "name": "KOI8-U"
      },
      {
        "labels": [
          "csmacintosh",
          "mac",
          "macintosh",
          "x-mac-roman"
        ],
        "name": "macintosh"
      },
      {
        "labels": [
          "dos-874",
          "iso-8859-11",
          "iso8859-11",
          "iso885911",
          "tis-620",
          "windows-874"
        ],
        "name": "windows-874"
      },
      {
        "labels": [
          "cp1250",
          "windows-1250",
          "x-cp1250"
        ],
        "name": "windows-1250"
      },
      {
        "labels": [
          "cp1251",
          "windows-1251",
          "x-cp1251"
        ],
        "name": "windows-1251"
      },
      {
        "labels": [
          "ansi_x3.4-1968",
          "ascii",
          "cp1252",
          "cp819",
          "csisolatin1",
          "ibm819",
          "iso-8859-1",
          "iso-ir-100",
          "iso8859-1",
          "iso88591",
          "iso_8859-1",
          "iso_8859-1:1987",
          "l1",
          "latin1",
          "us-ascii",
          "windows-1252",
          "x-cp1252"
        ],
        "name": "windows-1252"
      },
      {
        "labels": [
          "cp1253",
          "windows-1253",
          "x-cp1253"
        ],
        "name": "windows-1253"
      },
      {
        "labels": [
          "cp1254",
          "csisolatin5",
          "iso-8859-9",
          "iso-ir-148",
          "iso8859-9",
          "iso88599",
          "iso_8859-9",
          "iso_8859-9:1989",
          "l5",
          "latin5",
          "windows-1254",
          "x-cp1254"
        ],
        "name": "windows-1254"
      },
      {
        "labels": [
          "cp1255",
          "windows-1255",
          "x-cp1255"
        ],
        "name": "windows-1255"
      },
      {
        "labels": [
          "cp1256",
          "windows-1256",
          "x-cp1256"
        ],
        "name": "windows-1256"
      },
      {
        "labels": [
          "cp1257",
          "windows-1257",
          "x-cp1257"
        ],
        "name": "windows-1257"
      },
      {
        "labels": [
          "cp1258",
          "windows-1258",
          "x-cp1258"
        ],
        "name": "windows-1258"
      },
      {
        "labels": [
          "x-mac-cyrillic",
          "x-mac-ukrainian"
        ],
        "name": "x-mac-cyrillic"
      }
    ],
    "heading": "Legacy single-byte encodings"
  },
  {
    "encodings": [
      {
        "labels": [
          "chinese",
          "csgb2312",
          "csiso58gb231280",
          "gb2312",
          "gb_2312",
          "gb_2312-80",
          "gbk",
          "iso-ir-58",
          "x-gbk"
        ],
        "name": "GBK"
      },
      {
        "labels": [
          "gb18030"
        ],
        "name": "gb18030"
      }
    ],
    "heading": "Legacy multi-byte Chinese (simplified) encodings"
  },
  {
    "encodings": [
      {
        "labels": [
          "big5",
          "big5-hkscs",
          "cn-big5",
          "csbig5",
          "x-x-big5"
        ],
        "name": "Big5"
      }
    ],
    "heading": "Legacy multi-byte Chinese (traditional) encodings"
  },
  {
    "encodings": [
      {
        "labels": [
          "cseucpkdfmtjapanese",
          "euc-jp",
          "x-euc-jp"
        ],
        "name": "EUC-JP"
      },
      {
        "labels": [
          "csiso2022jp",
          "iso-2022-jp"
        ],
        "name": "ISO-2022-JP"
      },
      {
        "labels": [
          "csshiftjis",
          "ms932",
          "ms_kanji",
          "shift-jis",
          "shift_jis",
          "sjis",
          "windows-31j",
          "x-sjis"
        ],
        "name": "Shift_JIS"
      }
    ],
    "heading": "Legacy multi-byte Japanese encodings"
  },
  {
    "encodings": [
      {
        "labels": [
          "cseuckr",
          "csksc56011987",
          "euc-kr",
          "iso-ir-149",
          "korean",
          "ks_c_5601-1987",
          "ks_c_5601-1989",
          "ksc5601",
          "ksc_5601",
          "windows-949"
        ],
        "name": "EUC-KR"
      }
    ],
    "heading": "Legacy multi-byte Korean encodings"
  },
  {
    "encodings": [
      {
        "labels": [
          "csiso2022kr",
          "hz-gb-2312",
          "iso-2022-cn",
          "iso-2022-cn-ext",
          "iso-2022-kr",
          "replacement"
        ],
        "name": "replacement"
      },
      {
        "labels": [
          "unicodefffe",
          "utf-16be"
        ],
        "name": "UTF-16BE"
      },
      {
        "labels": [
          "csunicode",
          "iso-10646-ucs-2",
          "ucs-2",
          "unicode",
          "unicodefeff",
          "utf-16",
          "utf-16le"
        ],
        "name": "UTF-16LE"
      },
      {
        "labels": [
          "x-user-defined"
        ],
        "name": "x-user-defined"
      }
    ],
    "heading": "Legacy miscellaneous encodings"
  }
]

encoding_labels=[z for x in encodings for y in x['encodings'] for z in y['labels']]


#%% ---Custom Exceptions and Warnings---

class CSVWError(Exception):
    ""
    
    
class CSVWWarning(Warning):
    ""


#%% ---Model for Tabular Data and Metadata---

#%% Section 4 - Tabular Data Models

#%% 4.6 Datatypes

def validate_value_for_datatype_base(
        value,
        errors,
        datatype_base,
        validate
        ):
    """
    
    :returns: True if valid; otherwise False
    
    """
    #  All values of the datatype must be valid values of the base datatype.
    
    
    
    if datatype_base=='integer':
        
        pass
    
    elif datatype_base=='long':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            minimum=-9223372036854775808,
            maximum=9223372036854775807
            )
    
    elif datatype_base=='int':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            minimum=-2147483648,
            maximum=2147483647
            )
    
    elif datatype_base=='short':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            minimum=-32768,
            maximum=32767
            )
    
    elif datatype_base=='byte':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            minimum=-128,
            maximum=127
            )
    
    elif datatype_base=='nonNegativeInteger':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            minimum=0
            )
    
    elif datatype_base=='positiveInteger':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            minimum_exclusive=0
            )
    
    elif datatype_base=='unsignedLong':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            maximum=18446744073709551615,
            minimum=0
            )
    
    elif datatype_base=='unsignedInt':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            maximum=4294967295,
            minimum=0
            )
    
    elif datatype_base=='unsignedShort':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            maximum=65535,
            minimum=0
            )
    
    elif datatype_base=='unsignedByte':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            maximum=255,
            minimum=0
            )
    
    elif datatype_base=='nonPositiveInteger':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            maximum=0
            )
    
    elif datatype_base=='negativeInteger':
        
        return check_value_constraints(
            value,
            errors,
            datatype_base,
            validate,
            maximum_exclusive=0
            )
    
    
    return True
    




#%% 4.6.1 Length Constraints

def check_length_constraints(
        value,
        errors,
        datatype_base,
        validate,
        length=None,
        minimum_length=None,
        maximum_length=None
        ):
    """
    """
    # The length, minimum length and maximum length annotations indicate 
    # the exact, minimum and maximum lengths for cell values.
    
    # The length of a value is determined as defined in [xmlschema11-2], 
    # namely as follows:
    # - if the value is null, its length is zero.
    # - if the value is a string or one of its subtypes, its length is the
    #   number of characters (ie [UNICODE] code points) in the value.
    # - if the value is of a binary type, its length is the number of bytes 
    #   in the binary value.
    
    # If the value is a list, the constraint applies to each element of the list.
    
    #
    if length is None and minimum_length is None and maximum_length is None:
        
        return True
    
    #
    if value is None:
        
        value_length=0
        
    elif datatype_base in datatypes_strings:
        
        value_length=len(value)
        
    elif datatype_base=='base64Binary':
            
        value_length=len(base64.b64decode(value))
            
    elif datatype_base=='binary':
            
        value_length=len(value)  #??? a guess, needs testing...
            
    elif datatype_base=='hexBinary':
            
        value_length=len(value)/2
        
    else:
        
        raise Exception(datatype_base)
    
    
    if not length is None:
    
        if not value_length==length:
            
            message=f'value {value} does not have length equal to {length}.'
            
            if validate:
                
                raise CSVWError(message)
                
            else:
                
                errors.append(message)
                
                warnings.warn(message)
                
                return False
        
        
    if not minimum_length is None:
        
        if value_length<minimum_length:
            
            message=f'value {value} does not have length less than {minimum_length}.'
            
            if validate:
                
                raise CSVWError(message)
                
            else:
                
                errors.append(message)
                
                warnings.warn(message)
                
                return False
        
        
    if not maximum_length is None:
        
        if value_length>maximum_length:
            
            message=f'value {value} does not have length greater than {maximum_length}.'
            
            if validate:
                
                raise CSVWError(message)
                
            else:
                
                errors.append(message)
                
                warnings.warn(message)
                
                return False
        
        
    return True
    
    
#%% 4.6.2 Value Constraints

def check_value_constraints(
        value,
        errors,
        datatype_base,
        validate,
        minimum=None,
        maximum=None,
        minimum_exclusive=None,
        maximum_exclusive=None
        ):
    """
    
    :returns: True is value passes the constraints; otherwise False
    
    """
    # The minimum, maximum, minimum exclusive, and maximum exclusive 
    # annotations indicate limits on cell values. 
    # These apply to numeric, date/time, and duration types.
    
    # Validation of cell values against these datatypes is as defined 
    # in [xmlschema11-2]. 
    
    # If the value is a list, the constraint applies to each element of the list.
    
    if not minimum is None:
        
        if value<minimum:
            
            message=f'value {value} is less than the minimum {minimum}'
                
            if validate:
                
                raise CSVWError(message)
                
            else:
            
                errors.append(message)
                
                warnings.warn(message)
                
                return False
        
    
    if not maximum is None:
        
        if value>maximum:
            
            message=f'value {value} is greater than the maximum {maximum}'
            
            if validate:
                
                raise CSVWError(message)
                
            else:
                
                errors.append(message)
                
                warnings.warn(message)
                
                return False
    
        
    if not minimum_exclusive is None:
        
        if value<=minimum_exclusive:
        
            message=f'value {value} is less than or equal to the minimum_exclusive {minimum_exclusive}'
                
            if validate:
                
                raise CSVWError(message)
                
            else:
            
                errors.append(message)
                
                warnings.warn(message)
                
                return False
    
    
    if not maximum_exclusive is None:
        
        if value>=maximum_exclusive:
        
            message=f'value {value} is greater than or equal to the maximum_exclusive {maximum_exclusive}'
                
            if validate:
                
                raise CSVWError(message)
                
            else:
            
                errors.append(message)
                
                warnings.warn(message)
                
                return False
    
    
    return True

#%% Section 5 - Locating Metadata

def locate_metadata(
        tabular_data_file_url,  
        tabular_data_file_headers,
        overriding_metadata_file_path_or_url,
        _link_header,
        _well_known_text,
        validate
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
        
    metadata_document_dict=None
    
    # 1
    if not overriding_metadata_file_path_or_url is None:
        
        metadata_document_dict, metadata_document_location=\
            get_overriding_metadata(
                overriding_metadata_file_path_or_url,
                tabular_data_file_url
                )
    
    # 2
    
    if metadata_document_dict is None:
    
        if not tabular_data_file_headers is None or not _link_header is None:
            
            metadata_document_dict, metadata_document_location=\
                get_metadata_from_link_header(
                    tabular_data_file_headers or _link_header,
                    tabular_data_file_url,
                    validate
                    )
            
        
    # 3
    if metadata_document_dict is None:
        
        metadata_document_dict, metadata_document_location=\
            get_metadata_from_default_or_site_wide_location(
                tabular_data_file_url,
                tabular_data_file_headers,
                _well_known_text
                )
    # 4
    if metadata_document_dict is None:
    
        metadata_document_dict=None
        metadata_document_location=None
            
    #print('-metadata_document_dict', metadata_document_dict)
    
    return metadata_document_dict, metadata_document_location

    



#%% 5.1 Overriding Metadata

def get_overriding_metadata(
        overriding_metadata_file_path_or_url,
        tabular_data_file_url
        ):
    """
    """
    # Processors should provide users with the facility to provide their 
    # own metadata for tabular data files that they process. 
    
    # This might be provided:
    # - through processor options, such as command-line options for a command-line implementation or checkboxes in a GUI.
    # - by enabling the user to select an existing metadata file, which may be local or remote.
    # - by enabling the user to specify a series of metadata files, which are merged by the processor and handled as if they were a single file.
    
    # For example, a processor might be invoked with:
    
    # EXAMPLE 2: Command-line CSV processing with column types
    # $ csvlint data.csv --datatypes:string,float,string,string
    # to enable the testing of the types of values in the columns of a 
    # CSV file, or with:
    
    # EXAMPLE 3: Command-line CSV processing with a schema
    # $ csvlint data.csv --schema:schema.json
    # to supply a schema that describes the contents of the file, against which it can be validated.
    
    # Metadata supplied in this way is called overriding, or user-supplied, 
    # metadata. 
    # Implementations should define how any options they define are 
    # mapped into the vocabulary defined in [tabular-metadata]. 
    # If the user selects existing metadata files, implementations must 
    # not use metadata located through the Link header (as described in 
    # section 5.2 Link Header) or site-wide location configuration (as 
    # described in section 5.3 Default Locations and Site-wide Location 
    # Configuration).
    
    # NOTE
    # Users should ensure that any metadata from those locations that 
    # they wish to use is explicitly incorporated into the overriding 
    # metadata that they use to process tabular data. 
    # Processors may provide facilities to make this easier by 
    # automatically merging metadata files from different locations, 
    # but this specification does not define how such merging is carried out.
    
    if os.path.isfile(overriding_metadata_file_path_or_url):
        
        x=os.path.abspath(overriding_metadata_file_path_or_url)
        x=x.replace('\\','/')
        x=r'file:///'+x
        x=urllib.parse.quote(x,safe=':/#?%')
        
        metadata_document_location=normalize_url(x)
            
    else:
        
        metadata_document_location=\
            normalize_url(
                overriding_metadata_file_path_or_url
                )
        
    
    with urllib.request.urlopen(metadata_document_location) as metadata_response:
    
        metadata_text=metadata_response.read().decode()
    
    metadata_document_dict=json.loads(metadata_text)
    
    return metadata_document_dict,metadata_document_location


#%% 5.2 Link Header

def get_metadata_from_link_header(
        link_header,
        tabular_data_file_url,  # absolute_url
        validate
        ):
    """
    """
    # If the user has not supplied a metadata file as overriding metadata, 
    # described in section 5.1 Overriding Metadata, then when retrieving a 
    # tabular data file via HTTP, processors must retrieve the metadata file 
    # referenced by any Link header with:
    # - rel="describedby", and
    # - type="application/csvm+json", type="application/ld+json" or type="application/json".
    
    # so long as this referenced metadata file describes the retrieved 
    # tabular data file (ie, contains a table description whose url 
    # matches the request URL).
    
    # If there is more than one valid metadata file linked to through 
    # multiple Link headers, then implementations must use the metadata 
    # file referenced by the last Link header.
    
    # If the metadata file found at this location does not explicitly 
    #  include a reference to the requested tabular data file then it must 
    #  be ignored. 
    #  URLs must be normalized as described in section 6.3 URL Normalization.
    
    # NOTE
    # The Link header of the metadata file may include references to the 
    # CSV files it describes, using the describes relationship. 
    # For example, in the countries' metadata example, the server might 
    # return the following headers:
    
    # Link: <http://example.org/countries.csv>; rel="describes"; type="text/csv"
    # Link: <http://example.org/country_slice.csv>; rel="describes"; type="text/csv"
    
    # However, locating the metadata should not depend on this mechanism.
        
    link_list=requests.utils.parse_header_links(link_header)
    
    urls=[]
    
    for link_dict in link_list:
        
        if link_dict.get('rel')=='describedby':
            
            if link_dict.get('type') in ['application/csvm+json',
                                         'application/ld+json',
                                         'application/json']:
                
                urls.append(link_dict['url'])
        
    #...look for the last valid link header
    for url in urls[::-1]:
        
        metadata_document_location=\
            urllib.parse.urljoin(
                tabular_data_file_url,
                url
                )  
        
        #...load metadata document
        
        try:
        
            with urllib.request.urlopen(metadata_document_location) as metadata_response:
                
                metadata_text=metadata_response.read().decode()
        
        except urllib.error.URLError:
            
            continue
        
        metadata_document_dict=json.loads(metadata_text)
        
        #print('-metadata_document_dict',metadata_document_dict)
        
        #
        if 'tables' in metadata_document_dict:  # it's a TableGroup object
            
            metadata_table_group_dict=metadata_document_dict
            
        elif 'url' in metadata_document_dict:  # it's a Table object
        
            #...convert to TableGroup object
            metadata_table_group_dict={
                '@context': metadata_document_dict['@context'],
                'tables': [metadata_document_dict]
                }
            metadata_table_group_dict['tables'][0].pop('@context', None)
            
        #...normalize metadata
        base_url, default_language=\
            validate_and_normalize_metadata_table_group_dict(
                metadata_table_group_dict,
                metadata_document_location,
                validate
                )     
            
        #print('-base_url',base_url)
            
        table_url=metadata_table_group_dict['tables'][0]['url']  
        
        x={'url':table_url}
        normalize_link_property(
            x, 
            'url', 
            base_url)
        table_url_normalized=x['url']
        
        #print('-table_url_normalized',table_url_normalized
        #print('-tabular_data_file_url',tabular_data_file_url)
        
        if table_url_normalized==tabular_data_file_url:
            
            return metadata_table_group_dict, metadata_document_location
            
        else:
            
            message='Metadata file found does not explicitly '
            message+='include a reference to the requested tabular data file. '
            message+='Metadata file is ignored. '
            
            warnings.warn(message)
            
    return None, None


#%% 5.3 Default Locations and Site-wide Location Configuration


def get_metadata_from_default_or_site_wide_location(
        tabular_data_file_url,
        tabular_data_file_headers,
        _well_known_text,
        _print_intermediate_outputs=False
        ):
    """
    """
    #print('-tabular_data_file_url',tabular_data_file_url)
    #print('-tabular_data_file_headers',tabular_data_file_headers)
    
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
    
    #if tabular_data_file_headers is None:   # i.e. a local file
    
    well_known_path_url=urllib.parse.urljoin(
            tabular_data_file_url,
            '/.well-known/csvm'
            )
        
    if _print_intermediate_outputs: print('-well_known_path_url',well_known_path_url)
    
    #
    try:
    
        with urllib.request.urlopen(well_known_path_url) as well_known_path_response:
            
            well_known_text=well_known_path_response.read().decode()
    
    except urllib.error.URLError:
        
        well_known_text='{+url}-metadata.json\ncsv-metadata.json'
    
    
    
    #print('well_known_path_response',well_known_path_response)
    #print(dir(well_known_path_response))
    
    
    #...for testing
    if not _well_known_text is None:
    
        well_known_text=_well_known_text
    
    # Starting with the first such URI template, processors must:
        
    locations=[x.strip() for x in well_known_text.split('\n')
               if len(x.strip())>0]
        
    for uri_template in locations:
        
        #print('location',location)
        
        # 1. Expand the URI template, with the variable url being set to 
        #    the URL of the requested tabular data file (with any fragment 
        #    component of that URL removed).
        
        variables={'url':urllib.parse.urldefrag(tabular_data_file_url)[0]}
            
        #print(variables)
        
        expanded_url_quoted=uritemplate.expand(uri_template,
                                               variables)
        #print('expanded_url_quoted',expanded_url_quoted)
        
        expanded_url=urllib.parse.unquote(expanded_url_quoted)   # needed for file paths as they get quoted in the expand process
        #print('expanded_url', expanded_url)
        
        
        # 2. Resolve the resulting URL against the URL of the requested 
        #    tabular data file.
        
        metadata_url=\
            urllib.parse.urljoin(
                tabular_data_file_url,
                expanded_url
                )
        if _print_intermediate_outputs: print('-metadata_url',metadata_url)
        
        
        # 3. Attempt to retrieve a metadata document at that URL.
        
        try:
        
            with urllib.request.urlopen(metadata_url) as metadata_response:
                
                metadata_text=metadata_response.read().decode()
                metadata_document_dict=json.loads(metadata_text)
        
        except urllib.error.URLError:
        
            metadata_document_dict=None
            
       
        
        
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
            
                raise CSVWError
            #print('-table_url',table_url)
            
            x={'url':table_url}
            normalize_link_property(
                x, 
                'url', 
                base_url)
            table_url_normalized=x['url']
                
            #print('-table_url_normalized',table_url_normalized)
            #print('-tabular_data_file_url',tabular_data_file_url)
            #print(table_url_normalized==tabular_data_file_url)
            
            if table_url_normalized==tabular_data_file_url:
                    
                #print('test')
                
                return metadata_document_dict,metadata_url
            
            else:
                
                #print('-table_url_resolved',table_url_resolved)
                #print('-tabular_data_file_url',tabular_data_file_url)
                
                message='Metadata file found from default or site wide location '
                message+='does not explicitly '
                message+='include a reference to the requested tabular data file. '
                message+='Metadata file is ignored. '
                
                #print(message)
                
                warnings.warn(message)
                
    
    # if no matches
    metadata_document_dict=None
    metadata_url=None
    
    return metadata_document_dict,metadata_url
        

#%% 5.4 Embedded Metadata

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

#%% 6.1 Creating Annotated Tables

def parse_tabular_data_from_text_non_normative_definition(
        *args,
        **kwargs
        ):
    """
    """
    return parse_tabular_data_from_text(*args,**kwargs)
    

def get_embedded_metadata(
        input_file_path_or_url,
        relative_path=False,
        nrows=None,
        encoding=None,
        skip_rows=None,
        parse_tabular_data_function=parse_tabular_data_from_text_non_normative_definition,
        ):
    """
    """
    embedded_metadata_dict=\
        create_annotated_table_group(
            input_file_path_or_url,
            _return_embedded_metadata=True,
            nrows=nrows,
            encoding=encoding,
            skip_rows=skip_rows,
            parse_tabular_data_function=parse_tabular_data_function
            )
        
    if relative_path:
        
        embedded_metadata_dict=replace_string(
            embedded_metadata_dict,
            'file:///'+os.getcwd().replace('\\','/')+'/',
            ''
            )
      
    return embedded_metadata_dict
    
    
def create_annotated_table_group(
        input_file_path_or_url,
        overriding_metadata_file_path_or_url=None,
        nrows=None,
        validate=False,
        encoding=None,
        skip_rows=None,
        parse_tabular_data_function=parse_tabular_data_from_text_non_normative_definition,
        _return_embedded_metadata=False,  # returns only the embedded metadata
        _link_header=None,  # for testing link headers,
        _well_known_text=None,  # for testing well known paths
        _save_intermediate_and_final_outputs_to_file=False,  # for testing, to see metadata objects etc.
        _print_intermediate_outputs=False  # for testing, to see intermediate outputs as the code is running
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
    
    if _print_intermediate_outputs: print('---create_annotated_table_group---')
    
    # After locating metadata, metadata is normalized and coerced into a 
    # single table group description. 
    # When starting with a metadata file, this involves normalizing the 
    # provided metadata file and verifying that the embedded metadata for 
    # each tabular data file referenced from the metadata is compatible 
    # with the metadata. 
    # When starting with a tabular data file, this involves locating the 
    # first metadata file as described in section 5. Locating Metadata and 
    # normalizing into a single descriptor.
    
    
    #...determine if input file is a metadata document or tabular data file
    if input_file_path_or_url.endswith('.json'):
        
        input_is_tabular_data_file=False
        
    elif input_file_path_or_url.endswith('.csv'):
        
        input_is_tabular_data_file=True
    
    else:
    
        input_is_tabular_data_file=True
        
        #if '?' in input_file_path_or_url:
            
        #    input_file_path_or_url=input_file_path_or_url.split('?')[0]
        
        
        #message='"input_file_path_or_url" must end with either ".json" or ".csv"/'    
    
        #raise CSVWError(message)
      
    
      
        
    # If processing starts with a tabular data file, implementations:
        
    if input_is_tabular_data_file:
        
        # 1. Retrieve the tabular data file.
        
        #...set up the url
        
        #... if it looks like a local file
        if not hyperlink.parse(input_file_path_or_url).absolute:  # only returns true if e.g. 'http://...'
            
            if input_file_path_or_url==os.path.abspath(input_file_path_or_url):
                
                x=input_file_path_or_url
            
            else:
                
                x=os.path.join(os.getcwd(),input_file_path_or_url)
            
            x=x.replace('\\','/')
            x=r'file:///'+x
            x=urllib.parse.quote(x,safe=':/#?%')
            x=normalize_url(x)
            
            tabular_data_file_url=x
                
            tabular_data_file_headers=None
            
        #... if it looks like a remote file
        else:
            
            tabular_data_file_url=\
                normalize_url(
                    input_file_path_or_url
                    )
                
            tabular_data_file_headers=requests.head(tabular_data_file_url).headers
            
        if _print_intermediate_outputs: print('-tabular_data_file_url',tabular_data_file_url)     
        if _print_intermediate_outputs: print('-tabular_data_file_headers',tabular_data_file_headers)

        
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
                tabular_data_file_headers,
                overriding_metadata_file_path_or_url,
                _link_header,
                _well_known_text,
                validate
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
        
        # NO CODE NEEDED
        
    # If the process starts with a metadata file:
    else:
        
        # 1. Retrieve the metadata file yielding the metadata UM (which is 
        #    treated as overriding metadata, see section 5.1 Overriding 
        #    Metadata).
        
        #...set up the url
        
        if not hyperlink.parse(input_file_path_or_url).absolute:  # only returns true if e.g. 'http://...'
            
            if input_file_path_or_url==os.path.abspath(input_file_path_or_url):
                
                x=input_file_path_or_url
            
            else:
                
                x=os.path.join(os.getcwd(),input_file_path_or_url)
            
            x=x.replace('\\','/')
            x=r'file:///'+x
            x=urllib.parse.quote(x,safe=':/#?%')
            x=normalize_url(x)
        
            metadata_document_location=x
        
        else:
            
            metadata_document_location=\
                normalize_url(
                    input_file_path_or_url
                    )
        
        #...get metadata document
        with urllib.request.urlopen(metadata_document_location) as metadata_response:
        
            metadata_text=metadata_response.read().decode()
        
        
        try:
        
            metadata_document_dict=json.loads(metadata_text)
        
        except json.decoder.JSONDecodeError as e: 
            
            raise CSVWError(str(e))
        
        #
        tabular_data_file_text=None
        
        use_embedded_metadata_flag=False
        
        
    
    if _print_intermediate_outputs: print('-metadata_document_location',metadata_document_location)
    
        
    #...for testing
    if _save_intermediate_and_final_outputs_to_file:
    
        with open('metadata_table_group_dict.json','w') as f:
            
            if use_embedded_metadata_flag:
                
                json.dump(
                    {},
                    f,
                    indent=4
                    )
                
            else:
                
                json.dump(
                    metadata_document_dict,
                    f,
                    indent=4
                    )
        
        
    # 2. Normalize UM using the process defined in Normalization 
    # in [tabular-metadata], coercing UM into a table group 
    # description, if necessary.
        
    #...a TableGroup object
    if 'tables' in metadata_document_dict:
        
        metadata_table_group_dict=metadata_document_dict
        
    #... a Table object
    elif 'url' in metadata_document_dict:
    
        #...convert to TableGroup object
        metadata_table_group_dict={
            '@context': metadata_document_dict['@context'],
            'tables': [metadata_document_dict]
            }
        metadata_table_group_dict['tables'][0].pop('@context', None)
        
    elif metadata_document_dict.get('@type')=='TableGroup':  
        
        metadata_table_group_dict=metadata_document_dict
        
    elif metadata_document_dict.get('@type')=='Table':  
        
        #...convert to TableGroup object
        metadata_table_group_dict={
            '@context': metadata_document_dict['@context'],
            'tables': [metadata_document_dict]
            }
        metadata_table_group_dict['tables'][0].pop('@context', None)
        
    else:
        
        message='Metadata document is not a Table Group description object'
        message+='or a Table description object.'
        
        raise CSVWError(message)
        
        
        
    #...normalize metadata
    base_url, default_language=\
        validate_and_normalize_metadata_table_group_dict(
            metadata_table_group_dict,
            metadata_document_location,
            validate
            )
        
        
    # 3. For each table (TM) in UM in order, create one or more annotated tables:
    
    #...initial annotated table group object    
    annotated_table_group_dict={
        'id':None,
        'notes':[],   # changed from False
        'tables':[]
        }
    
    #...loop through tables in metadata
    for table_index,metadata_table_dict in \
        enumerate(metadata_table_group_dict['tables']):
            
        #...set up table url and headers
            
        tabular_data_file_url=metadata_table_dict['url']
        
        
                
        if tabular_data_file_url.startswith('file'):
            
            tabular_data_file_headers=None
        
        else:
                
            tabular_data_file_headers=requests.head(tabular_data_file_url).headers
            
        #print('-tabular_data_file_url',tabular_data_file_url)     
            
        #print('-tabular_data_file_headers',tabular_data_file_headers)

            
            
        # 3.1 Extract the dialect description (DD) from UM for the table 
        #     associated with the tabular data file. If there is no such 
        #     dialect description, extract the first available dialect 
        #     description from a group of tables in which the tabular data 
        #     file is described. Otherwise use the default dialect description.

        dialect_description_dict=\
            metadata_table_dict.get('dialect',None)
            
        #...gets the first dialect description in the group of tables
        #...check the table group object
        if dialect_description_dict is None:
            dialect_description_dict=\
                metadata_table_group_dict.get('dialect',None)
        
        #...check each table in turn
        if dialect_description_dict is None:
            for metadata_table_dict2 in \
                metadata_table_group_dict['tables']:
                    if 'dialect' in metadata_table_dict2:
                        dialect_description_dict=\
                            metadata_table_dict2['dialect']
                        break
                    
        #...if none found, gets the default dialect description
        if dialect_description_dict is None:
            
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
            
            
            #... additional overrides from function arguments
            
            if not encoding is None:
                dialect_description_dict['encoding']=encoding
            
            if not skip_rows is None:
                dialect_description_dict['skipRows']=skip_rows
            
        
        # 3.3 Parse the tabular data file, using DD as a guide, to create a 
        #     basic tabular data model (T) and extract embedded metadata (EM), 
        #     for example from the header line.

        


        annotated_table_dict, embedded_metadata_dict=\
            parse_tabular_data_function(
                tabular_data_file_url,
                dialect_description_dict
                )
            
        annotated_table_group_dict['tables'].append(annotated_table_dict)
            
        #... returns only the embedded metadata if requested
        if _return_embedded_metadata:
            
            return embedded_metadata_dict
        
        
        #...for testing
        if _save_intermediate_and_final_outputs_to_file:
            with open('embedded_metadata_dict.json','w') as f:
                
                json.dump(
                    embedded_metadata_dict,
                    f,
                    indent=4
                    )
        
        
        #...if using embedded metadata
        if use_embedded_metadata_flag:
            
            metadata_table_dict=copy.deepcopy(embedded_metadata_dict)
            
            validate_and_normalize_metadata_table_dict(
                metadata_table_dict,
                metadata_document_location=None,
                metadata_table_group_dict=None
                )
            
            metadata_table_dict.pop('@context')
            
            metadata_table_group_dict['tables'][table_index]=metadata_table_dict
        
        #print('-metadata_table_group_dict_NORMALIZED',metadata_table_group_dict)
        
        #...for testing
        if _save_intermediate_and_final_outputs_to_file:
            with open('metadata_table_group_dict_NORMALIZED.json','w') as f:
                
                json.dump(
                    metadata_table_group_dict,
                    f,
                    indent=4
                    )
        
        
        
        #...REMOVED: setting column names to _col.1 etc.
        #... as now done in annotating schema
            
        #...REMOVED: including virtual columns in annotated table dict
        #...as now done in annotating tables
        
        
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
        
        
        #...REMOVED: section on merging the embedded metadata with the metadata document
        #...was being used for Section 8.2.1.1.
        
        
    # 3.6 Use the metadata TM to add annotations to the tabular data model 
    #     T as described in Section 2 Annotating Tables in [tabular-metadata].
    
    annotated_table_group_dict=\
        annotate_table_group_dict(
            annotated_table_group_dict,
            metadata_table_group_dict,
            base_url,
            default_language,
            validate
            )
            
    #...Not directly in this section of the standard, but at this stage the 
    #...cell values are parsed.
    #...This is done after the metadata annotations are included in the 
    #...annotated_table_group_dict (3.6)
    
    for annotated_table_dict in annotated_table_group_dict['tables']:
        
        for annotated_column_dict in annotated_table_dict['columns']:
            
            parse_cells_in_annotated_column_dict(
                annotated_column_dict,
                dialect_description_dict.get('trim',True),
                validate
                )
    
    #...generate URIs
                
    for annotated_table_dict in annotated_table_group_dict['tables']:
        
        for annotated_column_dict in annotated_table_dict['columns']:
            
            for annotated_cell_dict in annotated_column_dict['cells']:
                
                #... from Section 6.4
                # If there is a about URL annotation on the column, it becomes 
                # the about URL annotation on the cell, after being transformed 
                # into an absolute URL as described in URI Template Properties 
                # of [tabular-metadata].
                if not annotated_column_dict['aboutURL'] is None:
                    
                    annotated_cell_dict['aboutURL']=\
                        get_URI_from_URI_template(
                            annotated_column_dict['aboutURL'],
                            annotated_cell_dict,
                            annotated_table_dict['url']
                            )  
                        
                        
                # If there is a property URL annotation on the column, it becomes 
                # the property URL annotation on the cell, after being transformed 
                # into an absolute URL as described in URI Template Properties 
                # of [tabular-metadata].
                if not annotated_column_dict['propertyURL'] is None:
                    
                    annotated_cell_dict['propertyURL']=\
                        get_URI_from_URI_template(
                            annotated_column_dict['propertyURL'],
                            annotated_cell_dict,
                            annotated_table_dict['url']
                            )  
                        
                        
                # If there is a value URL annotation on the column, it becomes 
                # the value URL annotation on the cell, after being transformed 
                # into an absolute URL as described in URI Template Properties 
                # of [tabular-metadata]. The value URL annotation is null if the cell value is null and the column virtual annotation is false.
                if not annotated_column_dict['valueURL'] is None:
                    
                    
                    annotated_cell_dict['valueURL']=\
                        get_URI_from_URI_template(
                            annotated_column_dict['valueURL'],
                            annotated_cell_dict,
                            annotated_table_dict['url']
                            )  
         
                    #print(annotated_cell_dict['value'])
                    #print(annotated_cell_dict['valueURL'])

    #...annotate row titles
    for annotated_table_dict,metadata_table_dict \
        in zip(annotated_table_group_dict['tables'],
               metadata_table_group_dict['tables']
               ):
            
        if 'tableSchema' in metadata_table_dict:
            
            metadata_schema_dict=metadata_table_dict['tableSchema']
    
            if 'rowTitles' in metadata_schema_dict:
                
                # A column reference property that holds either a single reference 
                # to a column description object or an array of references. 
                # The value of this property determines the titles annotation for 
                # each row within a table that uses this schema. 
                # The titles annotation holds the list of the values of the cells 
                # in that row that are in the referenced columns; if the value is 
                # not a string or has no associated language, it is interpreted 
                # as a string with an undefined language (und).
                
                row_titles=metadata_schema_dict['rowTitles']
                
                if not isinstance(row_titles,list):
                    row_titles=[row_titles]
                    
                column_indexes=[]
                
                for row_titles_column_name in row_titles:
                    
                    for i,annotated_column_dict in enumerate(annotated_table_dict['columns']):
                        
                        if annotated_column_dict['name']==row_titles_column_name:
                            
                            column_indexes.append(i)
                            
                #            
                for row in annotated_table_dict['rows']:
                    
                    for column_index in column_indexes:
                        
                        value=dict(**row['cells'][column_index]['value'])  # create a copy
                        
                        if value['@type']=='http://www.w3.org/2001/XMLSchema#string':
                            
                            if not '@language' in value:
                                
                                value['@language']='und'
                                
                        else:
                            
                            value['@type']=='http://www.w3.org/2001/XMLSchema#string'
                            value['@language']='und'
                        
                        
                        row['titles'].append(value)
                
      
    #... annotate referenced rows
    #... done here as the cell values are needed.          
      
    for annotated_table_dict,metadata_table_dict \
        in zip(annotated_table_group_dict['tables'],
               metadata_table_group_dict['tables']
               ):
            
        #print('-url',annotated_table_dict['url'])
        
        if 'tableSchema' in metadata_table_dict:
            
            metadata_schema_dict=metadata_table_dict['tableSchema']
        
            if 'foreignKeys' in metadata_schema_dict:
            
                #foreign_key_definitions=metadata_schema_dict['foreignKeys']
                
                for j in range(len(annotated_table_dict['rows'])):
                    
                    #print('j',j)
                    
                    for foreign_key_definition in annotated_table_dict['foreignKeys']:
                        
                        # get foreign key values in this row of this table
                        
                        foreign_key_definition_columns=foreign_key_definition[0]
                        
                        foreign_key_definition_values=\
                            [x['cells'][j]['value'] 
                             for x in foreign_key_definition_columns]
                            
                        #print('foreign_key_definition_values',foreign_key_definition_values)
                            
                        # get rows that matches in the reference table
                        
                        foreign_key_reference_columns=foreign_key_definition[1]
                        foreign_key_reference_table=foreign_key_reference_columns[0]['table']
                        
                        row_indexes=[]
                        
                        for k in range(len(foreign_key_reference_columns[0]['cells'])):
                            
                            #print('k',k)
                            
                            foreign_key_reference_values=\
                                [x['cells'][k]['value'] 
                                 for x in foreign_key_reference_columns]
                                
                            #print('foreign_key_reference_values',foreign_key_reference_values)
                                
                            if foreign_key_reference_values==foreign_key_definition_values:
                                
                                row_indexes.append(k)
                                
                              
                         
                            
                            #print('test')
                            
                        if len(row_indexes)==0:
                            
                            message=f'Columns referenced by foreign key do not contain the value required: {foreign_key_definition_values}'
                            
                            if validate:  
                            
                                raise CSVWError(message)
                                
                            else:
                                
                                warnings.warn(message)
                            
                        elif len(row_indexes)==1:
                            
                            first_row=foreign_key_reference_table['rows'][row_indexes[0]]
                            
                            annotated_table_dict['rows'][j]['referencedRows'].append(
                                [foreign_key_definition,
                                 first_row]
                                )
                            
                        else:
                            
                            message=f'Columns referenced by foreign key do not a unique row with the values required: {foreign_key_definition_values}'
                            
                            if validate:  
                                
                                raise CSVWError(message)
                                
                            else:
                                
                                warnings.warn(message)
                            
                        
      
    #... for testing
    
    # Section 6.6 - Validating tables
    
    
    

    if _save_intermediate_and_final_outputs_to_file:
        with open('annotated_table_group_dict.json','w') as f:
            
            json.dump(
                display_annotated_table_group_dict(annotated_table_group_dict),
                f,
                indent=4
                )
        
    #    
    return annotated_table_group_dict


def display_annotated_table_group_dict(
        annotated_table_group_dict,
        ):
    ""
    return remove_recursion(
        annotated_table_group_dict,
        []
        )


def remove_recursion(
        value,
        ignore_list
        ):
    ""
    if isinstance(value,dict):
        
        d={}
        for k,v in value.items():
            
            
            if k in ignore_list:
                
                d[k]='__removed__'
            
            elif k=='table':
                
                d[k]={'__url__': v['url']}
            
            elif k=='column':
                
                d[k]={'__name__':v['name'],
                      '__table_url__': v['table']['url']
                      }
            
            elif k=='row':
                
                d[k]={'__number__':v['number'],
                      '__table_url__': v['table']['url']
                      }
                
            
            elif k=='primaryKey':
                
                
                d[k]=remove_recursion(v,ignore_list+['cells'])
            
            elif k=='foreignKeys':
                
                d[k]=remove_recursion(v,ignore_list+['cells'])
            
            elif k=='referencedRows':
                
                d[k]={'__len__':len(v)}
                
                d[k]=remove_recursion(v,ignore_list+['referencedRows','cells'])
                
                #pass
                
            else:
                
                d[k]=remove_recursion(v,ignore_list)
                
                
                
        #     elif 
        
        
        # d={k:remove_recursion(v) if not k in 
        #         ['table','column','row',
        #           'referencedRows',
        #           #'primaryKey',
        #           'foreignKeys'] else '__recursion__'
        #         for k,v in value.items()
        #         }
        
        return d
    
    elif isinstance(value,list):
        
        return [remove_recursion(x,ignore_list) for x in value]

    else:
        
        return value


    


#%% 6.3 URL Normalization

def normalize_url(
        url
        ):
    """
    
    'http://example.com/test001.csv' -> 'http://example.com/test001.csv' (same)
    
    'http://example.com/test/../test001.csv' -> 'http://example.com/test001.csv'
    
    'file:///c:/test 001.csv' -> 'file:///c:/test 001.csv'
    
    'file:///c:/test/../test001.csv' -> 'file:///c:/test001.csv'
    
    
    see: https://hyperlink.readthedocs.io/en/latest/api.html?highlight=normalize#hyperlink.URL.normalize
    
    does not percent encode (except for stray percent characters in an already percent encoded url)
    
    
    """
    
    # Metadata Discovery and Compatibility involve comparing URLs. 
    # When comparing URLs, processors must use Syntax-Based Normalization 
    # as defined in [RFC3968]. 
    # Processors must perform Scheme-Based Normalization for HTTP (80) 
    # and HTTPS (443) and should perform Scheme-Based Normalization for 
    # other well-known schemes.
    
    return str(hyperlink.parse(url).normalize())
    

#%% 6.4 Parsing Cells

def parse_cells_in_annotated_column_dict(
        annotated_column_dict,
        trim,
        validate,
        _print_intermediate_outputs=False
        ):
    """
    """
    # Unlike many other data formats, tabular data is designed to be read 
    # by humans. 
    # For that reason, it's common for data to be represented within 
    # tabular data in a human-readable way. 
    # The datatype, default, lang, null, required, and separator annotations 
    # provide the information needed to parse the string value of a cell 
    # into its (semantic) value annotation. 
    # This is used:
    # - by validators to check that the data in the table is in the 
    #   expected format,
    # - by converters to parse the values before mapping them into values 
    #   in the target of the conversion,
    # - when displaying data, to map it into formats that are meaningful 
    #   for those viewing the data (as opposed to those publishing it), and
    # - when inputting data, to turn entered values into representations 
    #   in a consistent format.
    
    # The process of parsing a cell creates a cell with annotations based 
    # on the original string value, parsed value and other column 
    # annotations and adds the cell to the list of cells in a row and 
    # cells in a column:
    # - The raw string value becomes the string value annotation on the cell.
    # - The ordered annotation on the column becomes the ordered annotation 
    #   on the cell.
    # - The text direction annotation on the column becomes the text 
    #   direction annotation on the cell.
    # - The row becomes the row annotation on the cell.
    # - The column becomes the column annotation on the cell.
    
    # After parsing, the cell value can be:
    # - null,
    # - a single value with an associated optional datatype or language, or
    # - a sequence of such values.
    
    # The process of parsing the string value into a single value or a 
    # list of values is as follows:
        
        
    #...set up a function to parse the cell based on the column datatype
    datatype=annotated_column_dict['datatype']
    
    # numbers
    if datatype['base'] in datatypes_numbers:
        
        datatype_parse_function=\
            get_parse_number_function(
                datatype
                )
        
    # booleans
    elif datatype['base']=='boolean':
        
        datatype_parse_function=\
            get_parse_boolean_function(
                datatype
                )
        
    # date
    elif datatype['base']=='date':
        
        datatype_parse_function=\
            get_parse_date_function(
                datatype
                )
        
    # time
    elif datatype['base']=='time':
       
        datatype_parse_function=\
            get_parse_time_function(
                datatype
                )
    
    # datetime
    elif datatype['base'] in ['dateTime', 'datetime']:
        
        datatype_parse_function=\
            get_parse_datetime_function(
                datatype
                )
    
    # datetimestamp
    elif datatype['base']=='dateTimeStamp':
        
        datatype_parse_function=\
            get_parse_datetimestamp_function(
                datatype
                )
        
    # durations
    elif datatype['base'] in ['duration','dayTimeDuration','yearMonthDuration']:
        
        datatype_parse_function=\
            get_parse_duration_function(
                datatype
                )
            
    # other types
    else:
    
       datatype_parse_function=\
           get_parse_other_types_function(
               datatype
               )
        
        
        
        
    for annotated_cell_dict in annotated_column_dict['cells']:
        
        cell_value,errors=\
            parse_cell_steps_1_to_5(
                annotated_cell_dict['stringValue'],
                annotated_column_dict['datatype'],
                annotated_column_dict['default'],
                annotated_column_dict['lang'],
                annotated_column_dict['null'],
                annotated_column_dict['required'],
                annotated_column_dict['separator'],
                datatype_parse_function,
                trim,
                validate
                )
            
        if _print_intermediate_outputs: print(cell_value,errors)
        
        annotated_cell_dict['value']=cell_value
        annotated_cell_dict['errors'].extend(errors)
        
        
def parse_cell_steps_1_to_5(
        string_value,
        datatype,
        default,
        lang,
        null,
        required,
        separator,
        datatype_parse_function,
        trim,
        validate
        ):
    """
    
    :returns: (cell_value, errors)
    :rtype: tuple
    
    """
    #...convert null to list if needed
    if not isinstance(null,list):
        null=[null]
    
    errors=[]
    
    # The process of parsing the string value into a single value or a list 
    # of values is as follows:
        
        
    # 1. unless the datatype base is string, json, xml, html or anyAtomicType, 
    #    replace all carriage return (#xD), line feed (#xA), and tab (#x9) 
    #    characters with space characters.
    if datatype['base'] not in ['string','json','xml','html','anyAtomicType']:
        
        string_value=string_value.replace('\r',' ')
        string_value=string_value.replace('\n',' ')
        string_value=string_value.replace('\t',' ')
        
        
    # 2. unless the datatype base is string, json, xml, html, anyAtomicType, 
    #    or normalizedString, strip leading and trailing whitespace from the 
    #    string value and replace all instances of two or more whitespace #
    #    characters with a single space character.
    if datatype['base'] not in ['string','json','xml','html','anyAtomicType','normalizedString']:
        
        string_value=string_value.strip()
        
        string_value=" ".join(string_value.split()) # includes whitespace such as '\n' etc.
                                                      # if this should be spaces only,  
                                                      # could use st=re.sub(' +',' ', st)
    
    
    # 3. if the normalized string is an empty string, apply the remaining 
    #    steps to the string given by the column default annotation.
    if separator is None and string_value=='':
        
        string_value=str(default)
        
        
    # 4. if the column separator annotation is not null and the normalized 
    #    string is an empty string, the cell value is an empty list. If the 
    #    column required annotation is true, add an error to the list of errors for the cell.
    if not separator is None and string_value=='':
        
        list_of_cell_values=[]
        
        if required:
            
            message='Error in Section 6.4 Step 4. '
            message+='Separator is not null and string value is an empty string. '
            message+='Value set to an empty array. '
            
            errors.append(message)
            
        return list_of_cell_values,errors
        
    
    # 5. if the column separator annotation is not null, the cell value is a 
    #    list of values; set the list annotation on the cell to true, and create 
    #    the cell value created by:
        
    if not separator is None:  # i.e. a list of value expected
        
        #..."set the list annotation on the cell to true"
        #...- not done, as there is no 'list' annotation on the cell object
        
        
        # 5.1. if the normalized string is the same as any one of the values 
        #      of the column null annotation, then the resulting value is null.
        if string_value in null:
            
            cell_value=None
            
            return cell_value, errors
            
        
        # 5.2. split the normalized string at the character specified by the 
        #      column separator annotation.
        else:
            
            list_of_string_values=string_value.split(separator)
                
            
            # 5.3. unless the datatype base is string or anyAtomicType, strip 
            #      leading and trailing whitespace from these strings.  
            if not datatype['base'] not in ['string','anyAtomicType']:
                list_of_string_values=[x.strip() for x in list_of_string_values]
                
                
            # 5.4. applying the remaining steps to each of the strings in turn.
            
            list_of_cell_values=[]
            for string_value in list_of_string_values:
                
                json_value,language,type_,errors=\
                    parse_cell_steps_6_to_9(
                        string_value,
                        errors,
                        datatype,
                        default,
                        lang,
                        null,
                        required,
                        separator,
                        datatype_parse_function,
                        validate
                        )
                
                #...needed for test036 ????????????????
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
                
       
    else:  # i.e. a single value expected         
       
        json_value,language,type_,errors=\
            parse_cell_steps_6_to_9(
                string_value,
                errors,
                datatype,
                default,
                lang,
                null,
                required,
                separator,
                datatype_parse_function,
                validate
                )
            
        if not json_value is None:
            
            cell_value={'@value':json_value,
                        '@type':datatypes[type_]}
                
            if not language is None:
                cell_value['@language']=lang
                
        else:
            
            cell_value=None
            
        return cell_value,errors


def parse_cell_steps_6_to_9(
        string_value,
        errors,
        datatype,
        default,
        lang,
        null,
        required,
        separator,
        datatype_parse_function,
        validate
        ):
    """
    :returns: (json_value, language, value_type, errors)
    :rtype: tuple
    
    """
    # 6. if the string is an empty string, apply the remaining steps to the 
    #    string given by the column default annotation.
    if string_value=='':
        
        string_value=str(default)
        
        
    # 7. if the string is the same as any one of the values of the column null 
    #    annotation, then the resulting value is null. If the column separator 
    #    annotation is null and the column required annotation is true, add 
    #    an error to the list of errors for the cell.
    if string_value in null:
        
        json_value=None
        
        language=None
        
        if separator is None and required==True:
            
            # ADD ERROR
            message='column separator annotation is null and the column required annotation is true'
            
            if validate:
                
                raise CSVWError(message)
            
            else:
            
                warnings.warn(message)
            
                errors.append(message)
        
        return json_value, language, datatype['base'], errors  # returns None
    
    
    # 8. parse the string using the datatype format if one is specified, as 
    #    described below to give a value with an associated datatype. 
    #    If the datatype base is string, or there is no datatype, the value has 
    #    an associated language from the column lang annotation. 
    #    If there are any errors, add them to the list of errors for the cell; 
    #    in this case the value has a datatype of string; if the datatype base 
    #    is string, or there is no datatype, the value has an associated language 
    #    from the column lang annotation.
    if datatype['base']=='string' or datatype['base'] is None:
        
        language=lang
        
    else:
        
        language=None
        
    json_value,value_type,errors=\
        datatype_parse_function(
            string_value,
            errors,
            validate
            )
        
    # if error occurs as value not converted
    if value_type=='string':
        language=lang
    
    
    # 9. validate the value based on the length constraints described in 
    #    section 4.6.1 Length Constraints, the value constraints described 
    #    in section 4.6.2 Value Constraints and the datatype format annotation 
    #    if one is specified, as described below. 
    #    If there are any errors, add them to the list of errors for the cell.
    
    #
    result=\
        check_length_constraints(
            json_value,
            errors,
            datatype['base'],
            validate,
            length=datatype.get('length'),
            minimum_length=datatype.get('minLength'),
            maximum_length=datatype.get('maxLength')
            )
    
    if not result:
        
        language=lang
        
        return string_value, language, 'string', errors
    
    #
    
    if len(errors)==0:  # added 2023-03-01
                        # to solve the issue that if say a string isn't converted to an integer (i.e. "100.0") then it should be checked against minimums and maximums etc.
    
        result=\
            check_value_constraints(
                json_value,
                errors,
                datatype['base'],
                validate,
                minimum=datatype.get('minimum') or datatype.get('minInclusive'),
                maximum=datatype.get('maximum') or datatype.get('maxInclusive'),
                minimum_exclusive=datatype.get('minExclusive'),
                maximum_exclusive=datatype.get('maxExclusive')
                )
        
        if not result:
            
            language=lang
            
            return string_value, language, 'string', errors
    
    return json_value, language,value_type, errors
    

#%% 6.4.2 Formats for numeric type


def get_parse_number_function(
        datatype,
        _print_intermediate_outputs=False
        ):
    """
    """
    
    datatype_format=datatype.get('format')
    
    datatype_base=datatype['base']
    
    # By default, numeric values must be in the formats defined in
    # [xmlschema11-2]. 
    # It is not uncommon for numbers within tabular data to be 
    # formatted for human consumption, which may involve using 
    # commas for decimal points, grouping digits in the number 
    # using commas, or adding percent signs to the number.

    # If the datatype base is a numeric type, the datatype format 
    # annotation indicates the expected format for that number. 
    # Its value must be either a single string or an object with 
    # one or more of the properties:
    
    # decimalChar
    # A string whose value is used to represent a decimal point within the number. 
    # The default value is ".". If the supplied value is not a string, 
    # implementations must issue a warning and proceed as if the property 
    # had not been specified.
    
    if isinstance(datatype_format,dict) and 'decimalChar' in datatype_format:
        
        decimal_char=datatype_format['decimalChar']
        
    else:
        
        decimal_char='.'
    
    
    # groupChar
    # A string whose value is used to group digits within the number. 
    # The default value is null. If the supplied value is not a string, 
    # implementations must issue a warning and proceed as if the property had 
    # not been specified.
    
    if isinstance(datatype_format,dict) and 'groupChar' in datatype_format:
        
        group_char=datatype_format['groupChar']
        
    else:
        
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
    
    # If the datatype format annotation is a single string, this is 
    # interpreted in the same way as if it were an object with a pattern 
    # property whose value is that string.
    
    if not datatype_format is None:
    
        if isinstance(datatype_format,dict):
            
            pattern=datatype_format.get('pattern')
            
        else:  #... datatype_format is a string or None
            
            pattern=datatype_format
            
        #...check if pattern is valid
        if not pattern is None:
            
            for x in pattern:
                
                if not x in ['0','#','.',',','E','+','-','%','‰','\u2030']:
                
                    message=f'Number format pattern with value "{pattern}" is invalid '
                    message+=f'as it contains the character "{x}". '
                    message+='Pattern is set to None.'
                    
                    if _print_intermediate_outputs: print(message)
                    
                    warnings.warn(message)
                    
                    pattern=None
                    
                    break
                
        if not pattern is None:
                
            pattern_dict=\
                parse_LDML_number_pattern(
                    pattern
                    )
                
            #...sets a default group_char if present in the number pattern
            if not pattern_dict['integral_part_primary_grouping_size'] is None \
                or not pattern_dict['fractional_part_grouping_size'] is None:
                
                group_char=','
                
            #print(pattern_dict)
                
    else:
        
        pattern=None
        
    #print('-datatype',datatype)
    #print('-pattern',pattern)
    #print('-decimal_char',decimal_char)
    #print('-group_char',group_char)
        
    
    def parse_number(
            string_value,
            errors,
            validate
            ):
        """
        """
        #print('-string_value',string_value)
        
        def convert_string_value_to_number(
                string_value,
                datatype_base,
                decimal_char,
                group_char
                ):
            """
            
            :raises ValueError: if string_value cannot be converted
            
            :rtype: int or float
            
            """
            #...deals with percent and permille
            modifier=1
            
            if string_value.startswith('%'):
                
                string_value=string_value[1:]
                
                modifier=0.01
            
            elif string_value.endswith('%'):
                
                string_value=string_value[:-1]
                
                modifier=0.01
                
            elif string_value.startswith('‰') or string_value.startswith('\u2030'):
                
                string_value=string_value[1:]
                
                modifier=0.001
                
            elif string_value.endswith('‰') or string_value.endswith('\u2030'):
                
                string_value=string_value[:-1]
                
                modifier=0.001
            
            
            #...replace decimal_char 
            string_value=string_value.replace(decimal_char,'X')
                
            
            #...replace group_char
            if not group_char is None: 
                
                string_value=string_value.replace(group_char,'')
                
            string_value=string_value.replace('X','.')
        
        
            #...convert
            if datatype_base in datatypes_integers:
                
                return int(string_value)*modifier
            
            else:
                
                return float(string_value)*modifier
        
        
        #---
        #...convert the string value to a number
        try:
            
            json_value=convert_string_value_to_number(
                    string_value,
                    datatype_base,
                    decimal_char,
                    group_char
                    )
            conversion_error=False
            
        except ValueError:
            
            json_value=None
            conversion_error=True
            
        #print('-json_value',json_value)
        #print('-conversion_error',conversion_error)
        
        
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
        
        #...DONE BELOW
        
        
        # or that are one of the special values:
        
        # 1. NaN,
        # 2. INF, or
        # 3. -INF.
        
        if string_value in ['NaN','INF','-INF']:
            
            json_value=string_value
            
                
        # Implementations may also recognise numeric values that are in any of the 
        # standard-decimal, standard-percent or standard-scientific formats listed 
        # in the Unicode Common Locale Data Repository.
        
        #...NOT DONE, ALTHOUGH ANY NUMBER THAT PYTHON CAN PARSE WILL WORK
        
    
        # Implementations must add a validation error to the errors annotation 
        # for the cell, and set the cell value to a string rather than a number 
        # if the string being parsed:
    
        # - is not in the format specified in the pattern, if one is defined
        
        if not pattern is None:
            
            result=validate_LDML_number(
                string_value,
                errors,
                pattern_dict,
                decimal_char,
                group_char
                )
            
            if not result:
                
                message=f'Cell value "{string_value}" is not in the format '
                message+=f'specified "{pattern}". '
                message+='Cell value is not converted to a number. '
                
                if validate:
                    
                    raise CSVWError(message)
                    
                else:
                
                    warnings.warn(message)
                    
                    errors.append(message)
                    
                    return string_value, 'string', errors
            
            
        # - otherwise, if the string
        #     - does not meet the numeric format defined above,
        
        else:
            
            if conversion_error:
                
                message=f'Cell string value "{string_value}" is not in a format '
                message+='which can be converted to a number '
                message+=f'of type "{datatype_base}". '
                message+='Cell value is not converted to a number. '
                
                if validate:
                    
                    raise CSVWError(message)
                    
                else:
                
                    warnings.warn(message)
                    
                    errors.append(message)
                    
                    return string_value, 'string', errors
                
            else:
                
                result=\
                    validate_value_for_datatype_base(
                        json_value,
                        errors,
                        datatype_base,
                        validate
                        )
                    
                if not result:
                    
                    return string_value, 'string', errors
            
            
        #     - contains two consecutive groupChar strings,
        if not group_char is None:
    
            if group_char+group_char in string_value:
                
                message=f'Cell value "{string_value}" contains '
                message+='two consecutive groupChar strings. '
                message+='Cell value is not converted to a number. '
                
                if validate:
                    
                    raise CSVWError(message)
                
                else:
                
                    warnings.warn(message)
                    
                    errors.append(message)
                    
                    return string_value, 'string', errors
            

        # - contains the decimalChar, if the datatype base is integer or one of 
        #   its sub-types,
        
        if datatype['base'] in datatypes_integers:
            
            if decimal_char in string_value:
                
                message=f'Cell value "{string_value}" not valid as it '
                message+=f'contains the decimalChar character "{decimal_char}". '
                message+='Cell value is not converted to a number. '
                
                if validate:
                    
                    raise CSVWError(message)
                
                else:
                
                    warnings.warn(message)
                    
                    errors.append(message)
                    
                    return string_value, 'string', errors  
                
            
        # - contains an exponent, if the datatype base is decimal or one of its 
        #   sub-types, or
        
        elif datatype['base'] in datatypes_decimals:
            
            if 'E' in string_value or 'e' in string_value:
                
                message=f'Cell value "{string_value}" not valid as it '
                message+='contains an exponent. '
                message+='Cell value is not converted to a number. '
                
                if validate:
                    
                    raise CSVWError(message)
                
                else:
                
                    warnings.warn(message)
                    
                    errors.append(message)
                    
                    return string_value, 'string', errors  
                
            
        # - is one of the special values NaN, INF, or -INF, if the datatype base 
        #   is decimal or one of its sub-types.
            
            if string_value in ['NaN','INF','-INF']:
                
                message=f'Cell value "{string_value}" not valid as its '
                message+='datatype base is decimal or one of its sub-types. '
                message+='Cell value is not converted to a number. '
                
                if validate:
                    
                    raise CSVWError(message)
                
                else:
                
                    warnings.warn(message)
                    
                    errors.append(message)  
                    
                    return string_value, 'string', errors  
        
        
        # Implementations must use the sign, exponent, percent, and per-mille signs 
        # when parsing the string value of a cell to provide the value of the cell. 
        # For example, the string value "-25%" must be interpreted as -0.25 and 
        # the string value "1E6" as 1000000.
        
        #...this is done in the convert_string_value_to_number function.
        
        
        #
        return json_value,  datatype_base, errors
            
        
    return parse_number


def parse_LDML_number_pattern(
        pattern,
        p=False,
        _print_intermediate_outputs=False
        ):
    """Breaks down a number pattern into constituent components.
    
    :param pattern: A number pattern as specified in the Unicode Locale
        Data Markup Language.
        
    :returns: (positive_subpattern, negative_subpattern)
    
    """
    def zero_and_hash_count(
            pattern2,
            mode, # "integral" or "fractional"
            ):
        ""
        #print('-pattern2',pattern2)
        #print('-mode',mode)
        
        zero_count=0
        hash_count=0
        
        pattern2=pattern2.replace(',','')
        i=0
        end_of_zeros_flag=False
        
        #
        if mode=='integral':
            
            pattern2=pattern2[::-1]
            
        elif mode=='fractional':
            
            pass
        
        else:
            
            raise Exception
        
        #
        while i<len(pattern2):
            
            if pattern2[i]=='0':
                
                if end_of_zeros_flag:
                    
                    raise Exception
                
                else:
                    
                    zero_count+=1
            
            elif pattern2[i]=='#':
                
                end_of_zeros_flag=True
            
                hash_count+=1
            
            else:
                
                raise Exception
                
            i+=1
        
        #
        return zero_count,hash_count
                
    #---
    
    if _print_intermediate_outputs: print('-pattern',pattern)
    
    # prefix
    if pattern[0] in ['%','‰','\u2030']:
        
        prefix=pattern[0]
        pattern_no_prefix=pattern[1:]
        
    else:
        
        prefix=''
        pattern_no_prefix=pattern
        
    #print('-prefix',prefix)
        
    
    # prefix_sign
    if pattern_no_prefix[0] in ['+','-']:
        
        prefix_sign=pattern_no_prefix[0]
        pattern_no_prefix_no_sign=pattern_no_prefix[1:]
        
    else:
        
        prefix_sign=''
        pattern_no_prefix_no_sign=pattern_no_prefix
    
        
    # suffix
    if pattern_no_prefix_no_sign[-1] in ['+','-','%','‰','\u2030']:
        
        suffix=pattern_no_prefix_no_sign[-1]
        pattern_no_prefix_and_suffix=pattern_no_prefix_no_sign[:-1]
        
    else:
        
        suffix=''
        pattern_no_prefix_and_suffix=pattern_no_prefix_no_sign
        
    #print('-suffix',suffix)
    #print('-pattern_no_prefix_and_suffix',pattern_no_prefix_and_suffix)
    
    #...check no invalid prefixes or suffixes remain
    assert pattern_no_prefix_and_suffix[0] in ['#','0']
    assert pattern_no_prefix_and_suffix[-1] in ['#','0']
    
    
    # mantissa and exponent in scientific notation
    mantissa_part, _, exponent_part_with_prefix=\
        pattern_no_prefix_and_suffix.partition('E')
    
    
    # exponent_prefix
    if exponent_part_with_prefix.startswith('+'):
        
        exponent_prefix='+'
        exponent_part=exponent_part_with_prefix[1:]
        
    else:
        
        exponent_prefix=''
        exponent_part=exponent_part_with_prefix
    
    
    # integral and fractional parts
    integral_part, _, fractional_part=\
        mantissa_part.partition('.')
    
    
    # integral grouping size
    reverse_integral_part=integral_part[::-1]
    integral_part_primary_grouping_size=0
    integral_part_secondary_grouping_size=0
    
    positions_of_group_char=[i for i, x 
                             in enumerate(reverse_integral_part) 
                             if x==',']
    
    if len(positions_of_group_char)==0:
        
        integral_part_primary_grouping_size=None
        integral_part_secondary_grouping_size=None
    
    elif len(positions_of_group_char)==1:
        
        integral_part_primary_grouping_size=positions_of_group_char[0]
        integral_part_secondary_grouping_size=integral_part_primary_grouping_size 
        
    else:
        
        integral_part_primary_grouping_size=positions_of_group_char[0]
        integral_part_secondary_grouping_size=\
            positions_of_group_char[1]-positions_of_group_char[0]-1
                
    
    # integral zeros and hashes
    integral_part_zero_padding_count,\
        integral_part_hash_padding_count=\
            zero_and_hash_count(
                    integral_part,
                    mode='integral'
                    )
            
    # fractional grouping size
    positions_of_group_char=[i for i, x 
                             in enumerate(fractional_part) 
                             if x==',']
    
    if len(positions_of_group_char)==0:
        
        fractional_part_grouping_size=None
        
    elif len(positions_of_group_char)==1:
        
        fractional_part_grouping_size=positions_of_group_char[0]
        
    else:
        
        raise Exception
    
    
    # fractional zeros and hashes
    fractional_part_zero_padding_count,\
        fractional_part_hash_padding_count=\
            zero_and_hash_count(
                    fractional_part,
                    mode='fractional'
                    )
    
    
    # exponent zeros and hashes
    exponent_part_zero_padding_count,\
        exponent_part_hash_padding_count=\
            zero_and_hash_count(
                    exponent_part,
                    mode='integral'
                    )
    
    return dict(
        pattern=pattern,
        prefix=\
            prefix,
        prefix_sign=\
            prefix_sign,
        suffix=\
            suffix,
        mantissa_part=\
            mantissa_part,
        integral_part=\
            integral_part,
        integral_part_primary_grouping_size=\
            integral_part_primary_grouping_size,
        integral_part_secondary_grouping_size=\
            integral_part_secondary_grouping_size,
        integral_part_zero_padding_count=\
            integral_part_zero_padding_count,
        integral_part_hash_padding_count=\
            integral_part_hash_padding_count,
        fractional_part=\
            fractional_part,
        fractional_part_grouping_size=\
            fractional_part_grouping_size,
        fractional_part_zero_padding_count=\
            fractional_part_zero_padding_count,
        fractional_part_hash_padding_count=\
            fractional_part_hash_padding_count,
        exponent_prefix=\
            exponent_prefix,
        exponent_part=\
            exponent_part,
        exponent_part_zero_padding_count=\
            exponent_part_zero_padding_count,
        exponent_part_hash_padding_count=\
            exponent_part_hash_padding_count
            )
        
    
def validate_LDML_number(
        string_value,
        errors,
        pattern_dict,
        decimal_char,
        group_char
        ):
    """
    
    :returns: True if valid; otherwise False
    
    """
    try:
        
        prefix, integral_sign, integral_part, fractional_part, exponent_sign, exponent_part, suffix=\
            parse_LDML_number(
                string_value,
                errors,
                decimal_char,
                group_char
                )
            
    except ValueError:
        
        return False
        
    #print((prefix, integral_sign, integral_part, fractional_part, exponent_sign, exponent_part, suffix))
    
    #print(pattern_dict)
        
    
    # prefix
    if not prefix==pattern_dict['prefix']:
            
        message='prefix'
        
        warnings.warn(message)
        
        return False
            
    
    # prefix sign
    if pattern_dict['prefix_sign']:
        
        if not integral_sign==pattern_dict['prefix_sign']:
            
            message='prefix sign'
            
            warnings.warn(message)
            
            return False
    
    
    
    # integral_part
    if not group_char is None:
        
        x=integral_part.replace(group_char,'')
        
    else:
    
        x=integral_part
    
    if len(x)<pattern_dict['integral_part_zero_padding_count']:
        
        message='integral part zero padding'
        
        warnings.warn(message)
        
        return False
    
    # integral part group sizes
    
    #print(pattern_dict['integral_part_primary_grouping_size'])
    #print(pattern_dict['integral_part_secondary_grouping_size'])
    
    x=integral_part[::-1]
    
    # positions_of_group_char=[i for i, y 
    #                          in enumerate(x) 
    #                          if y==(group_char or ',')]
    
    # print(positions_of_group_char)
    
    i=0
    gs1=pattern_dict['integral_part_primary_grouping_size']
    gs2=pattern_dict['integral_part_secondary_grouping_size']
    
    while i<len(x):
        
        #print('i',i, 'x[i]', x[i], 'gs1', gs1, 'gs2', gs2, '(i-gs1)%(gs2+1)', (i-gs1)%(gs2+1) if gs1 else '')
        
        if not gs1 is None and i==gs1:
            
            if not x[i]==(group_char or ','):
                
                message='integral part primary grouping size #1'
                
                #print(message)
                            
                warnings.warn(message)
                
                return False
        
        elif not gs1 is None and not gs2 is None and i>gs1 and \
            (i-gs1)%(gs2+1)==0:  # changed from (gs1-i)%gs2==0:
                
            if not x[i]==(group_char or ','):
                
                #print('x',x)
                
                message='integral part primary grouping size #2'
                
                #print(message)
                            
                warnings.warn(message)
                
                return False
            
        elif x[i]==(group_char or ','):
    
            message='integral part primary grouping size #3'
            
            #print(message)
                        
            warnings.warn(message)
            
            return False
    
        i+=1
    
    
    # if not pattern_dict['integral_part_primary_grouping_size'] is None:
        
    #     try:
            
    #         if not x[pattern_dict['integral_part_primary_grouping_size']]\
    #             ==(group_char or ','):
                
    #             message='integral part primary grouping size'
                
    #             warnings.warn(message)
                
    #             return False
    
    #     except IndexError:
            
    #         pass
    
    
    
    
    
    
    
    
    
    
    
        
    # fractional_part
    
    if not group_char is None:
        
        x=fractional_part.replace(group_char,'')
        
    else:
    
        x=fractional_part
    
    if len(x)<pattern_dict['fractional_part_zero_padding_count']:
        
        message='fractional part zero padding'
        
        warnings.warn(message)
        
        return False
        
    if len(x)> \
        pattern_dict['fractional_part_zero_padding_count']\
            + pattern_dict['fractional_part_hash_padding_count']:
        
        message='fractional part hash padding'
        
        warnings.warn(message)
        
        return False
    
    
    # exponent prefix
    if exponent_sign=='+':
        
        if not pattern_dict['exponent_prefix']=='+':
            
            message='exponent prefix'
            
            warnings.warn(message)
            
            return False
        
    
    # exponent part
    
    if not group_char is None:
        
        x=exponent_part.replace(group_char,'')
        
    else:
    
        x=exponent_part
    
    if len(x)<pattern_dict['exponent_part_zero_padding_count']:
        
        message='exponent part'
        
        warnings.warn(message)
        
        return False
    
    
    #
    return True
    
    
def parse_LDML_number(
        string_value,
        errors,
        decimal_char,
        group_char,
        _print_intermediate_outputs=False
        ):
    """
    
    :returns: (prefix, 
              integral_sign,
            integral_part, 
            fractional_part, 
            exponent_prefix, 
            exponent_part, 
            suffix)
    
    
    """
    
    # prefix
    if string_value[0] in ['%','‰','\u2030']:
        
        prefix=string_value[0]
        string_value_no_prefix=string_value[1:]
        
    else:
        
        prefix=''
        string_value_no_prefix=string_value
        
    #print('-prefix',prefix)
    #print('-string_value_no_prefix',string_value_no_prefix)
        
        
    # integral sign
    if string_value_no_prefix[0] in ['+','-']:
        
        integral_sign=string_value_no_prefix[0]
        string_value_no_prefix_no_sign=string_value_no_prefix[1:]
        
    else:
        
        integral_sign=''
        string_value_no_prefix_no_sign=string_value_no_prefix
    
        
    # suffix
    if string_value_no_prefix_no_sign[-1] in ['%','‰','\u2030']:
        
        suffix=string_value_no_prefix_no_sign[-1]
        string_value_no_prefix_no_sign_no_suffix=string_value_no_prefix_no_sign[:-1]
        
    else:
        
        suffix=''
        string_value_no_prefix_no_sign_no_suffix=string_value_no_prefix_no_sign
    
    if _print_intermediate_outputs: print('-string_value_no_prefix_no_sign_no_suffix',string_value_no_prefix_no_sign_no_suffix)
    
    #...check start
    if not string_value_no_prefix_no_sign_no_suffix[0] in ['0','1','2','3','4','5','6','7','8','9']:
        
        message='start does not contain a number or sign'
            
        errors.append(message)
            
        raise ValueError
        
        
    #...check end
    if not string_value_no_prefix_no_sign_no_suffix[-1] in ['0','1','2','3','4','5','6','7','8','9']:
        
        message='end does not contain a number'
            
        errors.append(message)
            
        raise ValueError
    
    
    # mantissa and exponent in scientific notation
    mantissa_part, _, exponent_part=\
        string_value_no_prefix_no_sign_no_suffix.partition('E')
    
    
    # integral and fractional parts
    integral_part, _, fractional_part=\
        mantissa_part.partition(decimal_char)
        
    #print('-integral_part',integral_part)
        
        
    # exponent sign
    if exponent_part and exponent_part[0] in ['+','-']:
        
        exponent_sign=exponent_part[0]
        exponent_part_no_sign=exponent_part[1:]
        
    else:
        
        exponent_sign=''
        exponent_part_no_sign=exponent_part
        
        
    #...check integral part
    for x in integral_part:
        
        if not x in ['0','1','2','3','4','5','6','7','8','9',group_char]:
            
            message='error in integral part'
            
            errors.append(message)
            
            raise ValueError
        
        
    #...check fractional part
    for x in fractional_part:
        
        if not x in ['0','1','2','3','4','5','6','7','8','9',group_char]:
            
            message='error in fractional part'
            
            errors.append(message)
            
            raise ValueError
        
        
    #...check exponent part
    for x in exponent_part_no_sign:
        
        if not x in ['0','1','2','3','4','5','6','7','8','9']:
            
            message='error in exponent part'
            
            errors.append(message)
            
            raise ValueError
        
    
    #
    return (prefix, 
            integral_sign,
            integral_part, 
            fractional_part, 
            exponent_sign, 
            exponent_part, 
            suffix)
    
    
    
            
    
        
        

#%% 6.4.3 Formats for booleans

def get_parse_boolean_function(
        datatype
        ):
    """
    """
    # Boolean values may be represented in many ways aside from the standard 
    # 1 and 0 or true and false.
    
    # If the datatype base for a cell is boolean, the datatype format 
    # annotation provides the true value followed by the false value, 
    # separated by |. 
    # For example if format is Y|N then cells must hold either Y or N with 
    # Y meaning true and N meaning false. 
    # If the format does not follow this syntax, implementations must 
    # issue a warning and proceed as if no format had been provided.
    
    # The resulting cell value will be one or more boolean true or false values.
    
    datatype_base=datatype['base']
    
    datatype_format=datatype.get('format')
    
    true_values=['1','true']
    
    false_values=['0','false']

    
    #
    if isinstance(datatype_format,str):
    
        x=datatype_format.split('|')
        
        if not len(x)==2:
            
            message='Boolean format "{datatype_format}" must contain exactly one "|" character.'
            
            warnings.warn(message)
            
    
        elif x[0]==x[1]:
                
            message='Boolean format "{datatype_format}" must contain different string values for true and false. '
            
            warnings.warn(message)
                    
        else:
            
            true_values=[x[0]]
            
            false_values=[x[1]]
            
    else:
        
        message='Boolean format "{datatype_format}" must be a string.'
        
        warnings.warn(message)
            
            
    #print('-true_values',true_values)
    #print('-false_values',false_values)


    def parse_boolean(
            string_value,
            errors,
            validate
            ):
        """
        """
        if string_value in true_values:
            
            json_value=True
            
        elif string_value in false_values:
            
            json_value=False
            
        else:
            
            message=f'Boolean value "{string_value}" does not match either '
            message+=f'the true values "{true_values}" '
            message+=f'or false values "{false_values}". '
            message+='Boolean value is not converted to boolean, string value returned.'
            
            if validate:
                
                raise CSVWError(message)
            
            else:
            
                errors.append(message)
                
                warnings.warn(message)
                
                return string_value, 'string', errors
        
        
        #
        return json_value, datatype_base, errors
        
        
        
          
    return parse_boolean
      
 
#%% 6.4.4. Formats for dates and times

def get_timezone_format(
        datatype_format
        ):
    """
    """
    x=datatype_format#  .strip()
    
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
        timezone_gap=None
    
    #
    if not timezone_format is None:
        
        x=len(timezone_format)
        
        if datatype_format[-x-1]==' ':
            
            timezone_gap=' '
            
        else:
            
            timezone_gap=''
    
    #
    return timezone_format,timezone_gap


def get_timezone_string(
        string_value,
        timezone_format
        ):
    """
    """
    x=string_value#  .strip()
    
    if timezone_format is None:
    
        if x.endswith('Z'):
            
            return 'Z'
        
        elif len(x)>5 and x[-3]==':' and x[-6] in ['+','-']:
        
            return x[-6:]
        
        else:
            
            return ''
    
    
    else:
    
        if timezone_format=='':
            
            return ''
        
        
        elif timezone_format=='X':
            
            if string_value.endswith('Z'):
                
                return 'Z'
        
            elif x[-3] in ['+','-']:
                
                return x[-3:]
            
            elif x[-5] in ['+','-']:
                
                return x[-5:]
            
            else:
                
                raise ValueError
            
    
        elif timezone_format=='XX':
            
            if string_value.endswith('Z'):
                
                return 'Z'
        
            elif x[-5] in ['+','-']:
                
                return x[-5:]
            
            else:
                
                raise ValueError
        
        
        elif timezone_format=='XXX':
            
            if string_value.endswith('Z'):
                
                return 'Z'
        
            elif x[-6] in ['+','-']:
                
                return x[-6:]
            
            else:
                
                raise ValueError
                
        
        elif timezone_format=='x':
            
            if x[-3] in ['+','-']:
                
                return x[-3:]
            
            elif x[-5] in ['+','-']:
                
                return x[-5:]
            
            else:
                
                raise ValueError
                
        
        elif timezone_format=='xx':
            
            if x[-5] in ['+','-']:
                
                return x[-5:]
            
            else:
                
                raise ValueError
        
        
        elif timezone_format=='xxx':
            
            if x[-6] in ['+','-']:
                
                return x[-6:]
            
            else:
                
                raise ValueError
        
        
        else:
            
            raise Exception
        

def parse_timezone_string(
        timezone_string
        ):
    ""
    
    if timezone_string=='':
        
        return ''
    
    elif timezone_string=='Z':
        
        return 'Z'
    
    elif len(timezone_string)==3:
        
        sign=timezone_string[0]
        hours=int(timezone_string[1:])
        minutes=0
        
    elif len(timezone_string)==5:
        
        sign=timezone_string[0]
        hours=int(timezone_string[1:3])
        minutes=int(timezone_string[3:])
        
    elif len(timezone_string)==6:
        
        sign=timezone_string[0]
        hours=int(timezone_string[1:3])
        minutes=int(timezone_string[4:])
        
    else:
        
        raise ValueError
    
    #    
    if not sign in ['+','-']:
        
        raise ValueError
        
    if hours<0 or hours>23:
        
        raise ValueError
        
    if minutes<0 or minutes>59:
        
        raise ValueError
        
    return f'{sign}{hours:02}:{minutes:02}'


def get_parse_date_function(
        datatype,
        _print_intermediate_outputs=False
        ):
    """
    """
    # By default, dates and times are assumed to be in the format defined 
    # in [xmlschema11-2]. However dates and times are commonly represented 
    # in tabular data in other formats.
    
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
    
    # NOTE
    # For interoperability, authors of metadata documents should use 
    # only the formats listed in this section.
    
    # The following date format patterns must be recognized by implementations:
    # yyyy-MM-dd e.g., 2015-03-22
    # yyyyMMdd e.g., 20150322
    # dd-MM-yyyy e.g., 22-03-2015
    # d-M-yyyy e.g., 22-3-2015
    # MM-dd-yyyy e.g., 03-22-2015
    # M-d-yyyy e.g., 3-22-2015
    # dd/MM/yyyy e.g., 22/03/2015
    # d/M/yyyy e.g., 22/3/2015
    # MM/dd/yyyy e.g., 03/22/2015
    # M/d/yyyy e.g., 3/22/2015
    # dd.MM.yyyy e.g., 22.03.2015
    # d.M.yyyy e.g., 22.3.2015
    # MM.dd.yyyy e.g., 03.22.2015
    # M.d.yyyy e.g., 3.22.2015
    
    # Implementations must also recognise date, time, and date/time format patterns that end with timezone markers consisting of between one and three x or X characters, possibly after a single space. These must be interpreted as follows:

    # X e.g., -08, +0530, or Z (minutes are optional)
    # XX e.g., -0800, +0530, or Z
    # XXX e.g., -08:00, +05:30, or Z
    # x e.g., -08 or +0530 (Z is not permitted)
    # xx e.g., -0800 or +0530 (Z is not permitted)
    # xxx e.g., -08:00 or +05:30 (Z is not permitted)
    
    # For example, date format patterns could include yyyy-MM-ddTHH:mm:ssXXX 
    # for 2015-03-15T15:02:37Z or 2015-03-15T15:02:37-05:00, or HH:mm x for 15:02 -05.
    
    # The cell value will one or more dates/time values extracted using the format.
    
    # NOTE
    # For simplicity, this version of this standard does not support 
    # abbreviated or full month or day names, or double digit years. 
    # Future versions of this standard may support other date and time 
    # formats, or general purpose date/time pattern strings. 
    # Authors of schemas should use appropriate regular expressions, along 
    # with the string datatype, for dates and times that use a format other 
    # than that specified here.
    
    if _print_intermediate_outputs: print('-datatype',datatype)
    
    datatype_format=datatype.get('format')
    
    if not datatype_format is None and not isinstance(datatype_format,str):
        
        message='datatype format'
        
        warnings.warn(message)
        
        datatype_format=None
    
    
    if not datatype_format is None:
        
        timezone_format,timezone_gap=\
            get_timezone_format(
                    datatype_format
                    )
        
        #
        if not timezone_format is None:
        
            date_format=\
                datatype_format.removesuffix(
                    timezone_gap+timezone_format
                    )
                
        else:
            
            date_format=datatype_format
            
        #
        if not date_format in [
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
            
            message='date format'
            
            warnings.warn(message)
            
            date_format=None
            
    else:
        
        timezone_format=None
        timezone_gap=''
        date_format=None
            
        
    if not date_format is None:
    
        #...get separator
        if '-' in date_format:
            
            separator='-'
            
        elif '/' in date_format:
            
            separator='/'
            
        elif '.' in date_format:
            
            separator='.'
            
        else:
            
            separator=''
            
    else:
        
        separator=None
        
    
    #...get separated codes
    if not date_format is None:
    
        if date_format=='yyyyMMdd':
            
            separated_codes=['yyyy','MM','dd']
            
        else:
            
            separated_codes=date_format.split(separator)
            
    else:
        
        separated_codes=None
    
        
    if _print_intermediate_outputs: print('-timezone_format',timezone_format)
    if _print_intermediate_outputs: print('-timezone_gap',timezone_gap)
    if _print_intermediate_outputs: print('-date_format',date_format)
    if _print_intermediate_outputs: print('-separator',separator)
    if _print_intermediate_outputs: print('-separated_codes',separated_codes)
    
    
    def parse_date(
            string_value,
            errors,
            validate
            ):
        """
        """
        if _print_intermediate_outputs: print('-string_value',string_value)
        
        #
        try:
            
            timezone_string=\
                get_timezone_string(
                    string_value,
                    timezone_format
                    )
        
        except ValueError:
            
            message='date conversion error'
            
            if validate:
                
                raise CSVWError(message)
                
            else:
            
                errors.append(message)
                warnings.warn(message)
                return string_value,'string',errors
            
        if _print_intermediate_outputs: print('-timezone_string',timezone_string)
        
        #
        xsd_timezone_string=\
            parse_timezone_string(
                timezone_string
                )
        if _print_intermediate_outputs: print('-xsd_timezone_string',xsd_timezone_string)
            
        #
        date_string=\
            string_value.removesuffix(
                (timezone_gap or '')
                +timezone_string
                )
        
        if _print_intermediate_outputs: print('-date_string',date_string)
        
        if not date_format is None:
            
            #
            if separator=='':
                
                separated_date_string=\
                    (
                        date_string[:4],
                        date_string[4:6],
                        date_string[6:]
                     )
            
            else:
                
                separated_date_string=date_string.split(separator)
                
            if _print_intermediate_outputs: print('-separated_date_string',separated_date_string)
                
            #
            for code, date_substring in zip(
                    separated_codes,
                    separated_date_string
                    ):
                
                if code=='yyyy':
                    
                    if not len(date_substring)==4:
                        
                        message='date conversion error'
                        
                        if validate:
                            
                            raise CSVWError(message)
                            
                        else:
                            
                            errors.append(message)
                            warnings.warn(message)
                            return string_value,'string',errors
                    
                    try:
                        
                        year=int(date_substring)
                    
                    except ValueError:
                        
                        message='date conversion error'
                        
                        if validate:
                            
                            raise CSVWError(message)
                            
                        else:
                            
                            errors.append(message)
                            warnings.warn(message)
                            return string_value,'string',errors
                    
                elif code=='MM':
                    
                    if not len(date_substring)==2:
                        
                        message='date conversion error'
                        
                        if validate:
                            
                            raise CSVWError(message)
                            
                        else:
                            
                            errors.append(message)
                            warnings.warn(message)
                            return string_value,'string',errors
                    
                    try:
                    
                        month=int(date_substring)
                    
                    except ValueError:
                        
                        message='date conversion error'
                        
                        if validate:
                            
                            raise CSVWError(message)
                            
                        else:
                            
                            errors.append(message)
                            warnings.warn(message)
                            return string_value,'string',errors
                    
                    
                elif code=='M':
                    
                    try:
                    
                        month=int(date_substring)
                    
                    except ValueError:
                        
                        message='date conversion error'
                        
                        if validate:
                            
                            raise CSVWError(message)
                            
                        else:
                            
                            errors.append(message)
                            warnings.warn(message)
                            return string_value,'string',errors
                    
                    if month<10:
                        
                        if not len(date_substring)==1:
                            
                            message='date conversion error'
                            
                            if validate:
                                
                                raise CSVWError(message)
                                
                            else:
                                
                                errors.append(message)
                                warnings.warn(message)
                                return string_value,'string',errors
                        
                    else:
                        
                        if not len(date_substring)==2:
                            
                            message='date conversion error'
                            
                            if validate:
                                
                                raise CSVWError(message)
                                
                            else:
                                
                                errors.append(message)
                                warnings.warn(message)
                                return string_value,'string',errors
                    
                    
                        
                elif code=='dd':
                    
                    if not len(date_substring)==2:
                        
                        message='date conversion error'
                        
                        if validate:
                            
                            raise CSVWError(message)
                            
                        else:
                            
                            errors.append(message)
                            warnings.warn(message)
                            return string_value,'string',errors
                    
                    try:
                    
                        day=int(date_substring)
                    
                    except ValueError:
                        
                        message='date conversion error'
                        
                        if validate:
                            
                            raise CSVWError(message)
                            
                        else:
                            
                            errors.append(message)
                            warnings.warn(message)
                            return string_value,'string',errors
                    
                    
                elif code=='d':
                    
                    try:
                    
                        day=int(date_substring)
                        
                    except ValueError:
                        
                        message='date conversion error'
                        
                        if validate:
                            
                            raise CSVWError(message)
                            
                        else:
                            
                            errors.append(message)
                            warnings.warn(message)
                            return string_value,'string',errors
                    
                    if day<10:
                        
                        if not len(date_substring)==1:
                            
                            message='date conversion error'
                            
                            if validate:
                                
                                raise CSVWError(message)
                                
                            else:
                                
                                errors.append(message)
                                warnings.warn(message)
                                return string_value,'string',errors
                        
                    else:
                        
                        if not len(date_substring)==2:
                            
                            message='date conversion error'
                            
                            if validate:
                                
                                raise CSVWError(message)
                                
                            else:
                                
                                errors.append(message)
                                warnings.warn(message)
                                return string_value,'string',errors
                        
                    
                else:
                    
                    raise Exception
                    
            if _print_intermediate_outputs: print('-year,month,day',year,month,day)
            
            
            try:
        
                x=datetime.date(year,month,day)
        
            except ValueError:
                
                message='Invalid value in date string.'
        
                if validate:
                    
                    raise CSVWError(message)
                    
                else:
        
                    errors.append(message)
                    warnings.warn(message)
                    return string_value, 'string', errors
        
        
        else:
            
            try:
            
                x=datetime.date.fromisoformat(date_string)
        
            except ValueError:
                
                message=f'Invalid isoformat string {string_value} for "date" datatype. '
        
                if validate:
                    
                    raise CSVWError(message)
                    
                else:
        
                    errors.append(message)
                    warnings.warn(message)
                    return string_value, 'string', errors
        
    
        #
        json_value=x.isoformat()+xsd_timezone_string
        
        return json_value, datatype['base'], errors
    
    
    return parse_date
    

    
def get_parse_time_function(
        datatype,
        _print_intermediate_outputs=False
        ):
    """
    """
    # By default, dates and times are assumed to be in the format defined 
    # in [xmlschema11-2]. However dates and times are commonly represented 
    # in tabular data in other formats.

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
    
    if _print_intermediate_outputs: print('-datatype',datatype)
    
    datatype_format=datatype.get('format')
    
    if not datatype_format is None:
        
        timezone_format,timezone_gap=\
            get_timezone_format(
                    datatype_format
                    )
        
        if not timezone_format is None:
        
            time_format=\
                datatype_format.removesuffix(
                    timezone_gap+timezone_format
                    )
        else:
            
            time_format=datatype_format

            
        #...separate main and fractional part
        main_time_format,_,fractional_time_format=\
            time_format.partition('.')
                
        #...check if main time format is in the approved list
        if main_time_format in [
            'HH:mm:ss',
            'HHmmss',
            'HH:mm',
            'HHmm'
            ]:
            
            #...check fractional time format
            if fractional_time_format:
                
                if not main_time_format=='HH:mm:ss':
                    
                    message='fractional time format1'
                    
                    warnings.warn(message)
                    
                    time_format=None
                    main_time_format=None
                    fractional_time_format=None
                
                
                if not all(x=='S' for x in fractional_time_format):
                    
                    message='fractional time format2'
                    
                    warnings.warn(message)
                    
                    time_format=None
                    main_time_format=None
                    fractional_time_format=None
            
        else:
        
            message='main time format'
            
            warnings.warn(message)
            
            time_format=None
            main_time_format=None
            fractional_time_format=None
            
            
    else:
        
        time_format=None
        timezone_format=None
        timezone_gap=None
        main_time_format=None
        fractional_time_format=None
        
    
    if _print_intermediate_outputs: print('-timezone_format',timezone_format)
    if _print_intermediate_outputs: print('-timezone_gap',timezone_gap)
    if _print_intermediate_outputs: print('-time_format',time_format)
    if _print_intermediate_outputs: print('-main_time_format',main_time_format)
    if _print_intermediate_outputs: print('-fractional_time_format',fractional_time_format)
    
    
    def parse_time(
            string_value,
            errors,
            validate
            ):
        """
        """
        if _print_intermediate_outputs: print('-string_value',string_value)
        
        #
        try:
            
            timezone_string=\
                get_timezone_string(
                    string_value,
                    timezone_format
                    )
        
        except ValueError:
            
            message='time conversion error'
            
            if validate:
                
                raise CSVWError(message)
            
            else:
                
                errors.append(message)
                warnings.warn(message)
                return string_value,'string',errors
                
        if _print_intermediate_outputs: print('-timezone_string',timezone_string)
        
        #
        xsd_timezone_string=\
            parse_timezone_string(
                timezone_string
                )
        if _print_intermediate_outputs: print('-xsd_timezone_string',xsd_timezone_string)
            
        #
        time_string=\
            string_value.removesuffix(
                (timezone_gap or '')
                +timezone_string
                )
        
        if _print_intermediate_outputs: print('-time_string',time_string)
        
        
        #
        if not time_format is None:
            
            
            #
            main_time_string,_,fractional_time_string=\
                time_string.partition('.')
                
            if main_time_format=='HH:mm:ss':
                
                try:
                
                    hour,minute,second=[int(x) for x in main_time_string.split(':')]
                
                except ValueError:
                    
                    message=f'time conversion error "{time_string}"'
                    
                    if validate:
                        
                        raise CSVWError(message)
                    
                    else:
                        
                        errors.append(message)
                        warnings.warn(message)
                        return string_value,'string',errors
                
                
                
            elif main_time_format=='HHmmss':
                
                try:
                
                    hour=int(main_time_string[:2])
                    minute=int(main_time_string[2:4])
                    second=int(main_time_string[4:])
                
                except ValueError:
                    
                    message='time conversion error'
                    
                    if validate:
                        
                        raise CSVWError(message)
                    
                    else:
                        
                        errors.append(message)
                        warnings.warn(message)
                        return string_value,'string',errors
                
                
            elif main_time_format=='HH:mm':
                
                try:
                
                        hour,minute=[int(x) for x in main_time_string.split(':')]
                
                except ValueError:
                
                    message='time conversion error'
                    
                    if validate:
                        
                        raise CSVWError(message)
                    
                    else:
                        
                        errors.append(message)
                        warnings.warn(message)
                        return string_value,'string',errors
                
                second=0
                
            elif main_time_format=='HHmm':
                
                try:
                    
                    hour=int(main_time_string[:2])
                    minute=int(main_time_string[2:4])
                
                except ValueError:
                    
                    message='time conversion error'
                    
                    if validate:
                        
                        raise CSVWError(message)
                    
                    else:
                        
                        errors.append(message)
                        warnings.warn(message)
                        return string_value,'string',errors
                
                second=0
                
            if fractional_time_format:
                
                if len(fractional_time_string)>len(fractional_time_format):
                    
                    message='time conversion error'
                    
                    if validate:
                        
                        raise CSVWError(message)
                    
                    else:
                        
                        errors.append(message)
                        warnings.warn(message)
                        return string_value,'string',errors
                
                else:
                
                    try:    
                    
                        microsecond=\
                            int(float(f'0.{fractional_time_string}')*1000000)
                            
                    except ValueError:
                        
                        message='time conversion error'
                        
                        if validate:
                            
                            raise CSVWError(message)
                        
                        else:
                            
                            errors.append(message)
                            warnings.warn(message)
                            return string_value,'string',errors
                        
            else:
                
                microsecond=0
                
            x=datetime.time(hour,minute,second,microsecond)
            
        else:
            
            try:
            
                x=datetime.time.fromisoformat(time_string)
        
            except ValueError:
                
                message=f'Invalid isoformat string {string_value}. '
                
                if validate:
                    
                    raise CSVWError(message)
                
                else:
        
                    errors.append(message)
                    warnings.warn(message)
                    return string_value, 'string', errors
        
        #
        x=x.isoformat()
        
        #...format significant digits if needed
        if fractional_time_format:
            
            x=x.split('.')
            
            x=x[0]+'.'+x[1][:len(fractional_time_format)]
        
        json_value=x+xsd_timezone_string
        
        return json_value, datatype['base'], errors
    
    
    return parse_time


def get_parse_datetime_function(
        datatype,
        timezone_required=False,
        _print_intermediate_outputs=False
        ):
    """
    """
    # By default, dates and times are assumed to be in the format defined 
    # in [xmlschema11-2]. However dates and times are commonly represented 
    # in tabular data in other formats.
    
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
    
    if _print_intermediate_outputs: print('-datatype',datatype)
    
    datatype_format=datatype.get('format')
    
    if not datatype_format is None:
        
        timezone_format,timezone_gap=\
            get_timezone_format(
                    datatype_format
                    )
            
        if not timezone_format is None:
        
            datetime_format=\
                datatype_format.removesuffix(
                    timezone_gap+timezone_format
                    )
                
        else:
            
            datetime_format=datatype_format
            
    else:
        
        datetime_format=None
        timezone_format=None
        timezone_gap=None
        
        
    if not datetime_format is None:
        
        #
        if 'T' in datetime_format:
            
            separator='T'
            
        else:
            
            separator=' '
            
        #
        date_format,_,time_format=\
            datetime_format.partition(separator)
            
    else:
        
        date_format=None
        time_format=None
        separator=None
            
    
            
        
    if _print_intermediate_outputs: print('-timezone_format',timezone_format)
    if _print_intermediate_outputs: print('-timezone_gap',timezone_gap)
    if _print_intermediate_outputs: print('-datetime_format',datetime_format)
    if _print_intermediate_outputs: print('-separator',separator)
    if _print_intermediate_outputs: print('-date_format',date_format)
    if _print_intermediate_outputs: print('-time_format',time_format)
    
    

    def parse_datetime(
            string_value,
            errors,
            validate
            ):
        """
        """
        if _print_intermediate_outputs: print('-string_value',string_value)
        
        #
        try:
            
            timezone_string=\
                get_timezone_string(
                    string_value,
                    timezone_format
                    )
        
        except ValueError:
            
            message='datetime conversion error 1'
            errors.append(message)
            warnings.warn(message)
            return string_value,'string',errors
                
        if _print_intermediate_outputs: print('-timezone_string',timezone_string)
        
        #
        if timezone_required and timezone_string=='':
            
            message='timezone_required'
            
            errors.append(message)
            
            warnings.warn(message)
            
            return string_value, 'string', errors
            
        #
        xsd_timezone_string=\
            parse_timezone_string(
                timezone_string
                )
        if _print_intermediate_outputs: print('-xsd_timezone_string',xsd_timezone_string)
            
        #
        datetime_string=\
            string_value.removesuffix(
                (timezone_gap or '')
                +timezone_string
                )
        
        if _print_intermediate_outputs: print('-datetime_string',datetime_string)
        
        
        if not datetime_format is None:
            
            date_string, _, time_string =\
                datetime_string.partition(separator)
                
            #
            date_datatype=\
                {'base':'date',
                 'format':date_format}
                
            date_errors=[]
                
            date_json_value,_,date_errors=\
                get_parse_date_function(
                    date_datatype
                    )(date_string,
                      date_errors,
                      validate)
                      
            errors.extend(date_errors)
            
            #
            time_datatype=\
                {'base':'time',
                 'format':time_format}
                
            time_errors=[]
                
            time_json_value,_,time_errors=\
                get_parse_time_function(
                    time_datatype
                    )(time_string,
                      time_errors,
                      validate)
            
            errors.extend(time_errors)
                      
            if len(date_errors)>0 or len(time_errors)>0:
                
                return string_value,'string',errors
                      
            #

            try:
            
                x=dateutil.parser.isoparse(
                    f'{date_json_value}T{time_json_value}'
                    )
            
            except ValueError:
                
                message='datetime conversion error 2' + ' -- ' + f'{date_json_value}T{time_json_value}'
                #print(message)
                errors.append(message)
                warnings.warn(message)
                return string_value,'string',errors
            
            #
            json_value=f'{date_json_value}T{time_json_value}'+xsd_timezone_string
            
            
        else:
            
            x=dateutil.parser.isoparse(datetime_string)
            
            json_value=x.isoformat()+xsd_timezone_string
            
            
        #
        #print('json_value',json_value)
        return json_value, datatype['base'], errors
        
        
    return parse_datetime
    

def get_parse_datetimestamp_function(
        datatype
        ):
    """
    """
    #...timezone is required
    
    return get_parse_datetime_function(
        datatype,
        timezone_required=True
        )


#%% 6.4.5 Formats for durations

def get_parse_duration_function(
        datatype,
        _print_intermediate_outputs=False
        ):
    """
    """
    # Durations must be formatted and interpreted as defined in 
    # [xmlschema11-2], using the [ISO8601] format -?PnYnMnDTnHnMnS. 
    # For example, the duration P1Y1D is used for a year and a day; 
    # the duration PT2H30M for 2 hours and 30 minutes.

    # If the datatype base is a duration type, the datatype format 
    # annotation provides a regular expression for the string values, 
    # with syntax and processing defined by [ECMASCRIPT]. 
    # If the supplied value is not a valid regular expression, 
    # implementations must issue a warning and proceed as if no format 
    # had been provided.
    
    # NOTE
    # Authors are encouraged to be conservative in the regular expressions 
    # that they use, sticking to the basic features of regular expressions 
    # that are likely to be supported across implementations.
    
    # The cell value will be one or more durations extracted using the format.
        
    if _print_intermediate_outputs: print('-datatype',datatype)
    
    datatype_format=datatype.get('format')
    
    if not datatype_format is None:
    
        try:
            
            re_compiled=re.compile(datatype['format'])
            
        except re.error:
            
            message='Property "format" with value "{datatype["format"]}" '
            message+='is not a valid regular expression. '
            message+='Property is removed. '
            
            warnings.warn(message)
            
            re_compiled=None
            
    else:
        
        re_compiled=None
            
    if _print_intermediate_outputs: print('-re_compiled', re_compiled)
    
    
    def parse_duration(
            string_value,
            errors,
            validate
            ):
        ""
        if _print_intermediate_outputs: print('-string_value',string_value)

        if not re_compiled is None:
            
            if not re_compiled.search(string_value):
                
                message=f'duration conversion error 1 - string value ="{string_value}"'
                
                if validate:
                    
                    raise CSVWError(message)
                    
                else:
                    
                    errors.append(message)
                    warnings.warn(message)
                    return string_value, 'string', errors

        #...parsing -?PnYnMnDTnHnMnS
        x=string_value
        
        # minus sign
        
        if x[0]=='-':
            
            x=x.removeprefix('-')
        
        #P
        
        if not x[0]=='P':
            
            message=f'duration conversion error 2 - string value ="{string_value}"'
            
            if validate:
                
                raise CSVWError(message)
                
            else:
                
                errors.append(message)
                warnings.warn(message)
                return string_value, 'string', errors
        
        x=x.removeprefix('P')
        
        #Y
        if 'Y' in x:
            
            year_string,_,x=x.partition('Y')
            
            try:
                
                int(year_string)
                
            except ValueError:
                
                message=f'duration conversion error 3 - string value ="{string_value}"' + f'"{year_string}"'
                
                if validate:
                    
                    raise CSVWError(message)
                    
                else:
                    
                    errors.append(message)
                    warnings.warn(message)
                    return string_value, 'string', errors
            
            
        #M
        if 'M' in x.split('T')[0]:
        
            month_string,_,x=x.partition('M')
            
            if month_string:
                
                try:
                    
                    int(month_string)
                    
                except ValueError:
                    
                    message=f'duration conversion error 4 - string value ="{string_value}"'
                    
                    if validate:
                        
                        raise CSVWError(message)
                        
                    else:
                        
                        errors.append(message)
                        warnings.warn(message)
                        return string_value, 'string', errors
        
        
        #D
        if 'D' in x:
        
            day_string,_,x=x.partition('D')
            
            if day_string:
                
                try:
                    
                    int(day_string)
                    
                except ValueError:
                    
                    message=f'duration conversion error 5 - string value ="{string_value}"'
                    
                    if validate:
                        
                        raise CSVWError(message)
                        
                    else:
                        
                        errors.append(message)
                        warnings.warn(message)
                        return string_value, 'string', errors
                
        #T
        if len(x)>0:
            
            if not x[0]=='T':
                
                message=f'duration conversion error 6 - string value ="{string_value}"' + ' -- ' + x
                
                if validate:
                    
                    raise CSVWError(message)
                    
                else:
                    
                    errors.append(message)
                    warnings.warn(message)
                    return string_value, 'string', errors
                
        x=x.removeprefix('T')
        
        
        #H
        
        if 'H' in x:
        
            hour_string,_,x=x.partition('H')
            
            if hour_string:
                
                try:
                    
                    int(hour_string)
                    
                except ValueError:
                    
                    message=f'duration conversion error 7 - string value ="{string_value}"'
                    
                    if validate:
                        
                        raise CSVWError(message)
                        
                    else:
                        
                        errors.append(message)
                        warnings.warn(message)
                        return string_value, 'string', errors
        

        #M
        if 'M' in x:
        
            minute_string,_,x=x.partition('M')
            
            if minute_string:
                
                try:
                    
                    int(minute_string)
                    
                except ValueError:
                    
                    message=f'duration conversion error 8 - string value ="{string_value}"'
                    
                    if validate:
                        
                        raise CSVWError(message)
                        
                    else:
                        
                        errors.append(message)
                        warnings.warn(message)
                        return string_value, 'string', errors
        

        #S
        
        if 'S' in x:
            
            second_string,_,x=x.partition('S')
            
            if second_string:
                
                try:
                    
                    int(second_string)
                    
                except ValueError:
                    
                    message=f'duration conversion error 9 - string value ="{string_value}"'
                    
                    if validate:
                        
                        raise CSVWError(message)
                        
                    else:
                        
                        errors.append(message)
                        warnings.warn(message)
                        return string_value, 'string', errors
            
        
        #
        return string_value, datatype['base'], errors


    return parse_duration



    
#%% 6.4.6 Formats for other types

def get_parse_other_types_function(
        datatype
        ):
    """
    """
    # If the datatype base is not numeric, boolean, a date/time type, 
    # or a duration type, the datatype format annotation provides a 
    # regular expression for the string values, with syntax and 
    # processing defined by [ECMASCRIPT]. 
    # If the supplied value is not a valid regular expression, 
    # implementations must issue a warning and proceed as if no 
    # format had been provided.

    # NOTE
    # Authors are encouraged to be conservative in the regular 
    # expressions that they use, sticking to the basic features of 
    # regular expressions that are likely to be supported across 
    # implementations.
    
    # Values that are labelled as html, xml, or json should not be 
    # validated against those formats.
    
    # NOTE
    # Metadata creators who wish to check the syntax of HTML, XML, 
    # or JSON within tabular data should use the datatype format 
    # annotation to specify a regular expression against which such 
    # values will be tested.
    
    if not datatype['base'] in ['html','xml','json'] and \
        'format' in datatype:
            
        try:
            
            re.compile(datatype['format'])
            
            datatype2=dict(**datatype)
            
        except re.error:
            
            message='Property "format" with value "{datatype["format"]}" '
            message+='is not a valid regular expression. '
            message+='Property is removed. '
            
            warnings.warn(message)
            
            datatype2={k:v for k,v in datatype.items()
                       if not k=='format'}
            
    else:
        
        datatype2=dict(**datatype)
    
    
    def parse_other_types(
            string_value,
            errors,
            validate
            ):
        """
        """
        
        json_value=string_value
        
        value_type=datatype2['base']
        
        if not datatype2['base'] in ['html','xml','json'] and \
            'format' in datatype2:
            
            regexp = re.compile(datatype2['format'])
            
            if not regexp.fullmatch(string_value):
                
                message=f'string_value "{string_value}" does not match '
                message+=f'regular expression "{datatype["format"]}".'
                
                if validate:
                    
                    raise CSVWError(message)
                    
                else:
                
                    warnings.warn(message)
                
                    errors.append(message)
        
    
        return json_value,value_type,errors


    return parse_other_types
    
    
#%% 6.6 Validating Tables

    # Validators test whether given tabular data files adhere to the 
    # structure defined within a schema. 
    # Validators must raise errors (and halt processing) and issue warnings 
    # (and continue processing) as defined in [tabular-metadata]. 
    # In addition, validators must raise errors but may continue 
    # validating in the following situations:

    # - if the table description is not compatible with the embedded 
    #   metadata extracted from the tabular data file, as defined in 
    #   Table Compatibility in [tabular-metadata].
    
    #... ALREADY DONE in compare_table_descriptions
    
    # - if there is more than one row with the same primary key, that 
    #   is where the cells listed for the primary key for the row have 
    #   the same values as the cells listed for the primary key for another row,
    
    #... ALREADY DONE in annotate_schema_dict
    
    # - for each row that does not have a unique referenced row for each 
    #   of the foreign keys on the table in which the row appears, or
    
    #... ALREADY DONE in create_annotated_table_group
        
    # - for each error on each cell. 
    
    #... ALREADY DONE
    

def validate_annotated_table_dict(
        annotated_table_dict,
        embedded_metadata_dict
        ):
    """
    """

    


#%% 8 - Parsing Tabular Data

def parse_tabular_data_from_text(
        tabular_data_file_url,
        dialect_description_dict,
        _print_intermediate_outputs=False
        ):
    """
    """
    
    comment_prefix=dialect_description_dict.get('commentPrefix',None)
    delimiter=dialect_description_dict.get('delimiter',',')
    encoding=dialect_description_dict.get('encoding','utf-8')
    escape_character=dialect_description_dict.get('escapeCharacter','"')
    header=dialect_description_dict.get('headerRowCount',True)
    header_row_count=dialect_description_dict.get('headerRowCount',1 if header else 0)
    line_terminators=dialect_description_dict.get('lineTerminators',['\r\n', '\n'])
    quote_character=dialect_description_dict.get('quoteCharacter','"')
    skip_blank_rows=dialect_description_dict.get('skipBlankRows',False)
    skip_columns=dialect_description_dict.get('skipColumns',0)
    skipInitialSpace=dialect_description_dict.get('skipInitialSpace',False)
    skip_rows=dialect_description_dict.get('skipRows',0)
    trim=dialect_description_dict.get('trim',True)  # one of True, False, "true", "false", "start", "end"
    
    #...skipInitialSpace is ignored as there is a contradiction here
    #...if the trim property has a default of True, then this always overrides
    #...the skipInitialSpace property
    
    # skipInitialSpace
    # A boolean atomic property that, if true, sets the trim flag to "start" 
    # and if false, to false. If the trim property is provided, the 
    # skipInitialSpace property is ignored. The default is false.
    # if trim is None:
        
    #     if skipInitialSpace:
            
    #         trim='start'
            
    #     else:
            
    #         trim=False
            
    # print('trim',trim)
    
    
                
    #print('dialect_description_obj_dict',dialect_description_obj_dict)
    #print('header_row_count',header_row_count)
            
    # The algorithm for using these flags to parse a document containing 
    # tabular data to create a basic annotated tabular data model and to 
    # extract embedded metadata is as follows:
    
    # 1. Create a new table T with the annotations:
    #    - columns set to an empty list
    #    - rows set to an empty list
    #    - id set to null
    #    - url set to the location of the file, if known, or null
    #    - table direction set to auto
    #    - suppress output set to false
    #    - notes set to false
    #    - foreign keys set to an empty list
    #    - transformations set to an empty list    
    
    table_dict=dict(
        columns=[],
        rows=[],
        id=None,
        url=tabular_data_file_url,
        tableDirection='auto',
        suppressOutput=False,
        notes=[],  # not False as stated in the standard
        foreignKeys=[],
        transformations=[]       
        )
    
    
    # 2. Create a metadata document structure M that looks like:
    #    {
    #      "@context": "http://www.w3.org/ns/csvw",
    #      "rdfs:comment": []
    #      "tableSchema": {
    #        "columns": []
    #      }
    #    }

    metadata_table_dict={
        "@context": "http://www.w3.org/ns/csvw",
        "rdfs:comment": [],
        "tableSchema": {
            "columns": []
            }
      }
    

    # 3. If the URL of the tabular data file being parsed is known, set the 
    #    url property on M to that URL.
    
    if not tabular_data_file_url is None:
        metadata_table_dict['url']=tabular_data_file_url


    # 4. Set source row number to 1.
    source_row_number=1
    
    
    # 5. Read the file using the encoding, as specified in [encoding], using 
    #    the replacement error mode. If the encoding is not a Unicode encoding, 
    #    use a normalizing transcoder to normalize into Unicode Normal Form C 
    #    as defined in [UAX15].
    
    #...remove fragments and queries from tabular_data_file_url
    url=urllib.parse.urljoin(tabular_data_file_url, 
                         urllib.parse.urlparse(tabular_data_file_url).path)
    
    with urllib.request.urlopen(url) as response:
    
        tabular_data_file_text=response.read().decode(encoding)
    
    
    if _print_intermediate_outputs: print('-tabular_data_file_text',tabular_data_file_text)
    
            
    #...
    character_index=0  # index for processing each character in the file
    
    
    # 6. Repeat the following the number of times indicated by skip rows:
    
    for _ in range(skip_rows): 
        
        #...for testing
        #print('-source_row_number',source_row_number)
        
        #print(_)
        
        # 6.1 Read a row to provide the row content.
        character_index, row_content=\
            get_row_content(
                tabular_data_file_text,
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
        
        #...for testing
        #print('-source_row_number',source_row_number)
        
        # 7.1 Read a row to provide the row content.
        character_index, row_content=\
            get_row_content(
                tabular_data_file_text,
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
                    [{'titles':[]} 
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
    
    if header_row_count==0 or len(metadata_table_dict['tableSchema']['columns'])==0:
        
        original_character_index=character_index
        
        while True: # loops until a non-comment row is found
            character_index, row_content=\
                get_row_content(
                    tabular_data_file_text,
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
        
        #...for testing
        #if row_number%1000==0:
        #print('-row_number',row_number)
        
        
        if character_index>len(tabular_data_file_text)-1:
            break
        
        # 10.1 Set the source column number to 1.
        source_column_number=1
        
        # 10.2 Read a row to provide the row content.
        character_index, row_content=\
            get_row_content(
                tabular_data_file_text,
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
                        datatype={'base':'string'}, 
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
                    table=table_dict, 
                    column=column_dict, 
                    row=row_dict, 
                    stringValue=value,
                    value=None,   #...this is done later after the annotations from the metadata have been applied
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
        
    # validate_and_normalize_metadata_table_dict(
    #     metadata_table_dict,
    #     metadata_document_location=None,
    #     metadata_table_group_dict=None
    #     )
    
    
    
    return table_dict, metadata_table_dict
    
    

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
        
        #print(i, end=' ')
        
        #print('-line_terminators',line_terminators)
        
        #...placing this first...
        # 3 If there are no more characters to read, return the row content.
        if i>len(tabular_data_text)-1:
            break
        
    
        # 2.1 If the string starts with the escape character followed by the 
        # quote character, append both strings to the row content, and move on 
        # to process the string following the quote character.
        if tabular_data_text[i]==escape_character \
            and tabular_data_text[i+1]==quote_character:
                
            row_content+=escape_character+quote_character
            
            i+=2
            
            continue
            
        
        
        # 2.2 Otherwise, if the string starts with the escape character and the 
        # escape character is not the same as the quote character, append the 
        # escape character and the single character following it to the row 
        # content and move on to process the string following that character.
        if tabular_data_text[i]==escape_character \
            and escape_character!=quote_character:
                
            row_content+=tabular_data_text[i:i+2]
            
            i+=2
            
            continue
        
        # 2.3 Otherwise, if the string starts with the quote character, append 
        # the quoted value obtained by reading a quoted value to the row content 
        # and move on to process the string following the quoted value.
        if tabular_data_text[i]==quote_character:
            
            i, quoted_value=get_quoted_value(
                tabular_data_text,
                i,
                escape_character,
                quote_character
                )
            
            row_content+=quoted_value
            
            # TO BE DELETED
            # j, quoted_value=get_quoted_value(
            #     tabular_data_text[i:],
            #     escape_character,
            #     quote_character
            #     )
            
            # row_content+=quoted_value
            
            # i+=j
            
            continue
            
        # 2.4 Otherwise, if the string starts with one of the line terminators, 
        # return the row content.
        
        line_terminator_flag=False
        
        for x in line_terminators:
            
            #print('-x',x)
            #print(tabular_data_text[i:i+len(x)-1])
        
            if tabular_data_text[i:i+len(x)]==x:
                
                #print('TEST')
                
                i+=len(x)
                
                line_terminator_flag=True
                
                break
            
        if line_terminator_flag:
            
            break
            
        # 2.5 Otherwise, append the first character to the row content and move 
        # on to process the string following that character.
        row_content+=tabular_data_text[i]
        
        i+=1
    
        
    #print(i, row_content)
    
    return i, row_content
    
    
def get_quoted_value(
        tabular_data_text,
        i,
        escape_character,
        quote_character,
        ):
    """
    """
    
    # To read a quoted value to provide a quoted value, perform the following steps:
        
    # 1 Set the quoted value to an empty string.
    quoted_value=''
    
    # 2 Read the initial quote character and add a quote character to the quoted value.
    initial_quote_character=tabular_data_text[i]
    quoted_value+=initial_quote_character
    
    # 3 Read initial characters and process as follows:
    
    i+=1
    
    while True:    
        
        current_character=tabular_data_text[i]
        
        try:
            
            next_character=tabular_data_text[i+1]
            
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
            
            
    return i, quoted_value
    
    
# TO BE DELETED
# def get_quoted_value(
#         characters,
#         escape_character,
#         quote_character,
#         ):
#     """
#     """
    
    
#     logging=False
    
#     if logging: logging.info('    FUNCTION: get_quoted_value')
#     if logging: logging.debug(f'        ARGUMENT: characters: {characters}')
#     if logging: logging.debug(f'        ARGUMENT: escape_character: {escape_character}')
#     if logging: logging.debug(f'        ARGUMENT: quote_character: {quote_character}')
    
#     # To read a quoted value to provide a quoted value, perform the following steps:
        
#     # 1 Set the quoted value to an empty string.
#     quoted_value=''
    
#     # 2 Read the initial quote character and add a quote character to the quoted value.
#     initial_quote_character=characters[0]
#     quoted_value+=initial_quote_character
    
#     # 3 Read initial characters and process as follows:
#     i=1
    
#     while True:    
        
#         current_character=characters[i]
        
#         try:
            
#             next_character=characters[i+1]
            
#         except IndexError:
            
#             next_character=None
    
#         # 3.1 If the string starts with the escape character followed by the quote 
#         # character, append both strings to the quoted value, and move on to 
#         # process the string following the quote character.
#         if (current_character==escape_character 
#             and next_character==quote_character):
            
#             quoted_value+=escape_character+quote_character
            
#             i+=2
        
#         # 3.2 Otherwise, if string starts with the escape character and the escape 
#         # character is not the same as the quote character, append the escape 
#         # character and the character following it to the quoted value and move 
#         # on to process the string following that character.
#         elif (current_character==escape_character 
#               and escape_character!=quote_character):
            
#             quoted_value+=escape_character+next_character
            
#             i+=2
        
#         # 3.3 Otherwise, if the string starts with the quote character, return 
#         # the quoted value.
#         elif current_character==quote_character:
#             quoted_value+=quote_character
            
#             i+=1
            
#             break
        
#         # 3.4 Otherwise, append the first character to the quoted value and move 
#         # on to process the string following that character.
#         else:
            
#             quoted_value+=current_character
            
#             i+=1
            
#     if logging: logging.debug(f'        RETURN VALUE: i, quoted_value: {i}, {quoted_value}')
            
#     return i, quoted_value

    
def get_list_of_cell_values(
        characters, ## the row_content
        escape_character,
        quote_character,
        delimiter,
        trim
        ):
    """
    """
    #print('-characters',characters.encode())
    #print('-quote_character',quote_character)
    
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
    
        #print('-current_character',current_character)
        #print('-next_character',next_character)#,type(next_character))
    
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
                        raise Exception('No delimiter after quote character')
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

#%% 5.1 Property Syntax

#%% 5.1.1 Array Properties

def validate_array_property(
        metadata_obj_dict,
        property_name,
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
    
    property_value=metadata_obj_dict[property_name]
        
    if isinstance(property_value,list):
        
        if not expected_types is None:
        
            # remove invalid objects
            for item in property_value[::-1]:
                
                if not type(item) in expected_types:
                    
                    message=f'Item "{item}" ({type(item).__name__}) '
                    message+='in array property "{property_name}" is not valid. '
                    message+=f'Item should bo one of the following types: {expected_types}. '
                    message+='Item is removed from array.'
                    
                    warnings.warn(message)
                    
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
    
    
#%% 5.1.2 Link Properties
    
def validate_link_property(
        metadata_obj_dict,
        property_name,
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
    
    property_value=metadata_obj_dict[property_name]
    
    if not isinstance(property_value,str):
        
        property_value_type=type(property_value).__name__
        
        message=f'Property "{property_name}" with value "{property_value} "' 
        message+=f'({property_value_type}) is not valid.'
        message+=' String expected.'
        message+=' Value replaced with an empty string.'
        
        warnings.warn(message)
        
        property_value=''
        
        metadata_obj_dict[property_name]=property_value
  
        
#%% 5.1.3 - URI Template Properties

def validate_uri_template_property(
        metadata_obj_dict,
        property_name,
        ):
    """
    """
    # If the supplied value of a URI template property is not a string 
    # (e.g. if it is an integer), compliant applications must issue a 
    # warning and proceed as if the property had been supplied with an 
    # empty string.
    
    property_value=metadata_obj_dict[property_name]
    
    if not isinstance(property_value,str):
        
        message=f'Property "{property_name}" '
        message+=f'with value "{property_value}" <{type(property_value).__name__}> '
        message+='is invalid. String type expected. '
        message+='Value is replaced with an empty string.'
        
        warnings.warn(message)
        
        property_value=''
        
        metadata_obj_dict[property_name]=property_value
        
        
    


def get_URI_from_URI_template(
        uri_template_string,
        annotated_cell_dict,
        table_url,
        ):
    """
    """
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
    
    #print(uritemplate.variables(uri_template_string))
    
    for variable in uritemplate.variables(uri_template_string):  # loops through variables being asked for in the URI template
        
        if not variable.startswith('_'):
            
            for cell in annotated_cell_dict['row']['cells']:
                
                name=cell['column']['name']
                
                if name==variable:
                    
                    #...set value if not passed into the function
                    value=cell['value']
                    
                    if value is None:
                        
                        return None  # if a variable has value of None, then the returned URI is None
                    
                    elif isinstance(value,list):
                        
                        value=[x['@value'] for x in value]
                        
                    else:
                        
                        value=[value['@value']]
                        
                    variables[name]=value
                    
                    break
                        
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
    uri=uri.replace('%2F','/')  # reverses changes to forward slashes in the expand process
    
    
    # 2 expanding any prefixes as if the value were the name of a common 
    # property, as described in section 5.8 Common Properties.
    if ':' in uri:
        x=uri.split(':')
    else:
        x=uri.split('%3A')  # this is the percent encoded version
        
    if len(x)==2:
        
        if x[0] in prefixes:
            
            uri=prefixes[x[0]]+x[1]
    
    
    #print('-uri',uri)
    #print('-table_url',table_url)
    
    # 3 resolving the resulting URL against the base URL of the table url if not null.
    
    if not table_url is None:
                
        url=\
            urllib.parse.urljoin(
                table_url,
                uri
                )  
    
    else:
        
        url=uri
        
        
    #... percent encoding any spaces
    url=urllib.parse.quote(
        url.encode('utf8'),
        safe='/:#%=%'
        )
        
    #print('-url',url)
        
    return url
    
    
  
    
#%% 5.1.4 Column Reference Properties

def validate_column_reference_property(
        metadata_obj_dict,
        property_name,
        ):
    """
    """
    # Column reference properties hold one or more references to other 
    # column description objects. 
    # The referenced description object must have a name property. 
    # Column reference properties can then reference column description 
    # objects through values that are:
    # - strings — which must match the name on a column description object 
    #   within the metadata document.
    # - arrays — lists of strings as above.
    
    # Compliant applications must issue a warning and proceed as if the 
    # column reference property had not been specified if:
    # - the supplied value is not a string or array (e.g. if it is an integer).
    # - the supplied value is an empty array.
    # - any of the values in the supplied array are not strings.
    # - any of the supplied strings do not reference one or more columns.

    # For example, the primaryKey property is a column reference property 
    # on the schema. 
    # It has to hold references to columns defined elsewhere in the schema, 
    # and the descriptions of those columns must have name properties. 
    # It can hold a single reference, like this:

    # EXAMPLE 15
    # "tableSchema": {
    #   "columns": [{
    #     "name": "GID"
    #   }, ... ],
    #   "primaryKey": "GID"
    # }
    
    # or it can contain an array of references, like this:
    
    # EXAMPLE 16
    # "tableSchema": {
    #   "columns": [{
    #     "name": "givenName"
    #   }, {
    #     "name": "familyName"
    #   }, ... ],
    #   "primaryKey": [ "givenName", "familyName" ]
    # }
    
    # If the primaryKey property were given an invalid value, such as 1, or 
    # a column name were misspelled, the processor must issue a warning and 
    # ignore the value.
    
    # On the other hand, the columnReference property is a required 
    # property; if it has an invalid value, such as an empty array, 
    # then the processor will issue an error as if the property 
    # were not specified at all.
    
    property_value=metadata_obj_dict[property_name]
        
    if not isinstance(property_value,str) and not isinstance(property_value,list):
        
        message=f'Property "{property_name}" '
        message+=f'with value "{property_value}" <{type(property_value).__name__}> '
        message+='is invalid. String or list type expected. '
        message+='Property is removed.'
        
        warnings.warn(message)
        
        metadata_obj_dict.remove(property_name)
        
        return
    
    
    if property_value==[]:
        
        message=f'Property "{property_name}" '
        message+=f'with value "{property_value}" <{type(property_value).__name__}> '
        message+='is invalid. Value must not be an emply array. '
        message+='Property is removed.'
        
        warnings.warn(message)
        
        metadata_obj_dict.remove(property_name)
        
        return
    
    
    if isinstance(property_value,list) and \
        not all([isinstance(x,str) for x in property_value]):
            
        message=f'Property "{property_name}" '
        message+=f'with value "{property_value}" <{type(property_value).__name__}> '
        message+='is invalid. All list items must be string. '
        message+='Property is removed.'
        
        warnings.warn(message)
        
        metadata_obj_dict.remove(property_name)
        
        return
        
    
    


def get_columns_from_column_reference(
        column_reference,
        annotated_table_dict,
        ):
    """
    """
    
    # Column reference properties hold one or more references to other 
    # column description objects. 
    # The referenced description object must have a name property.
    # Column reference properties can then reference column description 
    # objects through values that are:
    # - strings — which must match the name on a column description object within the metadata document.
    # - arrays — lists of strings as above.
    
    if not isinstance(column_reference,list):
        
        column_reference=[column_reference]
        
    columns=[]
    
    for column_reference_name in column_reference:
    
        for annotated_column_dict in annotated_table_dict['columns']:
            
            if column_reference_name==annotated_column_dict['name']:
                
                columns.append(annotated_column_dict)

    return columns
       
    
    
#%% 5.1.5 Object Properties

def validate_object_property(
        metadata_obj_dict,
        property_name,
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
    
    property_value=metadata_obj_dict[property_name]
    
    if isinstance(property_value,str) or isinstance(property_value,dict):
        
        pass
    
    else:
        
        message=f'Property "{property_name}" ' 
        message+=f'with value "{property_value}" ({type(property_value).__name__}) is not valid. '
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
  
    
#%% 5.1.6 Natural Language Properties

def validate_natural_language_property(
        metadata_obj_dict,
        property_name,
        ):
    """
    """

    # Natural language properties hold natural language strings. 
    # Their values may be:
    
    # - strings — interpreted as natural language strings in the 
    #   default language.
    # - arrays — interpreted as alternative natural language strings 
    #   in the default language.
    # - objects whose properties must be language codes as defined by [BCP47] 
    #   and whose values are either strings or arrays, providing natural 
    #   language strings in that language.
    
    # Natural language properties are used for titles. 
    # For example, the titles property on a column description provides a 
    # natural language label for a column. 
    # If it's a plain string like this:
    
    # EXAMPLE 21
    # "titles": "Project title"
    
    # then that string is assumed to be in the default language (or have 
    # an undefined language, und, if there is no such property). 
    # Multiple alternative values can be given in an array:
    
    # EXAMPLE 22
    # "titles": [
    #   "Project title",
    #   "Project"
    # ]
    
    # It's also possible to provide multiple values in different languages, 
    # using an object structure. For example:
    
    # EXAMPLE 23
    # "titles": {
    #   "en": "Project title",
    #   "fr": "Titre du projet"
    # }
    # and within such an object, the values of the properties can themselves be arrays:
    
    # EXAMPLE 24
    # "titles": {
    #   "en": [ "Project title", "Project" ],
    #   "fr": "Titre du projet"
    # }
    
    # The annotation value of a natural language property is an object 
    # whose properties are language codes and where the values of those 
    # properties are an array of strings (see Language Maps in [JSON-LD]).
    
    # NOTE
    # When extracting a annotation value from a metadata that will have 
    # already been normalized, a natural language property will already have this form.
    
    # If the supplied value of a natural language property is not a string, 
    # array or object (e.g. if it is an integer), compliant applications 
    # must issue a warning and proceed as if the property had been specified 
    # as an empty array. 
    # If the supplied value is an array, any items in that array that are 
    # not strings must be ignored. 
    # If the supplied value is an object, any properties that are not 
    # valid language codes as defined by [BCP47] must be ignored, as 
    # must any properties whose value is not a string or an array, and 
    # any items that are not strings within array values of these properties.
    
    property_value=metadata_obj_dict[property_name]
        
    if isinstance(property_value,str):
        
        pass
    
    elif isinstance(property_value,list):
        
        x=[]
        
        for item in property_value:
            
            if isinstance(item,str):
                
                x.append(item)
                
            else:
                
                message='Property "{property_name}" '
                message+='with value "{property_value}" ({type(property_value).__name__}) '
                message+='has non-string value in array. '
                message+='Non-string value is removed'
                
                warnings.warn(message)
                
        property_value=x
    
    elif isinstance(property_value,dict):
        
        d={}
        
        for k,v in property_value.items():
            
            if langcodes.tag_is_valid(k):
                
                if isinstance(v,str):
                    
                    d[k]=v
                    
                elif isinstance(v,list):
                    
                    x=[]
                    
                    for item in v:
                        
                        if isinstance(item,str):
                            
                            x.append(item)
                            
                        else:
                            
                            message='Property "{property_name}" '
                            message+='with value "{property_value}" ({type(property_value).__name__}) '
                            message+='has non-string value in array. '
                            message+='Non-string value is removed'
                            
                            warnings.warn(message)
                            
                    d[k]=x
                    
            else:
                
                message='Property "{property_name}" '
                message+='with value "{property_value}" ({type(property_value).__name__}) '
                message+='has invalid language code. '
                message+='Property is removed'
                
                warnings.warn(message)
                
        property_value=d
    
    else:
        
        message='Property "{property_name}" '
        message+='with value "{property_value}" ({type(property_value).__name__}) '
        message+='is of invalid type. '
        message+='Natural language property values must be either a string, a list or an object. '
        message+='Property value is replaced with an empty array'
        
        warnings.warn(message)
        
        property_value=[]
        
    metadata_obj_dict[property_name]=property_value
        
    
    
    
#%% 5.1.7 Atomic Properties

def validate_atomic_property(
        metadata_obj_dict,
        property_name,
        expected_types=None,
        expected_values=None,
        default_value=None,
        required_values=None,
        greater_than_or_equal_to=None,
        use_default_value=True
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
    
    property_value=metadata_obj_dict[property_name]

    if not expected_types is None:

        if not type(property_value) in expected_types:
            
            message=f'Property "{property_name}" with value '
            message+=f'"{property_value}" ({type(property_value).__name__}) is not valid.'
            message+=f' One of these types expected: "{expected_types}". '
            
            if use_default_value:
            
                message+=f' Value replaced with "{default_value}". '
                
                metadata_obj_dict[property_name]=default_value
                
                warnings.warn(message)
                
            else:
                
                message+='Property removed.'
                
                metadata_obj_dict.pop(property_name)

                warnings.warn(message)
                
                return
            
            
    #
    property_value=metadata_obj_dict[property_name]
        
    if not expected_values is None:
        
        if not property_value in expected_values:
            
            message=f'Property "{property_name}" with value '
            message+=f'"{property_value}" ({type(property_value).__name__}) is not valid.'
            message+=f' One of these values expected: "{expected_values}". '
            
            if use_default_value:
            
                message+=f' Value replaced with "{default_value}". '
                
                metadata_obj_dict[property_name]=default_value
                
                warnings.warn(message)
                
            else:
                
                message+='Property removed.'
                
                metadata_obj_dict.pop(property_name)
    
                warnings.warn(message)
                
                return
        
            
    #
    property_value=metadata_obj_dict[property_name]
    
    if not greater_than_or_equal_to is None:
        
        if not property_value>=greater_than_or_equal_to:
            
            message=f'Property "{property_name}" with value '
            message+=f'"{property_value}" ({type(property_value).__name__}) is not valid.'
            message+=f' Value should be greater than or equal to {greater_than_or_equal_to}. '
            
            if use_default_value:
            
                message+=f' Value replaced with "{default_value}". '
                
                metadata_obj_dict[property_name]=default_value
                
                warnings.warn(message)
                
            else:
                
                message+='Property removed.'
                
                metadata_obj_dict.pop(property_name)

                warnings.warn(message)
                
                return
            
    
    #
    property_value=metadata_obj_dict[property_name]
    
    if not required_values is None:
        
        if not property_value in required_values:
            
            message=f'Property "{property_name}" with value '
            message+=f'"{property_value}" ({type(property_value).__name__}) is not valid.'
            message+=f' One of these values is required: "{expected_values}". '
            message+=f' Value replaced with "{default_value}". '
                
            raise CSVWError(message)
            
    
    
        
        
    

#%% 5.2 Top-Level Properties

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
        raise CSVWError(message)
    
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
    elif isinstance(context,list):
        
        if not len(context)==2:
            
            message='If an array, property "@context" must of two items only.'
            raise CSVWError(message)
            
        elif not context[0]=='http://www.w3.org/ns/csvw':
            
            message='If an array, the first item of property "@context" must be '
            message+='the string "http://www.w3.org/ns/csvw".'
            raise CSVWError(message)
            
        elif not isinstance(context[1],dict):
            
            message='If an array, the second item of property "@context" must be '
            message+='an object.'
            raise CSVWError(message)
            
        elif not len(context[1])>0:
            
            message='If an array, the second item of property "@context" must be '
            message+='an object with either or both of "@base" and "@language".'
            raise CSVWError(message)
            
            
        else:
        
          
          
          
          # and len(context)==2
          # and context[0]=='http://www.w3.org/ns/csvw'
          # and isinstance(context[1],dict)
          # and len(context[1])>0
          # ):
        
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
                
                if not isinstance(context_base_string,str):
                    
                    message=f'"@base" property must be a string (not {type(context_base_string)}).'
                    raise CSVWError(message)
                
                
                
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
            
                    message=f'Property "@language" with value "{default_language}" ' 
                    message+='is not a valid language code.'        
            
                    warnings.warn(message)
                    
                    default_language='und'
            
            else:
                
                default_language='und'
            
            
            # check only @base and @language are present
            for k in context[1]:
                
                if not k in ['@base','@language']:
                    
                    message='If an array, the second item of property "@context" '
                    message+='can only contain properties "@base" and "@language". '
                    message+=f'Property "{k}" is not valid.'
                    
                    raise CSVWError(message)
        
    else:
            
        message='"@context" property must be either the string "http://www.w3.org/ns/csvw" '
        message+='or an array.'
        
        raise CSVWError(message)
            
        
    
    return base_url, default_language
    

#%% 5.3 Table Groups


def validate_table_group_metadata(
        metadata_document_location,
        validate=True
        ):
    """High level function to validate a table group metadata file.
    """
    with open(metadata_document_location) as f:
        metadata_table_dict=json.load(f)
        
    validate_and_normalize_metadata_table_group_dict(
            metadata_table_dict,
            metadata_document_location,
            validate
            )
    
    return metadata_table_dict
    


def validate_and_normalize_metadata_table_group_dict(
        metadata_table_group_dict,
        metadata_document_location,
        validate
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
    
    # loop through items
    for k in list(metadata_table_group_dict):
        
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
                [dict]
                )
            
            # Compliant application must raise an error if this array does not contain one or more table descriptions.
            
            tables=metadata_table_group_dict[k]
            
            if len(tables)<1:
                
                message='Property "tables" must contain one or more table descriptions.'
                
                raise CSVWError(message)
                
            # loop through tables
            for metadata_table_dict in tables:
                
                validate_and_normalize_metadata_table_dict(
                        metadata_table_dict,
                        metadata_document_location,
                        metadata_table_group_dict,
                        base_url,
                        default_language,
                        has_top_level_property=False,
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
                )
            
            referenced_url=\
                normalize_object_property(
                    metadata_table_group_dict,
                    k,
                    base_url
                    )
            
            validate_and_normalize_metadata_dialect_dict(
                    metadata_table_group_dict[k],
                    referenced_url or metadata_document_location,
                    base_url,
                    default_language,
                    has_top_level_property=True if referenced_url else False,
                    is_referenced=True if referenced_url else False,
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
                None
                )
            
            normalize_common_property_or_notes(
                metadata_table_group_dict,
                k
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
                expected_types=[str],
                expected_values=['rtl','ltr','auto'],
                default_value='auto',
                )
            
            normalize_atomic_property(
                metadata_table_group_dict,
                k
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
                k
                )
            
            referenced_url=\
                normalize_object_property(
                    metadata_table_group_dict,
                    k,
                    base_url
                    )
            
            validate_and_normalize_metadata_schema_dict(
                metadata_table_group_dict[k],
                referenced_url or metadata_document_location,
                base_url,
                default_language,
                has_top_level_property=True if referenced_url else False,
                is_referenced=True if referenced_url else False,
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
                [dict]
                )
            
            transformations=metadata_table_group_dict[k]
            
            # loop through transormations
            for i in range(len(transformations)):
                
                validate_object_property(
                    transformations,
                    i
                    )
                
                referenced_url=\
                    normalize_object_property(
                        transformations,
                        i,
                        base_url
                        )
                
                metadata_transformation_dict=transformations[i]
                
                validate_and_normalize_metadata_transformation_dict(
                        metadata_transformation_dict,
                        base_url,
                        default_language,
                        has_top_level_property=True if referenced_url else False,
                        is_referenced=True if referenced_url else False,
                        metadata_document_location=referenced_url or metadata_document_location
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
                k
                )
            
            id_=metadata_table_group_dict[k]
              
            if id_.startswith('_:'):
                
                message='Property "@id" must not start with "_:". '
                
                raise CSVWError(message)
            
            
            normalize_link_property(
                metadata_table_group_dict,
                k,
                base_url
                )
            
                
            
        # @type
        elif k=='@type':
            
            # If included, @type is an atomic property that must be set 
            # to "TableGroup". 
            # Publishers may include this to provide additional information 
            # to JSON-LD based toolchains.
            
            validate_atomic_property(
                metadata_table_group_dict,
                k,
                required_values=['TableGroup']
                )
            
            normalize_atomic_property(
                metadata_table_group_dict,
                k,
                )
            
        # inherited properties
        elif k in ['aboutUrl','datatype','default','lang','null','ordered',
                   'propertyUrl','required','separator','textDirection',
                   'valueUrl']:
            
            validate_and_normalize_inherited_property(
                metadata_table_group_dict,
                k,
                base_url,
                default_language
                )
            
        # common properties
        else:
            
            validate_and_normalize_common_property(
                metadata_table_group_dict,
                k,
                base_url,
                default_language
                )
    
    # required properties
    if not 'tables' in metadata_table_group_dict:
        
        message='Property "tables" is a required property.'
        
        raise CSVWError(message)
    
    
    
    
    
    
    
    
            
    #...from 5.1.4
    # Compliant applications must issue a warning and proceed as if the 
    # column reference property had not been specified if:
    # - any of the supplied strings do not reference one or more columns.
    
    #...from 5.5
    # resource
    # The table group must contain a table whose url annotation is identical 
    # to the expanded value of this property. 
    
    # TO DO
    
    #...from 5.5
    # schemaReference
    # The table group must contain a table with a tableSchema having a @id
    # that is identical to the expanded value of this property, and there must not be more than one such table. 
    
    # TO DO
    
    def test_column_reference_property(
            column_reference,
            metadata_table_dict,
            raise_error=False
            ):
        """
        """
        if 'tableSchema' in metadata_table_dict:
            
            metadata_schema_dict=metadata_table_dict['tableSchema']    
        
            if not isinstance(column_reference,list):
                
                column_references=[column_reference]
                
            else:
                
                column_references=column_reference
            
        for name in column_references:
            
            #print('-name',name)
            
            for metadata_column_dict in metadata_schema_dict.get('columns',[]):
                
                #print("-metadata_column_dict.get('name')",metadata_column_dict.get('name'))
                
                if name==metadata_column_dict.get('name'):
                    
                    return
            
        message=f'Column reference property with value "{column_reference}" '
        message+='does not match any column names. '

        if raise_error:
            
            raise CSVWError(message)    
            
        else:

            warnings.warn(message)
        
    
    
    for metadata_table_dict in metadata_table_group_dict.get('tables',[]):
        
        if 'tableSchema' in metadata_table_dict:
            
            metadata_schema_dict=metadata_table_dict['tableSchema']
            
            for foreign_key_dict in metadata_schema_dict.get('foreignKeys',[]):
                
                # foreign key column reference
                column_reference=foreign_key_dict['columnReference']
                
                test_column_reference_property(
                        column_reference,
                        metadata_table_dict,
                        raise_error=True
                        )
                    
                # reference
                reference_dict=foreign_key_dict['reference']
                
                referenced_table_dict=None
                
                # foreign key reference resource
                if 'resource' in reference_dict:
                    
                    resource=reference_dict['resource']
                    
                    for metadata_table_dict2 \
                        in metadata_table_group_dict.get('tables',[]):
                
                        if resource==metadata_table_dict2.get('url'):
                            
                            referenced_table_dict=metadata_table_dict2
                            
                            break
                        
                    if referenced_table_dict is None:
                        
                        message=f'Property "resource" with value "{resource}" '
                        message+='does not match any table url. '

                        raise CSVWError(message)
                        
                # foreign key reference resource
                if 'schemaReference' in reference_dict:
                    
                    schema_reference=reference_dict['schemaReference']
                    
                    for metadata_table_dict2 \
                        in metadata_table_group_dict.get('tables',[]):
                            
                        if 'tableSchema' in metadata_table_dict2:
                            
                            metadata_schema_dict2=metadata_table_dict2['tableSchema']
                            
                            if schema_reference==metadata_schema_dict2.get('@id'):
                            
                                referenced_table_dict=metadata_table_dict2
                            
                                break
                        
                    if referenced_table_dict is None:
                        
                        message=f'Property "schemaReference" with value "{schema_reference}" '
                        message+='does not match any schema @id property. '

                        raise CSVWError(message)
                        
                # foreign key reference column reference
                column_reference=reference_dict['columnReference']
                
                test_column_reference_property(
                        column_reference,
                        referenced_table_dict,
                        raise_error=True
                        )
                        
            # primary key
            if 'primaryKey' in metadata_schema_dict:
                
                column_reference=metadata_schema_dict['primaryKey']
                
                test_column_reference_property(
                        column_reference,
                        metadata_table_dict
                        )
                
            # rowTitles
            if 'rowTitles' in metadata_schema_dict:
                
                column_reference=metadata_schema_dict['rowTitles']
                
                test_column_reference_property(
                        column_reference,
                        metadata_table_dict
                        )
                
    #...Section 5.6
    # If there is no name property defined on this column, 
    # the first titles value having the same language tag as 
    # default language, or und or if no default language is 
    # specified, becomes the name annotation for the described column. 
    # This annotation must be percent-encoded as necessary to 
    # conform to the syntactic requirements defined in [RFC3986].
    
    #lang=metadata_table_group_dict.get('lang',None)
    
    for metadata_table_dict in metadata_table_group_dict.get('tables',[]):
        
        #if 'lang' in metadata_table_dict:
        #    lang=metadata_table_dict['lang']
        
        if 'tableSchema' in metadata_table_dict:
            
            metadata_schema_dict=metadata_table_dict['tableSchema']
            
            #if 'lang' in metadata_schema_dict:
            #    lang=metadata_schema_dict['lang']
            
            for metadata_column_dict in metadata_schema_dict.get('columns',[]):
                
                #if 'lang' in metadata_column_dict:
                #    lang=metadata_column_dict['lang']
    
                if not 'name' in metadata_column_dict:
                    
                    name=get_first_title_for_name_property(
                            metadata_column_dict,
                            default_language
                            )
                    
                    if not name is None:
                        
                        metadata_column_dict['name']=name
                    
                    
                    # if 'titles' in metadata_column_dict:
                        
                    #     for k,v in metadata_column_dict['titles'].items():
                            
                    #         #print(v,k, default_language)
                            
                    #         #if (k=='und' and lang is None) or k.startswith(lang):
                                
                    #         if k.startswith(default_language) \
                    #             or (k=='und' and default_language=='und'):
                                
                    #     #if default_language in metadata_column_dict['titles']:    
                    
                    #         #x=metadata_column_dict['titles'][default_language][0]
                        
                    #             x=v[0]
                        
                    #             name=urllib.parse.quote(
                    #                 x.encode('utf8'),
                    #                 safe=''  # does not include "/" in safe, so that "dd/MM/yyyy" is quoted to "dd%2DMM%2Dyyyy"
                    #                 )
                    #             name=name.replace('-','%2D')  # required by rdf test 188
                            
                    #             metadata_column_dict['name']=name
                                
                    #             break

                    #     if not 'name' in metadata_column_dict:
                            
                    #         message='No title exists with default language tag. '
                    #         message+=' Column name property is not set.'
                            
                    #         warnings.warn(message)
                            
       
    
    #print('-metadata_table_group_dict',metadata_table_group_dict)
    
    return base_url, default_language


def annotate_table_group_dict(
        annotated_table_group_dict,
        metadata_table_group_dict,
        base_url,
        default_language,
        validate
        ):
    """
    """
    
    table_group_inherited_properties_cache={}
    
    for k,v in metadata_table_group_dict.items():
        
        # notes
        if k=='notes':
            
            if annotated_table_group_dict['notes']:
            
                annotated_table_group_dict['notes'].extend(v)
                
            else:
                
                annotated_table_group_dict['notes']=v
            
        # tableDirection
        elif k=='tableDirection':
            
            for annotated_table_dict in annotated_table_group_dict['tables']:
            
                annotated_table_dict['tableDirection']=v
            
        # transformations
        elif k=='transformations':
            
            for annotated_table_dict in annotated_table_group_dict['tables']:
            
                annotated_table_dict['transformations']=v
        
        # @id
        elif k=='@id':
            
            annotated_table_group_dict['id']=v
            
        # inherited properties
        elif k in ['aboutUrl','datatype','default','lang','null','ordered',
                   'propertyUrl','required','separator','textDirection',
                   'valueUrl']:
            
            table_group_inherited_properties_cache[k]=v
            
        # common properties
        elif not k in ['@context','tables','dialect','tableSchema','@type']:
            
            annotated_table_group_dict[k]=v
            
            
    #
    for i in range(len(annotated_table_group_dict['tables'])):
        
        annotate_table_dict(
            annotated_table_group_dict['tables'][i],
            metadata_table_group_dict['tables'][i],
            base_url,
            default_language,
            validate,
            table_group_inherited_properties_cache
            )
        
        
        
        
    #... do the foreign key annotations
    # - need to do this after all column name properties are set.
    # - creates list of [list_of_coluns_in_the_table,list_of_columns_in_the_referenced_table]
    
    for i in range(len(metadata_table_group_dict.get('tables',[]))):
        
        metadata_table_dict=metadata_table_group_dict['tables'][i]
        annotated_table_dict=annotated_table_group_dict['tables'][i]
        
        if 'tableSchema' in metadata_table_dict:
            
            metadata_schema_dict=metadata_table_dict['tableSchema']
        
            if 'foreignKeys' in metadata_schema_dict:
            
                foreign_key_definitions=metadata_schema_dict['foreignKeys']
                
                for foreign_key_definition in foreign_key_definitions:
                    
                    fkd_column_reference=\
                        foreign_key_definition['columnReference']
                        
                    fkd_columns=\
                        get_columns_from_column_reference(
                            fkd_column_reference,
                            annotated_table_dict,
                            )
                    
                    foreign_key_reference=foreign_key_definition['reference']
                    
                    foriegn_key_reference_table, foriegn_key_reference_columns=\
                        get_referenced_table_and_columns_from_foreign_key_reference(
                            foreign_key_reference,
                            annotated_table_group_dict,
                            metadata_table_group_dict,
                            base_url
                            )
                    
                    annotated_table_dict['foreignKeys'].append(
                        [fkd_columns,foriegn_key_reference_columns]
                        )
    
    #             # referenced rows
                
    #             # -- NEEDS TO BE DONE LATER AS ALL 'VALUE's are None at present.
                
    #             for j in range(len(annotated_table_dict['rows'])):
                    
    #                 print('j',j)
                    
    #                 for foreign_key_definition in annotated_table_dict['foreignKeys']:
                        
    #                     # get foreign key values in this row of this table
                        
    #                     foreign_key_definition_columns=foreign_key_definition[0]
                        
    #                     foreign_key_definition_values=\
    #                         [x['cells'][j]['value'] 
    #                          for x in foreign_key_definition_columns]
                            
    #                     print('foreign_key_definition_values',foreign_key_definition_values)
                            
    #                     # get first row that matches in the reference table
                        
    #                     foreign_key_reference_columns=foreign_key_definition[1]
    #                     foreign_key_reference_table=foreign_key_reference_columns[0]['table']
                        
    #                     first_row=None
                        
    #                     for k in range(len(foreign_key_reference_columns[0]['cells'])):
                            
    #                         print('k',k)
                            
    #                         foreign_key_reference_values=\
    #                             [x['cells'][k]['value'] 
    #                              for x in foreign_key_reference_columns]
                                
    #                         print('foreign_key_reference_values',foreign_key_reference_values)
                                
    #                         if foreign_key_reference_values==foreign_key_definition_values:
                                
    #                             first_row=foreign_key_reference_table['rows'][k]
                                
    #                             break
                                
    #                     if validate:   
                            
    #                         if first_row is None:
                                
    #                             raise Exception
                            
    #                     # append pair to referencedRow property for this row
                        
    #                     #print(remove_recursion(foreign_key_definition))
    #                     #print(remove_recursion(first_row,[]))
                        
    #                     annotated_table_dict['rows'][j]['referencedRows'].append(
    #                         [foreign_key_definition,
    #                          first_row]
    #                         )
                        
    #                     #print(remove_recursion([foreign_key_definition,
    #                     # first_row]))
            
                    
    return annotated_table_group_dict


      
        
#%% 5.4 Tables

def validate_table_metadata(
        metadata_document_location,
        validate=True
        ):
    """High level function to validate a table metadata file.
    """
    with open(metadata_document_location) as f:
        metadata_table_dict=json.load(f)
        
    validate_and_normalize_metadata_table_dict(
            metadata_table_dict,
            metadata_document_location,
            metadata_table_group_dict={},
            )
    
    return metadata_table_dict


def validate_and_normalize_metadata_table_dict(
        metadata_table_dict,
        metadata_document_location,
        metadata_table_group_dict,
        base_url=None,
        default_language=None,
        has_top_level_property=True
        ):
    """
    """
    #print('-metadata_table_dict',metadata_table_dict)
    
    # A table description is a JSON object that describes a table within a CSV file.    
    
    if has_top_level_property:
    
        base_url, default_language=\
            validate_top_level_properties(
                metadata_table_dict,
                metadata_document_location
                )
            
    else:
        
        if '@context' in metadata_table_dict:
        
            message='Metadata table description should not contain a "@context" property. '
            
            raise CSVWError(message)
        
        
        
    # include tableSchema from table group, if not present
    if not 'tableSchema' in metadata_table_dict:
        
        if 'tableSchema' in metadata_table_group_dict:
        
            metadata_table_dict['tableSchema']=\
                metadata_table_group_dict['tableSchema']
        

    # loop through items
    for k in list(metadata_table_dict):
        
        # @context
        if k=='@context':
            
            pass
        
        # url
        elif k=='url':
        
            # This link property gives the single URL of the CSV file 
            # that the table is held in, relative to the location of the 
            # metadata document. 
            # The value of this property is the value of the url 
            # annotation for the annotated table this table description describes.
        
            validate_link_property(
                metadata_table_dict,
                k,
                )
            
            url=metadata_table_dict[k]
            
            if url is None or url=='':
                
                message='Property "url" must have a value.'
                
                raise CSVWError(message)
            
            normalize_link_property(
                metadata_table_dict,
                k,
                base_url
                )
        
        # dialect
        elif k=='dialect':
            
            # As defined for table groups.
            
            validate_object_property(
                metadata_table_dict,
                k,
                )
            
            referenced_url=\
                normalize_object_property(
                    metadata_table_dict,
                    k,
                    base_url
                    )
            
            validate_and_normalize_metadata_dialect_dict(
                    metadata_table_dict[k],
                    referenced_url or metadata_document_location,
                    base_url,
                    default_language,
                    has_top_level_property=True if referenced_url else False,
                    is_referenced=True if referenced_url else False,
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
                None
                )
            
            normalize_common_property_or_notes(
                metadata_table_dict,
                k,
                base_url,
                default_language
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
                expected_types=[bool],
                default_value=False,
                )
            
            normalize_atomic_property(
                metadata_table_dict,
                k,
                )
        
        # tableDirection
        elif k=='tableDirection':
            
            # As defined for table groups. 
            # The value of this property becomes the value of the table 
            # direction annotation for this table.
        
            validate_atomic_property(
                metadata_table_dict,
                k,
                expected_types=[str],
                expected_values=['rtl','ltr','auto'],
                default_value='auto',
                )
            
            normalize_atomic_property(
                metadata_table_dict,
                k,
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
                )
            
            referenced_url=\
                normalize_object_property(
                    metadata_table_dict,
                    k,
                    base_url
                    )
            
            validate_and_normalize_metadata_schema_dict(
                metadata_table_dict[k],
                referenced_url or metadata_document_location,
                metadata_table_group_dict,
                base_url,
                default_language,
                has_top_level_property=True if referenced_url else False,
                is_referenced=True if referenced_url else False,
                )
        
        
        # transformations
        elif k=='transformations':
            
            # As defined for table groups. 
            # The value of this property becomes the value of the 
            # transformations annotation for this table.
            
            validate_array_property(
                metadata_table_dict,
                k,
                [dict]
                )
            
            transformations=metadata_table_dict[k]
            
            # loop through transformations
            for i in range(len(transformations)):
                
                validate_object_property(
                    transformations,
                    i
                    )
                
                referenced_url=\
                    normalize_object_property(
                        transformations,
                        i,
                        base_url
                        )
                
                metadata_transformation_dict=transformations[i]
                
                validate_and_normalize_metadata_transformation_dict(
                        metadata_transformation_dict,
                        base_url,
                        default_language,
                        has_top_level_property=True if referenced_url else False,
                        is_referenced=True if referenced_url else False,
                        metadata_document_location=referenced_url or metadata_document_location
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
                )
            
            id_=metadata_table_dict[k]
            
            if id_.startswith('_:'):
                
                message='Property "@id" must not start with "_:". '
                
                raise CSVWError(message)
            
            normalize_link_property(
                metadata_table_dict,
                k,
                base_url
                )
                
            
        
        # @type
        elif k=='@type':
            
            # If included, @type is an atomic property that must be set 
            # to "Table". 
            # Publishers may include this to provide additional information 
            # to JSON-LD based toolchains.
            
            validate_atomic_property(
                metadata_table_dict,
                k,
                required_values=['Table']
                )
            
            normalize_atomic_property(
                metadata_table_dict,
                k,
                )
        
        
        # inherited properties
        elif k in ['aboutUrl','datatype','default','lang','null','ordered',
                   'propertyUrl','required','separator','textDirection',
                   'valueUrl']:
            
            # The description may contain inherited properties to describe 
            # cells within the table.
            
            validate_and_normalize_inherited_property(
                metadata_table_dict,
                k,
                base_url,
                default_language
                )
            
            
        # common properties
        else:
            
            # The description may contain any common properties to provide 
            # extra metadata about the table as a whole.
            
            validate_and_normalize_common_property(
                metadata_table_dict,
                k,
                base_url,
                default_language
                )
            
    # required properties
    if not 'url' in metadata_table_dict:
        
        message='Property "url" is a required property.'
        
        raise CSVWError(message)
    
    
    
    
    #...Section 5.6
    # If there is no name property defined on this column, 
    # the first titles value having the same language tag as 
    # default language, or und or if no default language is 
    # specified, becomes the name annotation for the described column. 
    # This annotation must be percent-encoded as necessary to 
    # conform to the syntactic requirements defined in [RFC3986].
    
    if has_top_level_property:
    
        if 'tableSchema' in metadata_table_dict:
                
            metadata_schema_dict=metadata_table_dict['tableSchema']
            
            for metadata_column_dict in metadata_schema_dict.get('columns',[]):
    
                if not 'name' in metadata_column_dict:
                    
                    name=get_first_title_for_name_property(
                            metadata_column_dict,
                            default_language
                            )
                    
                    if not name is None:
                        
                        metadata_column_dict['name']=name
                        
                    
                    # if 'titles' in metadata_column_dict:
                    
                    #     x=metadata_column_dict['titles'][default_language][0]
                        
                    #     name=urllib.parse.quote(x.encode('utf8'))
                        
                    #     metadata_column_dict['name']=name
                        
                        
                        
    


def annotate_table_dict(
        annotated_table_dict,
        metadata_table_dict,
        base_url,
        default_language,
        validate,
        table_group_inherited_properties_cache
        ):
    """
    """
    table_inherited_properties_cache=dict(**table_group_inherited_properties_cache)
    
    
    for k,v in metadata_table_dict.items():
        
        # url
        if k=='url':
        
            annotated_table_dict['url']=v
        
        # notes
        elif k=='notes':
            
            if annotated_table_dict['notes']:
                
                annotated_table_dict['notes'].extend(v)
                
            else:
                
                annotated_table_dict['notes']=v
            
        # suppressOutput
        elif k=='suppressOutput':
            
            annotated_table_dict['suppressOutput']=v
        
        # tableDirection
        elif k=='tableDirection':
            
            annotated_table_dict['tableDirection']=v
            
        # tableSchema
        elif k=='tableSchema':
    
            if '@id' in v:
                
                annotated_table_dict['schema']=v['@id']
            
        # transformations
        elif k=='transformations':
            
            annotated_table_dict['transformations']=v
        
        # @id
        elif k=='@id':
            
            annotated_table_dict['id']=v
            
        # inherited properties
        elif k in ['aboutUrl','datatype','default','lang','null','ordered',
                   'propertyUrl','required','separator','textDirection',
                   'valueUrl']:
            
            table_inherited_properties_cache[k]=v
            
        # common properties
        elif not k in ['dialect','@type']:
            
            annotated_table_dict[k]=v
            
    #    
    if 'tableSchema' in metadata_table_dict:
    
        annotate_schema_dict(
            annotated_table_dict,
            metadata_table_dict['tableSchema'],
            base_url,
            default_language,
            validate,
            table_inherited_properties_cache
            )
        
    
    #...provide default column names if needed
    for annotated_column_dict in annotated_table_dict['columns']:
        
        if annotated_column_dict['name'] is None:
    
            column_number=annotated_column_dict['number']
    
            annotated_column_dict['name']=f'_col.{column_number}'


#%% 5.4.3 Table Description Compatibility

def compare_table_descriptions(
        TM,  # table_dict
        EM,  # embedded dict
        validate
        ):
    """
    """
    
    # Two table descriptions are compatible if they have equivalent 
    # normalized url properties, and have compatible schemas as defined 
    # in section 5.5.1 Schema Compatibility.
    
    if not TM['url']==EM['url']:
        
        message='Supplied and embedded metadata table descriptions are '
        message+='not compatibles as their URLs are different. '
        message+=f'Supplied URL is "{TM["url"]}". '
        message+=f'Embedded URL is "{EM["url"]}". '
        
        if validate:
            
            raise CSVWError(message)
            
        else:
        
            warnings.warn(message)
        
    if 'tableSchema' in TM and 'tableSchema' in EM:
        
        lang=TM.get('lang','und')
        
        compare_schema_descriptions(
            TM['tableSchema'],
            EM['tableSchema'],
            lang,
            validate
            )
        

        
            
#%% 5.5 Schemas

def validate_schema_metadata(
        metadata_document_location,
        validate=True
        ):
    """High level function to validate a schema metadata file.
    """
    try:
        
        with open(metadata_document_location) as f:
            metadata_schema_dict=json.load(f)
    
    except FileNotFoundError:
        
        try:
            
            with urllib.request.urlopen(metadata_document_location) as f:
                
                metadata_schema_dict=json.load(f)
                
        except urllib.error.URLError:
            
            message='No file found - metadata document location is not a valid filepath or url.'
            raise CSVWError(message)
            
            
    
    validate_and_normalize_metadata_schema_dict(
            metadata_schema_dict,
            metadata_document_location,
            metadata_table_group_dict={},
            )
    
    return metadata_schema_dict
    

def validate_and_normalize_metadata_schema_dict(
        metadata_schema_dict,
        metadata_document_location,
        metadata_table_group_dict,
        base_url=None,
        default_language=None,
        has_top_level_property=True,
        is_referenced=False
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

    
        
    if has_top_level_property:
    
        base_url, default_language=\
            validate_top_level_properties(
                metadata_schema_dict,
                metadata_document_location
                )
            
    else:
        
        if '@context' in metadata_schema_dict:
        
            message='Metadata schema description should not contain a "@context" property. '
            
            raise CSVWError(message)
            
            
    for k in list(metadata_schema_dict):
        
        if k=='@context':
            
            pass
        
        # columns
        elif k=='columns':
            
            # An array property of column descriptions as described in 
            # section 5.6 Columns. 
            # These are matched to columns in tables that use the schema 
            # by position: the first column description in the array applies 
            # to the first column in the table, the second to the second and so on.

            # The name properties of the column descriptions must be unique 
            # within a given table description.
            
            validate_array_property(
                metadata_schema_dict,
                k,
                expected_types=[dict]
                )
            
            name_cache=[]
            previous_is_virtual=False
            
            columns=metadata_schema_dict[k]
            
            for column_number, metadata_column_dict in enumerate(columns):
            
                validate_and_normalize_metadata_column_dict(
                        metadata_column_dict,
                        metadata_document_location,
                        base_url,
                        default_language,
                        column_number
                        )
                
                if 'name' in metadata_column_dict:
       
                    name=metadata_column_dict['name']
                
                    if name in name_cache:
                        
                        message=f'Property "name" with value "{name}" '
                        message+='is not unique in the column descriptions.'
                        
                        raise CSVWError(message)
                        
                    else:
                        
                        name_cache.append(name)
                    
                # check if virual columns come after non-virtual columns
                is_virtual=metadata_column_dict.get('virtual',False)
                    
                if not is_virtual and previous_is_virtual:
                    
                    message='A non-virtual column cannot come after a virtual column.'
                    
                    raise CSVWError(message)
                    
                previous_is_virtual=is_virtual
                
            
        # foreignKeys
        elif k=='foreignKeys':
            
            # An array property of foreign key definitions that define how 
            # the values from specified columns within this table link to 
            # rows within this table or other tables. 
            
            # A foreign key definition is a JSON object that must contain 
            # only the following properties:

            # columnReference
            # A column reference property that holds either a single reference to a column description object within this schema, or an array of references. These form the referencing columns for the foreign key definition.
            
            # reference
            # An object property that identifies a referenced table and a set of referenced columns within that table. Its properties are:
            
            # resource
            # A link property holding a URL that is the identifier for a specific table that is being referenced. If this property is present then schemaReference must not be present. The table group must contain a table whose url annotation is identical to the expanded value of this property. That table is the referenced table.
            
            # schemaReference
            # A link property holding a URL that is the identifier for a schema that is being referenced. If this property is present then resource must not be present. The table group must contain a table with a tableSchema having a @id that is identical to the expanded value of this property, and there must not be more than one such table. That table is the referenced table.
            
            # columnReference
            # A column reference property that holds either a single reference (by name) to a column description object within the tableSchema of the referenced table, or an array of such references.
            
            # The value of this property becomes the foreign keys annotation on the table using this schema by creating a list of foreign keys comprising a list of columns in the table and a list of columns in the referenced table. The value of this property is also used to create the value of the referenced rows annotation on each of the rows in the table that uses this schema, which is a pair of the relevant foreign key and the referenced row in the referenced table.
            
            # As defined in [tabular-data-model], validators must check that, for each row, the combination of cells in the referencing columns references a unique row within the referenced table through a combination of cells in the referenced columns. For examples, see section 5.5.2.1 Foreign Key Reference Between Tables and section 5.5.2.2 Foreign Key Reference Between Schemas.
            
            # NOTE
            # It is not required for the table or schema referenced from a foreignKeys property to have a similarly defined primaryKey, though frequently it will.
            
            validate_array_property(
                metadata_schema_dict,
                k,
                expected_types=[dict]
                )
            
            for foreign_key_definition in metadata_schema_dict[k]:
                
                #...check keys in foreign_key_definition
                if not set(list(foreign_key_definition))==\
                    set(list(['columnReference','reference'])):
                    
                    raise CSVWError
                
                #...validate column reference property
                validate_column_reference_property(
                        foreign_key_definition,
                        'columnReference',
                        )
            
                #...validate_reference_property
                validate_object_property(
                        foreign_key_definition,
                        'reference'
                        )
                
                reference=foreign_key_definition['reference']
                
                #...check keys in reference
                if not set(list(reference))==\
                    set(list(['resource','columnReference'])) and \
                    not set(list(reference))==\
                        set(list(['schemaReference','columnReference'])):
                    
                    raise CSVWError
                
                # resource (in reference)
                if 'resource' in reference:
                    
                    validate_link_property(
                            reference,
                            'resource'
                            )
                    
                    normalize_link_property(
                            reference,
                            'resource',
                            base_url
                            )
                
                # schemaReference (in reference)
                if 'schemaReference' in reference:
                    
                    validate_link_property(
                            reference,
                            'schemaReference'
                            )
                    
                    normalize_link_property(
                            reference,
                            'schemaReference',
                            base_url
                            )
                
                # columnReference (in reference)
                validate_column_reference_property(
                        reference,
                        'columnReference',
                        )
            
            
        # primaryKey
        elif k=='primaryKey':
            
            # A column reference property that holds either a single 
            # reference to a column description object or an array of 
            # references. 
            # The value of this property becomes the primary key annotation 
            # for each row within a table that uses this schema by creating 
            # a list of the cells in that row that are in the referenced columns.

            # As defined in [tabular-data-model], validators must check 
            # that each row has a unique combination of values of cells 
            # in the indicated columns. 
            # For example, if primaryKey is set to ["familyName", "givenName"] 
            # then every row must have a unique value for the combination 
            # of values of cells in the familyName and givenName columns.
            
            validate_column_reference_property(
                metadata_schema_dict,
                k,
                )

            
        # rowTitles
        elif k=='rowTitles':
            
            # A column reference property that holds either a single 
            # reference to a column description object or an array of 
            # references. 
            # The value of this property determines the titles annotation 
            # for each row within a table that uses this schema. 
            # The titles annotation holds the list of the values of the 
            # cells in that row that are in the referenced columns; 
            # if the value is not a string or has no associated language, 
            # it is interpreted as a string with an undefined language (und).
            
            validate_column_reference_property(
                metadata_schema_dict,
                k,
                )
    
            
        # @id
        elif k=='@id':
        
            # If included, @id is a link property that identifies the schema, 
            # as defined in [tabular-data-model], described by this schema 
            # description. 
            # It must not start with _:. 
            
            validate_link_property(
                metadata_schema_dict,
                k,
                )
            
            id_=metadata_schema_dict[k]
                
            if id_.startswith('_:'):
                
                message='Property "@id" must not start with "_:". '
                
                raise CSVWError(message)
            
            normalize_link_property(
                metadata_schema_dict,
                k,
                base_url
                )
            
            
        
        # @type
        elif k=='@type':
            
            # If included, @type is an atomic property that must be set 
            # to "Schema". 
            # Publishers may include this to provide additional information 
            # to JSON-LD based toolchains.
            
            validate_atomic_property(
                metadata_schema_dict,
                k,
                required_values=['Schema']
                )
            
            normalize_atomic_property(
                metadata_schema_dict,
                k,
                )
        
        # inherited properties
        elif k in ['aboutUrl','datatype','default','lang','null','ordered',
                   'propertyUrl','required','separator','textDirection',
                   'valueUrl']:
            
            # The description may contain inherited properties to describe 
            # cells within tables that use this schema.
            
            validate_and_normalize_inherited_property(
                metadata_schema_dict,
                k,
                base_url,
                default_language
                )
            
            
        # common properties
        else:
            
            # The description may contain any common properties to provide 
            # extra metadata about the schema as a whole.
            
            validate_and_normalize_common_property(
                metadata_schema_dict,
                k,
                base_url,
                default_language
                )
        
    
    if is_referenced:
        
        metadata_schema_dict.pop('@context')
        
        if not '@id' in metadata_schema_dict:
            
            metadata_schema_dict['@id']=metadata_document_location
   
        #print('-metadata_schema_dict',metadata_schema_dict)
        
        
    
def annotate_schema_dict(
        annotated_table_dict,
        metadata_schema_dict,
        base_url,
        default_language,
        validate,
        table_inherited_properties_cache
        ):
    """
    """
    schema_inherited_properties_cache=dict(**table_inherited_properties_cache)
    
    for k,v in metadata_schema_dict.items():
        
        # primary key
        if k=='primaryKey':
            
            pass  # need to do this later after all the colum names are set
        
        # row titles
        elif k=='rowTitles':
            
            pass  # need to do this later after all the colum names are set
            
        # inherited properties
        elif k in ['aboutUrl','datatype','default','lang','null','ordered',
                   'propertyUrl','required','separator','textDirection',
                   'valueUrl']:
    
            schema_inherited_properties_cache[k]=v
            
            
    # #...include virtual columns from metadata
    
    for i, metadata_column_dict in \
        enumerate(metadata_schema_dict.get('columns',[])):
            
            if metadata_column_dict.get('virtual')==True:
                
                annotated_column_dict=dict(
                    table=annotated_table_dict, 
                    number=i+1,
                    sourceNumber=None,
                    name=None,
                    titles=[],
                    virtual=False,
                    suppressOutput=False,
                    datatype={'base':'string'}, 
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
    
    # annotate columns
    for i in range(len(metadata_schema_dict.get('columns',[]))):
        
        annotate_column_dict(
            annotated_table_dict['columns'][i],
            metadata_schema_dict['columns'][i],
            base_url,
            default_language,
            validate,
            schema_inherited_properties_cache,
            annotated_table_dict['tableDirection']
            )
        
        

    # primary key
    if 'primaryKey' in metadata_schema_dict:
        
        # A column reference property that holds either a single reference 
        # to a column description object or an array of references. 
        # The value of this property becomes the primary key annotation for 
        # each row within a table that uses this schema by creating a list 
        # of the cells in that row that are in the referenced columns.

        # As defined in [tabular-data-model], validators must check that 
        # each row has a unique combination of values of cells in the 
        # indicated columns. 
        # For example, if primaryKey is set to ["familyName", "givenName"] 
        # then every row must have a unique value for the combination of 
        # values of cells in the familyName and givenName columns.
        
        primary_key=metadata_schema_dict['primaryKey']
        
        if not isinstance(primary_key,list):
            primary_key=[primary_key]
            
        column_indexes=[]
        
        for primary_key_column_name in primary_key:
            
            for i,annotated_column_dict in enumerate(annotated_table_dict['columns']):
                
                if annotated_column_dict['name']==primary_key_column_name:
                    
                    column_indexes.append(i)
                    
        for annotated_row_dict in annotated_table_dict['rows']:
            
            for column_index in column_indexes:
                
                annotated_row_dict['primaryKey'].append(
                    annotated_row_dict['cells'][column_index]
                    )
        
        if validate:
            
            cache=[]
            
            for annotated_row_dict in annotated_table_dict['rows']:
            
                pk=[x['stringValue'] for x in annotated_row_dict['primaryKey']]
                
                if pk in cache:
                    
                    message=f'Primary key does not have a unique comination of values: {pk}.'
                
                    raise CSVWError(message)
                    
                else:
                    
                    cache.append(pk)
        
    
    
def get_referenced_table_and_columns_from_foreign_key_reference(
        foreign_key_reference,
        annotated_table_group_dict,
        metadata_table_group_obj_dict,
        base_url        
        ):
    """
    """
    # resource
    # A link property holding a URL that is the identifier for a specific 
    # table that is being referenced. 
    # If this property is present then schemaReference must not be present. 
    # The table group must contain a table whose url annotation is identical 
    # to the expanded value of this property. 
    # That table is the referenced table.
    resource=foreign_key_reference.get('resource',None)
    
    #print('-resource',resource)
    
    if not resource is None:
    
        referenced_annotated_table_dicts=\
            [annotated_table_dict
             for annotated_table_dict in annotated_table_group_dict['tables']
             if annotated_table_dict['url']==resource
             ]
        
        if not len(referenced_annotated_table_dicts)==1:
            
            raise Exception
    
        else:
            
            referenced_annotated_table_dict=referenced_annotated_table_dicts[0]
    
    # schemaReference
    # A link property holding a URL that is the identifier for a schema 
    # that is being referenced. 
    # If this property is present then resource must not be present. 
    # The table group must contain a table with a tableSchema having 
    # a @id that is identical to the expanded value of this property, 
    # and there must not be more than one such table. 
    # That table is the referenced table.
    schema_reference=foreign_key_reference.get('schemaReference',None)
    
    if not resource is None and not schema_reference is None:
        
        raise Exception
    
    if not schema_reference is None:
        
        referenced_metadata_table_dicts_indexes=\
            [i
             for i, metadata_table_dict 
             in enumerate(metadata_table_group_obj_dict['tables'])
             if metadata_table_dict['tableSchema'].get('@id',None)==schema_reference
             ]
            
        if not len(referenced_metadata_table_dicts_indexes)==1:
            
            raise Exception
    
        else:
            
            referenced_annotated_table_dict=\
                annotated_table_group_dict['tables'][referenced_metadata_table_dicts_indexes[0]]
    
    # columnReference
    # A column reference property that holds either a single reference 
    # (by name) to a column description object within the tableSchema 
    # of the referenced table, or an array of such references.
    
    column_reference=foreign_key_reference['columnReference']
    
    referenced_annotated_column_dicts=\
        get_columns_from_column_reference(
            column_reference,
            referenced_annotated_table_dict,
            )
    
    # The value of this property becomes the foreign keys annotation on the 
    # table using this schema by creating a list of foreign keys comprising 
    # a list of columns in the table and a list of columns in the 
    # referenced table. 
    # The value of this property is also used to create the value of the 
    # referenced rows annotation on each of the rows in the table that 
    # uses this schema, which is a pair of the relevant foreign key and the 
    # referenced row in the referenced table.

    # As defined in [tabular-data-model], validators must check that, 
    # for each row, the combination of cells in the referencing columns 
    # references a unique row within the referenced table through a 
    # combination of cells in the referenced columns. 
    # For examples, see section 5.5.2.1 Foreign Key Reference Between 
    # Tables and section 5.5.2.2 Foreign Key Reference Between Schemas.
    
    # TO DO
    
    return referenced_annotated_table_dict, referenced_annotated_column_dicts
    
    
    
    
#%% 5.5.1 Schema Compatibility

def compare_schema_descriptions(
        TM_schema,
        EM_schema,
        lang,
        validate
        ):
    """
    """
    lang=TM_schema.get('lang',lang)
    
    # Two schemas are compatible if they have the same number of non-virtual 
    # column descriptions, and the non-virtual column descriptions at the 
    # same index within each are compatible with each other. 
    
    TM_non_virtual_columns=[x for x in TM_schema.get('columns',[]) 
                            if x.get('virtual',False)==False]
    EM_non_virtual_columns=[x for x in EM_schema.get('columns',[]) 
                            if x.get('virtual',False)==False]
    
    if not len(TM_non_virtual_columns)==len(EM_non_virtual_columns):
        
        message='Supplied and embedded metadata schema descriptions are '
        message+='not compatible as they have a different number of virtual columns. '
        message+=f'Supplied number of virtual columns is "{len(TM_non_virtual_columns)}". '
        message+=f'Embedded number of virtual columns is "{len(EM_non_virtual_columns)}". '
        
        if validate:
            
            raise CSVWError(message)
            
        else:
        
            warnings.warn(message)
        
    # Column descriptions are compatible under the following conditions:
    # - If either column description has neither name nor titles properties.
    # - If there is a case-sensitive match between the name properties of the columns.
    # - If there is a non-empty case-sensitive intersection between the 
    #   titles values, where matches must have a matching language; und 
    #   matches any language, and languages match if they are equal when 
    #   truncated, as defined in [BCP47], to the length of the shortest language tag.
    # - If not validating, and one schema has a name property but not a 
    # titles property, and the other has a titles property but not a name property.
    
    
    for i in range(len(TM_non_virtual_columns)):
        
        TM_column=TM_non_virtual_columns[i]
        EM_column=EM_non_virtual_columns[i]
        
        lang=TM_column.get('lang',lang)
        
        #print('i',i)
        #print(validate)
        
        #
        if not 'name' in TM_column and not 'titles' in TM_column:
            
            continue
        
        if not 'name' in EM_column and not 'titles' in EM_column:
            
            continue
        
        #print('test')
        
        #
        if 'name' in TM_column and 'name' in EM_column:
            if TM_column['name']==EM_column['name']:
                continue
        
        
        
        #
        intersection=False
        
        TM_column_titles=TM_column.get('titles',{})
        if isinstance(TM_column_titles,str):
            TM_column_titles=[TM_column_titles]
        if isinstance(TM_column_titles,list):
            TM_column_titles={lang:TM_column_titles}
            
        EM_column_titles=EM_column.get('titles',{})
        if isinstance(EM_column_titles,str):
            EM_column_titles=[EM_column_titles]
        if isinstance(EM_column_titles,list):
            EM_column_titles={lang:EM_column_titles}
                
        
        
        for TM_lang_tag,TM_titles in TM_column_titles.items():
            
            
            
            for EM_lang_tag,EM_titles in EM_column_titles.items():
                
                TM_lang_tag=langcodes.standardize_tag(TM_lang_tag)
                EM_lang_tag=langcodes.standardize_tag(EM_lang_tag)
                
                if not EM_lang_tag=='und':
                    if len(TM_lang_tag)<len(EM_lang_tag):
                        EM_lang_tag=EM_lang_tag[:len(TM_lang_tag)]
                if not TM_lang_tag=='und':
                    if len(EM_lang_tag)<len(TM_lang_tag):
                        TM_lang_tag=TM_lang_tag[:len(EM_lang_tag)]
                        
                #print('TM_lang_tag,TM_titles',TM_lang_tag,TM_titles)
                #print('TM_lang_tag,TM_titles',TM_lang_tag,TM_titles)
                
                
                if TM_lang_tag=='und' or EM_lang_tag=='und' \
                    or TM_lang_tag==EM_lang_tag:
                            
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
        
        
        #
        if not validate:
            if ('name' in TM_column 
                and not 'titles' in TM_column
                and not 'name' in EM_column 
                and 'titles' in EM_column
                ):
                continue
            
            if (not 'name' in TM_column 
                and 'titles' in TM_column
                and 'name' in EM_column 
                and not 'titles' in EM_column
                ):
                continue
            
            
        message='Supplied and embedded metadata columns descriptions are '
        message+='not compatible. '
        message+=f'Supplied column is "{TM_column}". '
        message+=f'Embedded column is "{EM_column}". '
        
        if validate:
            
            raise CSVWError(message)
            
        else:
        
            warnings.warn(message)
        
        
    # NOTE
    # A column description within embedded metadata where the header 
    # dialect property is false will have neither name nor titles properties.
            
    

    
#%% 5.6 Columns

def validate_and_normalize_metadata_column_dict(
        metadata_column_dict,
        metadata_document_location,
        base_url,
        default_language,
        column_number
        ):
    """
    """
    # A column description is a JSON object that describes a single column. 
    # The description provides additional human-readable documentation for 
    # a column, as well as additional information that may be used to 
    # validate the cells within the column, create a user interface for 
    # data entry, or inform conversion into other formats. 
    # All properties are optional.
    
    for k in list(metadata_column_dict):
        
        # name
        if k=='name':
            
            # An atomic property that gives a single canonical name for 
            # the column. 
            # The value of this property becomes the name annotation 
            # for the described column. 
            # This must be a string and this property has no default value, 
            # which means it must be ignored if the supplied value is not a string.

            validate_atomic_property(
                metadata_column_dict,
                k,
                expected_types=[str],
                use_default_value=False
                )
            
            if k in metadata_column_dict:
                
                name=metadata_column_dict[k]
                
                name_percent_encoded=urllib.parse.quote(name.encode('utf8'),safe='_')
                #name_percent_encoded=name_percent_encoded.replace('%20','_')
                
                if not name_percent_encoded==name:
                    
                    message=f'Property "name" with value "{name}" is invalid.'
                    message+='Column names are restricted as defined in Variables in [URI-TEMPLATE]'
                    message+='Property is removed".'
                    
                    warnings.warn(message)
                    
                    metadata_column_dict.pop(k)
                    
            normalize_atomic_property(
                metadata_column_dict,
                k,
                )

            # For ease of reference within URI template properties, 
            # column names are restricted as defined in Variables in 
            # [URI-TEMPLATE] with the additional provision that names 
            # beginning with "_" are reserved by this specification and 
            # must not be used within metadata documents.
        
            # --NOTE--
            # Currently not validating on URI-TEMPLATE restrictions...
            # ?? TO DO ??
            
            if 'name' in metadata_column_dict:
            
                name=metadata_column_dict[k]
                
                if name.startswith('_'):
                    
                    message=f'Property "name" with value "{name}" is not valid. '
                    message+='Value must not start with "_". '
                    message+='Property is removed".'
                    
                    warnings.warn(message)
                    
                    metadata_column_dict.pop(k)
        
        # suppressOutput
        elif k=='suppressOutput':
        
            # A boolean atomic property. 
            # If true, suppresses any output that would be generated when 
            # converting cells in this column. 
            # The value of this property becomes the value of the suppress 
            # output annotation for the described column. 
            # The default is false.
            
            validate_atomic_property(
                metadata_column_dict,
                k,
                expected_types=[bool],
                default_value=False,
                )
            
            normalize_atomic_property(
                metadata_column_dict,
                k,
                )
            
        # titles
        elif k=='titles':
        
            # A natural language property that provides possible alternative 
            # names for the column. 
            # The string values of this property, along with their 
            # associated language tags, become the titles annotation 
            # for the described column.

            # If there is no name property defined on this column, 
            # the first titles value having the same language tag as 
            # default language, or und or if no default language is 
            # specified, becomes the name annotation for the described column. 
            # This annotation must be percent-encoded as necessary to 
            # conform to the syntactic requirements defined in [RFC3986].    
        
            validate_natural_language_property(
                metadata_column_dict,
                k,
                )
            
            normalize_natural_language_property(
                metadata_column_dict,
                k,
                default_language
                )
            
            #...title->name done later on
        
        # virtual
        elif k=='virtual':
        
            # A boolean atomic property taking a single value which 
            # indicates whether the column is a virtual column not present 
            # in the original source. 
            # The default value is false. 
            # The normalized value of this property becomes the virtual 
            # annotation for the described column. 
            # If present, a virtual column must appear after all other 
            # non-virtual column definitions.

            # NOTE
            # Virtual columns are useful for inserting cells with 
            # default values into an annotated table to control the 
            # results of conversions.
            
            validate_atomic_property(
                metadata_column_dict,
                k,
                expected_types=[bool],
                default_value=False,
                )
            
            normalize_atomic_property(
                metadata_column_dict,
                k
                )
                    
        # @id
        elif k=='@id':
        
            # If included, @id is a link property that identifies the 
            # columns, as defined in [tabular-data-model], and potentially 
            # appearing across separate tables, described by this column 
            # description. 
            # It must not start with _:.
        
            validate_link_property(
                metadata_column_dict,
                k,
                )
            
            id_=metadata_column_dict[k]
            
            if id_.startswith('_:'):
                
                message='Property "@id" must not start with "_:". '
                
                raise CSVWError(message)
            
            normalize_link_property(
                metadata_column_dict,
                k,
                base_url
                )
                
            
        
        # type
        elif k=='@type':
        
            # If included, @type is an atomic property that must be set 
            # to "Column". 
            # Publishers may include this to provide additional information 
            # to JSON-LD based toolchains.
            
            validate_atomic_property(
                metadata_column_dict,
                k,
                required_values=['Column']
                )
            
            normalize_atomic_property(
                metadata_column_dict,
                k,
                )
            
        
        # inherited properties
        elif k in ['aboutUrl','datatype','default','lang','null','ordered',
                   'propertyUrl','required','separator','textDirection',
                   'valueUrl']:
            
            # The description may contain inherited properties to describe 
            # cells within the column.
            
            validate_and_normalize_inherited_property(
                metadata_column_dict,
                k,
                base_url,
                default_language
                )
            
            
        # common properties
        else:
            
            # The description may contain any common properties to provide 
            # extra metadata about the column as a whole, such as a full 
            # description.
            
            validate_and_normalize_common_property(
                metadata_column_dict,
                k,
                base_url,
                default_language
                )    
    
    
    
    
    
def annotate_column_dict(
        annotated_column_dict,
        metadata_column_dict,
        base_url,
        default_language,
        validate,
        schema_inherited_properties_cache,
        table_direction
        ):
    """
    """
    column_inherited_properties_cache=dict(**schema_inherited_properties_cache)
    
    for k,v in metadata_column_dict.items():
        
        # name
        if k=='name':
            
            annotated_column_dict['name']=v
            
        # suppressOutput
        elif k=='suppressOutput':
            
            annotated_column_dict['suppressOutput']=v
            
        # title
        elif k=='titles':
            
            x=[]
            
            for lang_code,titles in v.items():  # lang_code, list of titles
            
                for title in titles:
                    
                    x.append(
                        {'@value':title,
                         '@language':lang_code
                            }
                        )
        
            annotated_column_dict['titles']=x
        
        # virtual
        elif k=='virtual':
            
            annotated_column_dict['virtual']=v
            
        # inherited properties
        elif k in ['aboutUrl','datatype','default','lang','null','ordered',
                   'propertyUrl','required','separator','textDirection',
                   'valueUrl']:
    
            column_inherited_properties_cache[k]=v
            
        # common properties
        elif not k in ['@id','@type']:
            
            annotated_column_dict[k]=v
            
            
    # inherited properties
    
    # datatype
    if 'datatype' in column_inherited_properties_cache:
        
        annotated_column_dict['datatype']=column_inherited_properties_cache['datatype']
    
    # default
    if 'default' in column_inherited_properties_cache:
        
        annotated_column_dict['default']=column_inherited_properties_cache['default']
    
    # lang
    if 'lang' in column_inherited_properties_cache:
        
        annotated_column_dict['lang']=column_inherited_properties_cache['lang']
    
    # null
    if 'null' in column_inherited_properties_cache:
        
        annotated_column_dict['null']=column_inherited_properties_cache['null']
    
    # ordered
    if 'ordered' in column_inherited_properties_cache:
        
        ordered=column_inherited_properties_cache['ordered']
        
        annotated_column_dict['ordered']=ordered
        
        for annotated_cell_dict in annotated_column_dict['cells']:
            
            annotated_cell_dict['ordered']=ordered
    
    # required
    if 'required' in column_inherited_properties_cache:
        
        annotated_column_dict['required']=column_inherited_properties_cache['required']
    
    # separator
    if 'separator' in column_inherited_properties_cache:
        
        annotated_column_dict['separator']=column_inherited_properties_cache['separator']
    
    # text direction
    if 'textDirection' in column_inherited_properties_cache:
        
        text_direction=column_inherited_properties_cache['textDirection']
        
        if text_direction=='inherit':
            
            text_direction=table_direction
        
        
        annotated_column_dict['textDirection']=text_direction
        
        for annotated_cell_dict in annotated_column_dict['cells']:
            
            annotated_cell_dict['textDirection']=text_direction
    
    # aboutURL
    if 'aboutUrl' in column_inherited_properties_cache:
        
        annotated_column_dict['aboutURL']=column_inherited_properties_cache['aboutUrl']
    
    # propertyURL
    if 'propertyUrl' in column_inherited_properties_cache:
        
        annotated_column_dict['propertyURL']=column_inherited_properties_cache['propertyUrl']
    
    # valueURL
    if 'valueUrl' in column_inherited_properties_cache:
        
        annotated_column_dict['valueURL']=column_inherited_properties_cache['valueUrl']
    
    # If the column description has neither name nor titles properties, 
    # the string "_col.[N]" where [N] is the column number, becomes the name 
    # annotation for the described column.
    if not 'name' in metadata_column_dict \
        and not 'titles' in metadata_column_dict:
            
            column_number=annotated_column_dict['number']
        
            annotated_column_dict['name']=f'_col.{column_number}'
    
    
def get_first_title_for_name_property(
        metadata_column_dict,
        default_language
        ):
    """
    """
    #...Section 5.6
    # If there is no name property defined on this column, 
    # the first titles value having the same language tag as 
    # default language, or und or if no default language is 
    # specified, becomes the name annotation for the described column. 
    # This annotation must be percent-encoded as necessary to 
    # conform to the syntactic requirements defined in [RFC3986].
    
    name=None
    
    if 'titles' in metadata_column_dict:
        
        titles_dict=metadata_column_dict['titles']
        
        if default_language=='und':
            
            for langcode,title_list in titles_dict.items():
                
                if langcode=='und':
                    
                    if len(title_list)>0:
                        
                        name=title_list[0]
                        
                        break
            
        else:
            
            for langcode,title_list in titles_dict.items():
                
                if langcode.startswith(default_language) \
                    or default_language.startwith(langcode):
                    
                    if len(title_list)>0:
                        
                        name=title_list[0]
                        
                        break
        
        
    if not name is None:

        name=urllib.parse.quote(
            name.encode('utf8'),
            safe=''  # does not include "/" in safe, so that "dd/MM/yyyy" is quoted to "dd%2DMM%2Dyyyy"
            )
        name=name.replace('-','%2D')  # required by rdf test 188

    else:
        
        message='No title exists with default language tag. '
        message+=' Column name property is not set.'
        
        warnings.warn(message)
        
    return name
    

    
            
#%% 5.7 Inherited Properties

def validate_and_normalize_inherited_property(
        metadata_obj_dict,
        property_name,
        base_url,
        default_language
        ):
    """
    """
    # Columns and cells may be assigned annotations based on properties on 
    # the description objects for groups of tables, tables, schemas, or 
    # columns. 
    # These properties are known as inherited properties and are listed below.

    # If an inherited property is not defined on a column description, 
    # it defaults to the first value, if any, found by looking through all 
    # of its containing objects: an inherited property defined in its 
    # containing schema description takes precedence over one defined 
    # in its containing table description, which in turn takes precedence 
    # of one defined in its containing table group description. 
    # This value is used to determine the value of the relevant annotation 
    # on the described column, which is then used to determine the value of 
    # the relevant annotation on the cells in that column.
    
    # aboutUrl
    if property_name=='aboutUrl':
        
        # A URI template property that may be used to indicate what a cell 
        # contains information about. 
        # The value of this property becomes the about URL annotation for 
        # the described column and is used to create the value of the 
        # about URL annotation for the cells within that column as 
        # described in section 5.1.3 URI Template Properties.

        # NOTE
        # aboutUrl is typically defined on a schema description or table 
        # description to indicate what each row is about. 
        # If defined on individual column descriptions, care must be taken 
        # to ensure that transformed cell values maintain a semantic relationship.
        
        validate_uri_template_property(
            metadata_obj_dict,
            property_name,
            )
        
        
    # datatype
    elif property_name=='datatype':
        
        # An atomic property that contains either a single string that is 
        # the main datatype of the values of the cell or a datatype 
        # description object. 
        # If the value of this property is a string, it must be the name 
        # of one of the built-in datatypes defined in section 5.11.1 
        # Built-in Datatypes and this value is normalized to an object 
        # whose base property is the original string value. 
        # If it is an object then it describes a more specialized datatype. 
        # If a cell contains a sequence (i.e. the separator property is 
        # specified and not null) then this property specifies the datatype 
        # of each value within that sequence. See 5.11 Datatypes and 
        # Parsing Cells in [tabular-data-model] for more details.
        
        # The normalized value of this property becomes the datatype 
        # annotation for the described column.
        
        validate_atomic_property(
            metadata_obj_dict, 
            property_name, 
            expected_types=[str,dict],
            )
        
        property_value=metadata_obj_dict[property_name]
        
        if isinstance(property_value,str):
        
            validate_atomic_property(
                metadata_obj_dict, 
                property_name, 
                expected_values=list(datatypes),
                default_value='string'
                )
            
            normalize_atomic_property(
                metadata_obj_dict, 
                property_name, 
                )
            
        else:   # it's a dict
        
            validate_and_normalize_derived_datatype(
                property_value,
                base_url,
                default_language
                )
            
    # default
    elif property_name=='default':
        
        # An atomic property holding a single string that is used to 
        # create a default value for the cell in cases where the original
        # string value is an empty string. 
        # See Parsing Cells in [tabular-data-model] for more details. 
        # If not specified, the default for the default property is the 
        # empty string, "". 
        # The value of this property becomes the default annotation for 
        # the described column.
        
        validate_atomic_property(
            metadata_obj_dict, 
            property_name, 
            expected_types=[str],
            default_value=''
            )
        
        normalize_atomic_property(
            metadata_obj_dict, 
            property_name, 
            )
        
    # lang
    elif property_name=='lang':
        
        # An atomic property giving a single string language code as 
        # defined by [BCP47]. 
        # Indicates the language of the value within the cell. 
        # See Parsing Cells in [tabular-data-model] for more details. 
        # The value of this property becomes the lang annotation for 
        # the described column. 
        # The default is und.
        
        validate_atomic_property(
            metadata_obj_dict, 
            property_name, 
            expected_types=[str],
            default_value='und'
            )
        
        property_value=metadata_obj_dict[property_name]
        
        if not property_value=='und':
        
            if not langcodes.tag_is_valid(property_value):
        
                message='Property "lang" is not a valid language code. ' 
                message+='Replacing with "und".'
        
                warnings.warn(message)
                
                property_value='und'
                
                metadata_obj_dict[property_name]=property_value
        
        
        normalize_atomic_property(
            metadata_obj_dict, 
            property_name, 
            )
        
        
    # null
    elif property_name=='null':
        
        # An atomic property giving the string or strings used for null 
        # values within the data. 
        # If the string value of the cell is equal to any one of these 
        # values, the cell value is null. 
        # See Parsing Cells in [tabular-data-model] for more details. 
        # If not specified, the default for the null property is the
        # empty string "". 
        # The value of this property becomes the null annotation for 
        # the described column.
        
        validate_atomic_property(
            metadata_obj_dict, 
            property_name, 
            expected_types=[str,list],
            default_value=''
            )
        
        property_value=metadata_obj_dict[property_name]
        
        if isinstance(property_value,list):
            
            x=[]
            
            for item in property_value:
                
                if isinstance(property_value,str):
                    
                    x.append(item)
            
                else:
                    
                    message='Property "null" has an invalid array item. '
                    message+='This is removed.'
                    
                    warnings.warn(message)
        
            property_value=x
            
            metadata_obj_dict[property_name]=property_value
        
        
        normalize_atomic_property(
            metadata_obj_dict, 
            property_name, 
            )
        
    # ordered
    elif property_name=='ordered':
        
        # A boolean atomic property taking a single value which indicates 
        # whether a list that is the value of the cell is ordered (if true) 
        # or unordered (if false). 
        # The default is false. 
        # This property is irrelevant if the separator is null or undefined, 
        # but this is not an error. 
        # The value of this property becomes the ordered annotation for 
        # the described column, and the ordered annotation for the cells 
        # within that column.
        
        validate_atomic_property(
            metadata_obj_dict, 
            property_name,
            expected_types=[bool],
            default_value=False
            )
        
        normalize_atomic_property(
            metadata_obj_dict, 
            property_name, 
            )
        
    # propertyUrl
    elif property_name=='propertyUrl':
        
        # A URI template property that may be used to create a URI for a 
        # property if the table is mapped to another format. 
        # The value of this property becomes the property URL annotation 
        # for the described column and is used to create the value of the 
        # property URL annotation for the cells within that column as 
        # described in section 5.1.3 URI Template Properties.

        # NOTE
        # propertyUrl is typically defined on a column description. 
        # If defined on a schema description, table description or table 
        # group description, care must be taken to ensure that transformed 
        # cell values maintain an appropriate semantic relationship, 
        # for example by including the name of the column in the generated 
        # URL by using _name in the template.
        
        validate_uri_template_property(
            metadata_obj_dict,
            property_name
            )
        
    # required
    elif property_name=='required':
        
        # A boolean atomic property taking a single value which indicates 
        # whether the cell value can be null. 
        # See Parsing Cells in [tabular-data-model] for more details. 
        # The default is false, which means cells can have null values. 
        # The value of this property becomes the required annotation 
        # for the described column.
        
        validate_atomic_property(
            metadata_obj_dict, 
            property_name, 
            expected_types=[bool],
            default_value=False
            )
        
        normalize_atomic_property(
            metadata_obj_dict, 
            property_name, 
            )
        
    # separator
    elif property_name=='separator':
        
        # An atomic property that must have a single string value that 
        # is the string used to separate items in the string value of 
        # the cell. 
        # If null (the default) or unspecified, the cell does not 
        # contain a list. 
        # Otherwise, application must split the string value of the cell
        # on the specified separator and parse each of the resulting 
        # strings separately. 
        # The cell's value will then be a list. 
        # See Parsing Cells in [tabular-data-model] for more details. 
        # The value of this property becomes the separator annotation 
        # for the described column.
        
        validate_atomic_property(
            metadata_obj_dict, 
            property_name, 
            expected_types=[str],
            default_value=None
            )
        
        normalize_atomic_property(
            metadata_obj_dict, 
            property_name, 
            )
        
        
        
    # textDirection
    elif property_name=='textDirection':
        
        # An atomic property that must have a single string value that 
        # is one of "ltr", "rtl", "auto" or "inherit" (the default). 
        # Indicates whether the text within cells should be displayed 
        # as left-to-right text (ltr), as right-to-left text (rtl), 
        # according to the content of the cell (auto) or in the direction 
        # inherited from the table direction annotation of the table. 
        # The value of this property determines the text direction 
        # annotation for the column, and the text direction annotation 
        # for the cells within that column: if the value is inherit then 
        # the value of the text direction annotation is the value of the 
        # table direction annotation on the table, otherwise it is the 
        # value of this property. See Bidirectional Tables in 
        # [tabular-data-model] for details.
        
        validate_atomic_property(
            metadata_obj_dict, 
            property_name, 
            expected_types=[str],
            expected_values=['ltr','rtl','auto','inherit'],
            default_value='inherit'
            )
        
        normalize_atomic_property(
            metadata_obj_dict, 
            property_name, 
            )
        
    # valueUrl
    elif property_name=='valueUrl':
        
        # A URI template property that is used to map the values of cells 
        # into URLs. 
        # The value of this property becomes the value URL annotation 
        # for the described column and is used to create the value of the 
        # value URL annotation for the cells within that column as 
        # described in section 5.1.3 URI Template Properties.

        # NOTE
        # This allows processors to build URLs from cell values, 
        # for example to reference RDF resources, as defined in [rdf-concepts]. 
        # For example, if the value URL were "{#reference}", each cell 
        # value of a column named reference would be used to create a 
        # URI such as http://example.com/#1234, if 1234 were a cell 
        # value of that column.
        
        # NOTE
        # valueUrl is typically defined on a column description. 
        # If defined on a schema description, table description or 
        # table group description, care must be taken to ensure that 
        # transformed cell values maintain an appropriate semantic relationship.
                
        validate_uri_template_property(
            metadata_obj_dict,
            property_name
            )
        
    
    


#%% 5.8 Common Properties            
            
def validate_and_normalize_common_property(
        metadata_obj_dict,
        property_name,
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
        metadata_obj_dict,
        property_name
        )
    
    if property_name in metadata_obj_dict:
    
        validate_common_property_value(
            metadata_obj_dict,
            property_name
            )
        
    if property_name in metadata_obj_dict:
    
        normalize_common_property_or_notes(
            metadata_obj_dict,
            property_name,
            base_url,
            default_language
            )
    
    
#%% 5.8.1 Names of Common Properties
    
def validate_common_property_name(
        metadata_obj_dict,
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
        
        if x[0] in prefixes:
            
            return
            
    # test absolute URLs 
    url = hyperlink.parse(property_name)
    
    if not url.absolute:
        
        message=f'Property "{property_name}" is a common property but '
        message+='has name which is not a prefixed name and is not an absolute URL. '
        message+='Property is removed.'
        
        warnings.warn(message)
        
        metadata_obj_dict.pop(property_name)
    

#%% 5.8.2 Values of Common Properties
    
def validate_common_property_value(
        metadata_obj_dict,
        property_name,
        index=None
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
            
        raise CSVWError(message)
        
    
    # Common properties can take any JSON value, so long as any objects 
    # within the value (for example as items of an array or values of 
    # properties on other objects) adhere to the following restrictions, 
    # which are designed to ensure compatibility between JSON-LD-aware 
    # and non-JSON-LD-aware processors:
        
    if index is None:
        
        property_value=metadata_obj_dict[property_name]    
        
    else:
        
        property_value=metadata_obj_dict[property_name][index]
    
    if isinstance(property_value,dict):
        
        if '@value' in property_value:
            
            value=property_value['@value']
            
            # If a @value property is used on an object, that object must not have 
            # any other properties aside from either @type or @language, and must 
            # not have both @type and @language as properties. 
            
            if list(property_value)==['@value']:
                
                pass
            
            elif list(property_value)==['@value','@type']:
                
                pass
            
            elif list(property_value)==['@value','@language']:
                
                pass
            
            else:
                
                message=f'Property "{property_name}" with value "{property_value}" '
                message+='has invalid object properties. '
                message+='As a "@value" property is present, there should '
                message+='only be either a "@type" property or a "@language" property. '
                
                raise CSVWError(message)
            
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
                
                raise CSVWError
            
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
                    
                    raise CSVWError(message)
                
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
                    
                    raise CSVWError(message)
                
                
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
            
        
                elif k=='@id':
                    
                    # The values of @id properties are link properties and are treated as URLs. 
                    #  During normalization, as described in section 6. Normalization, 
                    # they will have any prefix expanded and the result resolved against 
                    # the base URL. 
                    # Therefore, if an @id property is used on an object, it must have a 
                    # value that is a string and that string must not start with _:.
                    
                    if not isinstance(v,str):
                        
                        message='Property "@id" with value "{v}" ({type(v).__name__}) '
                        message+='is invalid. Expected type is string.'
                        
                        raise CSVWError(message)
                        
                    if v.startswith('_:'):
                        
                        message='Property "@id" with value "{v}" ({type(v).__name__}) '
                        message+='is invalid. The value must not start with "-:".'
                        
                        raise CSVWError(message)
        
                    
                elif k=='@language':
                        
                    # A @language property must not be used on an object unless it also 
                    # has a @value property.
                    
                    message='Property "@language" with value "{v}" ({type(v).__name__}) '
                    message+='is invalid. A @language property must not be '
                    message+='used on an object unless it also has a @value property.'
                    
                    raise CSVWError(message)
                
                else:
                    
                    # Aside from @value, @type, @language, and @id, the properties used 
                    # on an object must not start with @.
                    
                    if k.startswith('@'):
                        
                        message=f'Property "{k}" with value "{v}" ({type(v).__name__}) '
                        message+='is invalid. Aside from @value, @type, @language, ' 
                        message+='and @id, the properties used on an object must not start with "@".'
                        
                        raise CSVWError(message)
                    
                    # These restrictions are also described in section A. JSON-LD Dialect, 
                    # from the perspective of a processor that otherwise supports JSON-LD. 
                    # Examples of common property values and the impact of normalization 
                    # are given in section 6.1 Examples.
        
    
    elif isinstance(property_value,list):
        
        for i in range(len(property_value)):
            
            validate_common_property_value(
                metadata_obj_dict,
                property_name,
                index=i
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



#%% 5.9 Dialect Descriptions

def validate_dialect_metadata(
        metadata_document_location,
        validate=True
        ):
    """High level function to validate a schema metadata file.
    """
    with open(metadata_document_location) as f:
        metadata_dialect_dict=json.load(f)
        
    validate_and_normalize_metadata_schema_dict(
            metadata_dialect_dict,
            metadata_document_location,
            )
    
    return metadata_dialect_dict


def validate_and_normalize_metadata_dialect_dict(
        metadata_dialect_dict,
        metadata_document_location,
        base_url=None,
        default_language=None,
        has_top_level_property=True,
        is_referenced=False
        ):
    """
    """
    
    # Much of the tabular data that is published on the web is messy, 
    # and CSV parsers frequently need to be configured in order to 
    # correctly read in CSV. 
    # A dialect description provides hints to parsers about how to parse 
    # the file linked to from the url property in a table description. 
    # It can have any of the following properties, which relate to the 
    # flags described in Section 5 Parsing Tabular Data within 
    # the [tabular-data-model]:

    # NOTE
    # Dialect descriptions do not provide a mechanism for handling CSV files in which there are multiple tables within a single file (e.g. separated by empty lines).
    
    # The default dialect description for CSV files is:
    
    # {
    #   "encoding": "utf-8",
    #   "lineTerminators": ["\r\n", "\n"],
    #   "quoteChar": "\"",
    #   "doubleQuote": true,
    #   "skipRows": 0,
    #   "commentPrefix": "#",
    #   "header": true,
    #   "headerRowCount": 1,
    #   "delimiter": ",",
    #   "skipColumns": 0,
    #   "skipBlankRows": false,
    #   "skipInitialSpace": false,
    #   "trim": false
    # }        


    if has_top_level_property:
    
        base_url, default_language=\
            validate_top_level_properties(
                metadata_dialect_dict,
                metadata_document_location
                )
            
    else:
        
        if '@context' in metadata_dialect_dict:
        
            message='Metadata dialect description should not contain a "@context" property. '
            
            raise CSVWError(message)


    for k in list(metadata_dialect_dict):
        
        

        # commentPrefix
        if k=='commentPrefix':
        
            # An atomic property that sets the comment prefix flag to the 
            # single provided value, which must be a string. 
            # The default is "#".
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[str],
                default_value='#'
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # delimiter
        elif k=='delimiter':
        
            # An atomic property that sets the delimiter flag to the 
            # single provided value, which must be a string. 
            # The default is ",".
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[str],
                default_value=','
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # doubleQuote
        elif k=='doubleQuote':
            
            # A boolean atomic property that, if true, sets the escape 
            # character flag to ". 
            # If false, to \. 
            # The default is true.
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[bool],
                default_value=True
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # encoding
        elif k=='encoding':
            
            # An atomic property that sets the encoding flag to the 
            # single provided string value, which must be a defined 
            # in [encoding]. 
            # The default is "utf-8".
        
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[str],
                expected_values=encoding_labels,
                default_value='utf-8'
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # header
        elif k=='header':
            
            # A boolean atomic property that, if true, sets the header 
            # row count flag to 1, and if false to 0, unless headerRowCount 
            # is provided, in which case the value provided for the header 
            # property is ignored. 
            # The default is true.
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[bool],
                default_value=True
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # headerRowCount
        elif k=='headerRowCount':
            
            # A numeric atomic property that sets the header row count 
            # flag to the single provided value, which must be a 
            # non-negative integer. 
            # The default is 1.
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[int],
                greater_than_or_equal_to=0,
                default_value=1
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # lineTerminators
        elif k=='lineTerminators':
            
            # An atomic property that sets the line terminators flag 
            # to either an array containing the single provided string 
            # value, or the provided array. 
            # The default is ["\r\n", "\n"].
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[str,list],
                default_value=['\r\n','\n']
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # quoteChar
        elif k=='quoteChar':
            
            # An atomic property that sets the quote character flag 
            # to the single provided value, which must be a string or null. 
            # If the value is null, the escape character flag is also 
            # set to null. 
            # The default is '"'.
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[str,type(None)],
                default_value='"'
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # skipBlankRows
        elif k=='skipBlankRows':
            
            # A boolean atomic property that sets the skip blank rows 
            # flag to the single provided boolean value. 
            # The default is false.
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[bool],
                default_value=False
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # skipColumns
        elif k=='skipColumns':
            
            # A numeric atomic property that sets the skip columns 
            # flag to the single provided numeric value, which must be 
            # a non-negative integer. 
            # The default is 0.
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[int],
                greater_than_or_equal_to=0,
                default_value=0
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # skipInitialSpace
        elif k=='skipInitialSpace':
            
            # A boolean atomic property that, if true, sets the trim 
            # flag to "start" and if false, to false. 
            # If the trim property is provided, the skipInitialSpace 
            # property is ignored. 
            # The default is false.
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[bool],
                default_value=False
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # skipRows
        elif k=='skipRows':
            
            # A numeric atomic property that sets the skip rows flag to 
            # the single provided numeric value, which must be a 
            # non-negative integer. 
            # The default is 0.
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[int],
                greater_than_or_equal_to=0,
                default_value=0
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # trim
        elif k=='trim':
            
            # An atomic property that, if the boolean true, sets the 
            # trim flag to true and if the boolean false to false. 
            # If the value provided is a string, sets the trim flag to 
            # the provided value, which must be one of "true", "false", 
            # "start", or "end". 
            # The default is true.
            
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                expected_types=[bool,str],
                expected_values=[True,False,'true','false','start','end'],
                default_value=True
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
        
        # @id
        elif k=='@id':
            
            # If included, @id is a link property that identifies the 
            # dialect described by this dialect description. 
            # It must not start with _:.
        
            validate_link_property(
                metadata_dialect_dict,
                k,
                )
            
            id_=metadata_dialect_dict[k]
                
            if id_.startswith('_:'):
                
                message='Property "@id" must not start with "_:". '
                
                raise CSVWError(message)
            
            normalize_link_property(
                metadata_dialect_dict,
                k,
                base_url
                )
            
            
        
        # @type
        elif k=='@type':
            
            # If included, @type is an atomic property that must be set 
            # to "Dialect". 
            # Publishers may include this to provide additional information 
            # to JSON-LD based toolchains.
        
            validate_atomic_property(
                metadata_dialect_dict,
                k,
                required_values=['Dialect']
                )
            
            normalize_atomic_property(
                metadata_dialect_dict,
                k,
                )
            
            
    #
    if is_referenced:
        
        metadata_dialect_dict.remove('@context')
        
        if not '@id' in metadata_dialect_dict:
            
            metadata_dialect_dict['@id']=metadata_document_location
    


#%% 5.10 Transformation Definitions

def validate_transformation_metadata(
        metadata_document_location,
        validate=True
        ):
    """High level function to validate a schema metadata file.
    """
    with open(metadata_document_location) as f:
        metadata_transformation_dict=json.load(f)
        
    validate_and_normalize_metadata_schema_dict(
            metadata_transformation_dict,
            base_url=None,
            default_language=None,
            has_top_level_property=True,
            metadata_document_location=metadata_document_location
            )
    
    return metadata_transformation_dict
    

def validate_and_normalize_metadata_transformation_dict(
        metadata_transformation_dict,
        base_url,
        default_language,
        has_top_level_property=False,
        metadata_document_location=None,
        is_referenced=False
        ):
    """
    """
    
    if has_top_level_property:
    
        base_url, default_language=\
            validate_top_level_properties(
                metadata_transformation_dict,
                metadata_document_location
                )
            
    else:
        
        if '@context' in metadata_transformation_dict:
        
            message='Metadata transformation description should not contain a "@context" property. '
            
            raise CSVWError(message)
    
    
    
    # Transformation definitions must have the following properties:

    for k in list(metadata_transformation_dict):        

        # url
        if k=='url':
    
            # A link property giving the single URL of the file that the 
            # script or template is held in, relative to the location 
            # of the metadata document.
            
            validate_link_property(
                metadata_transformation_dict,
                k,
                )
            
            normalize_link_property(
                metadata_transformation_dict,
                k,
                base_url
                )
        
        # scriptFormat
        elif k=='scriptFormat':
        
            # A link property giving the single URL for the format 
            # that is used by the script or template. 
            # If one has been defined, this should be a URL for a media 
            # type, in the form 
            # http://www.iana.org/assignments/media-types/media-type such as 
            # http://www.iana.org/assignments/media-types/application/javascript. 
            # Otherwise, it can be any URL that describes the script or 
            # template format.
        
            # NOTE
            # The scriptFormat URL is intended as an informative identifier 
            # for the template format, and applications should not access 
            # the URL. The template formats that an application supports 
            # are implementation defined.
            
            validate_link_property(
                metadata_transformation_dict,
                k,
                )
            
            normalize_link_property(
                metadata_transformation_dict,
                k,
                base_url
                )
        
        # targetFormat
        elif k=='targetFormat':
        
            # A link property giving the single URL for the format that 
            # will be created through the transformation. 
            # If one has been defined, this should be a URL for a media 
            # type, in the form 
            # http://www.iana.org/assignments/media-types/media-type such as 
            # http://www.iana.org/assignments/media-types/text/calendar. 
            # Otherwise, it can be any URL that describes the target format.
        
            # NOTE
            # The targetFormat URL is intended as an informative identifier 
            # for the target format, and applications should not access the URL.
        
            validate_link_property(
                metadata_transformation_dict,
                k,
                )
            
            normalize_link_property(
                metadata_transformation_dict,
                k,
                base_url
                )
        
        # source
        elif k=='source':
        
            # A single string atomic property that provides, if specified, 
            # the format to which the tabular data should be transformed 
            # prior to the transformation using the script or template. 
            # If the value is json, the tabular data must first be 
            # transformed to JSON as defined by [csv2json] using standard mode. 
            # If the value is rdf, the tabular data must first be transformed 
            # to an RDF graph as defined by [csv2rdf] using standard mode. 
            # If the source property is missing or null (the default) then 
            # the source of the transformation is the annotated tabular 
            # data model. 
            # No other values are valid; applications must generate a 
            # warning and behave as if the property had not been specified.
            
            validate_atomic_property(
                metadata_transformation_dict,
                k,
                expected_values=['json','rdf',None]
                )
            
            normalize_atomic_property(
                metadata_transformation_dict,
                k,
                )
        
        # titles
        elif k=='titles':
        
            # A natural language property that describes the format that 
            # will be generated from the transformation. 
            # This is useful if the target format is a generic format 
            # (such as application/json) and the transformation is creating 
            # a specific profile of that format.
            
            validate_natural_language_property(
                metadata_transformation_dict,
                k,
                )
            
            normalize_natural_language_property(
                metadata_transformation_dict,
                k,
                default_language
                )
        
        # @id
        elif k=='@id':
        
            # If included, @id is a link property that identifies the 
            # transformation described by this transformation definition. 
            # It must not start with _:.
            
            validate_link_property(
                metadata_transformation_dict,
                k,
                )
            
            id_=metadata_transformation_dict[k]
                
            if id_.startswith('_:'):
                
                message='Property "@id" must not start with "_:". '
                
                raise CSVWError(message)
            
            normalize_link_property(
                metadata_transformation_dict,
                k,
                base_url
                )
        
        # @type
        elif k=='@type':
        
            # If included, @type is an atomic property that must be set 
            # to "Template". 
            # Publishers may include this to provide additional information to JSON-LD based toolchains.
        
            validate_atomic_property(
                metadata_transformation_dict,
                k,
                required_values=['Template']
                )
            
            normalize_atomic_property(
                metadata_transformation_dict,
                k,
                )
        
        else:
            
            # The transformation definition may contain any common properties 
            # to provide extra metadata about the transformation.
        
            validate_and_normalize_common_property(
                metadata_transformation_dict,
                k,
                base_url,
                default_language
                )
    
    # required properties
    
    # required properties
    if not 'url' in metadata_transformation_dict:
        
        message='Property "url" is a required property.'
        
        raise CSVWError(message)
        
    if not 'scriptFormat' in metadata_transformation_dict:
        
        message='Property "scriptFormat" is a required property.'
        
        raise CSVWError(message)
        
    if not 'targetFormat' in metadata_transformation_dict:
        
        message='Property "targetFormat" is a required property.'
        
        raise CSVWError(message)
    
  
    #
    if is_referenced:
        
        metadata_transformation_dict.remove('@context')
        
        if not '@id' in metadata_transformation_dict:
            
            metadata_transformation_dict['@id']=metadata_document_location
  
    
    
    
    
#%% 5.11 Datatypes


#%% 5.11.2 Derived Datatypes

def validate_and_normalize_derived_datatype(
        metadata_datatype_dict,
        base_url,
        default_language
        ):
    """
    """
    #
    if not 'base' in metadata_datatype_dict:
        
        metadata_datatype_dict['base']='string'
    
    #
    for k in list(metadata_datatype_dict):
        
        # base
        if k=='base':
            
            # An atomic property that contains a single string: the name of
            # one of the built-in datatypes, as listed above (and which 
            # are defined as terms in the default context). 
            # Its default is string. 
            # All values of the datatype must be valid values of the 
            # base datatype. 
            # The value of this property becomes the base annotation for 
            # the described datatype.
            
            validate_atomic_property(
                metadata_datatype_dict, 
                k,
                expected_types=[str],
                expected_values=list(datatypes),
                default_value='string'
                )
            
            normalize_atomic_property(
                metadata_datatype_dict, 
                k, 
                )
        
        # format
        elif k=='format':
            
            # An atomic property that contains either a single string or 
            # an object that defines the format of a value of this type, 
            # used when parsing a string value as described in Parsing 
            # Cells in [tabular-data-model]. 
            # The value of this property becomes the format annotation 
            # for the described datatype.
        
            validate_atomic_property(
                metadata_datatype_dict, 
                k,
                expected_types=[str,dict],
                )
            
            
            if isinstance(metadata_datatype_dict[k],str):
            
                validate_atomic_property(
                    metadata_datatype_dict,
                    'format',
                    expected_types=[str],
                    default_value=None
                    )
            
            elif isinstance(metadata_datatype_dict[k],dict):
                
                for k1 in list(metadata_datatype_dict[k]):
                    
                    # decimalChar
                    if k1=='decimalChar':
                        
                        # A string whose value is used to represent a decimal 
                        # point within the number. 
                        # The default value is ".". 
                        # If the supplied value is not a string, 
                        # implementations must issue a warning and proceed 
                        # as if the property had not been specified.
                        
                        validate_atomic_property(
                            metadata_datatype_dict[k],
                            'decimalChar',
                            expected_types=[str],
                            default_value='.'
                            )
                        
                    # groupChar
                    elif k1=='groupChar':
                        
                        # A string whose value is used to group digits within 
                        # the number. 
                        # The default value is null. 
                        # If the supplied value is not a string, 
                        # implementations must issue a warning and proceed 
                        # as if the property had not been specified.
                        
                        validate_atomic_property(
                            metadata_datatype_dict[k],
                            'groupChar',
                            expected_types=[str],
                            default_value=None
                            )
                        
                    # pattern
                    elif k1=='pattern':
                        
                        # A number format pattern as defined in [UAX35]. 
                        # Implementations must recognise number format 
                        # patterns containing the symbols 0, #, 
                        # the specified decimalChar (or "." if unspecified), 
                        # the specified groupChar (or "," if unspecified), 
                        # E, +, % and ‰. 
                        # Implementations may additionally recognise number 
                        # format patterns containing other special pattern 
                        # characters defined in [UAX35]. 
                        # If the supplied value is not a string, or if it 
                        # contains an invalid number format pattern or uses 
                        # special pattern characters that the implementation 
                        # does not recognise, implementations must issue a 
                        # warning and proceed as if the property had not 
                        # been specified.
                        
                        validate_atomic_property(
                            metadata_datatype_dict[k],
                            'pattern',
                            expected_types=[str],
                            default_value=None
                            )
                        
                    else:
                        
                        message='Property "{k1}" not allowd in format object. '
                        message+='Property is removed.'
                        
                        warnings.warn(message)
                        
                        metadata_datatype_dict[k].pop(k1)
                
                
            
            normalize_atomic_property(
                metadata_datatype_dict, 
                k, 
                )
            
        # length
        elif k=='length': 
        
            # A numeric atomic property that contains a single integer that 
            # is the exact length of the value. 
            # The value of this property becomes the length annotation for 
            # the described datatype. 
            # See Length Constraints in [tabular-data-model] for details.
            
            validate_atomic_property(
                metadata_datatype_dict, 
                k,
                expected_types=[int],
                greater_than_or_equal_to=0
                )
            
            normalize_atomic_property(
                metadata_datatype_dict, 
                k, 
                )
            
        
        # minLength
        elif k=='minLength':
            
            # An atomic property that contains a single integer that is the 
            # minimum length of the value. 
            # The value of this property becomes the minimum length 
            # annotation for the described datatype. 
            # See Length Constraints in [tabular-data-model] for details.
        
            validate_atomic_property(
                metadata_datatype_dict, 
                k,
                expected_types=[int],
                greater_than_or_equal_to=0
                )
            
            normalize_atomic_property(
                metadata_datatype_dict, 
                k, 
                )
        
        
        # maxLength
        elif k=='maxLength':
            
            # A numeric atomic property that contains a single integer 
            # that is the maximum length of the value. 
            # The value of this property becomes the maximum length 
            # annotation for the described datatype. 
            # See Length Constraints in [tabular-data-model] for details.
        
            validate_atomic_property(
                metadata_datatype_dict, 
                k,
                expected_types=[int],
                greater_than_or_equal_to=0
                )
            
            normalize_atomic_property(
                metadata_datatype_dict, 
                k, 
                )
        
        # minimum
        elif k=='minimum':
        
            # An atomic property that contains a single number or string 
            # that is the minimum valid value (inclusive); 
            # equivalent to minInclusive. 
            # The value of this property becomes the minimum annotation 
            # for the described datatype. 
            # See Value Constraints in [tabular-data-model] for details.
            
            validate_atomic_property(
                metadata_datatype_dict, 
                k,
                expected_types=[int,float,str],
                )
            
            normalize_atomic_property(
                metadata_datatype_dict, 
                k, 
                )
            
        
        # maximum
        elif k=='maximum':
        
            # An atomic property that contains a single number or string 
            # that is the maximum valid value (inclusive); 
            # equivalent to maxInclusive. 
            # The value of this property becomes the maximum annotation 
            # for the described datatype. 
            # See Value Constraints in [tabular-data-model] for details.   
            
            validate_atomic_property(
                metadata_datatype_dict, 
                k,
                expected_types=[int,float,str],
                )
            
            normalize_atomic_property(
                metadata_datatype_dict, 
                k, 
                )
        
        # minInclusive
        elif k=='minInclusive':
            
            # An atomic property that contains a single number or string 
            # that is the minimum valid value (inclusive). 
            # The value of this property becomes the minimum annotation 
            # for the described datatype. 
            # See Value Constraints in [tabular-data-model] for details.
            
            validate_atomic_property(
                metadata_datatype_dict, 
                k,
                expected_types=[int,float,str],
                )
            
            normalize_atomic_property(
                metadata_datatype_dict, 
                k, 
                )
        
        # maxInclusive
        elif k=='maxInclusive':
            
            # An atomic property that contains a single number or string 
            # that is the maximum valid value (inclusive). 
            # The value of this property becomes the maximum annotation 
            # for the described datatype. 
            # See Value Constraints in [tabular-data-model] for details.
            
            validate_atomic_property(
                metadata_datatype_dict, 
                k,
                expected_types=[int,float,str],
                )
            
            normalize_atomic_property(
                metadata_datatype_dict, 
                k, 
                )
        
        # minExclusive
        elif k=='minExclusive':
            
            # An atomic property that contains a single number or string 
            # that is the minimum valid value (exclusive). 
            # The value of this property becomes the minimum exclusive 
            # annotation for the described datatype. 
            # See Value Constraints in [tabular-data-model] for details.
            
            validate_atomic_property(
                metadata_datatype_dict, 
                k,
                expected_types=[int,float,str],
                )
            
            normalize_atomic_property(
                metadata_datatype_dict, 
                k, 
                )
            
        # maxExclusive
        elif k=='maxExclusive':
        
            # An atomic property that contains a single number or string 
            # that is the maximum valid value (exclusive). 
            # The value of this property becomes the maximum exclusive 
            # annotation for the described datatype. 
            # See Value Constraints in [tabular-data-model] for details.   
            
            validate_atomic_property(
                metadata_datatype_dict, 
                k,
                expected_types=[int,float,str],
                )
            
            normalize_atomic_property(
                metadata_datatype_dict, 
                k, 
                )
        
        # @id
        elif k=='@id':
            
            # If included, @id is a link property that identifies the 
            # dialect described by this dialect description. 
            # It must not start with _: and it must not be the URL of 
            # a built-in datatype.
        
            validate_link_property(
                metadata_datatype_dict,
                k,
                )
            
            id_=metadata_datatype_dict[k]
                
            if id_.startswith('_:'):
                
                message='Property "@id" must not start with "_:".'
                
                raise CSVWError(message)
                
            if id_ in list(datatypes.values()):
                
                message='Property "@id" must not be the URL of a built-in datatype.'
                
                raise CSVWError(message)
            
            normalize_link_property(
                metadata_datatype_dict,
                k,
                base_url
                )
            
            
        
        # @type
        elif k=='@type':
            
            # If included, @type is an atomic property that must be set 
            # to "Dialect". 
            # Publishers may include this to provide additional information 
            # to JSON-LD based toolchains.
        
            validate_atomic_property(
                metadata_datatype_dict,
                k,
                required_values=['Datatype']
                )
            
            normalize_atomic_property(
                metadata_datatype_dict,
                k,
                )
    
    
        # common properties
        else:
            
            # The description may contain any common properties to provide 
            # extra metadata about the column as a whole, such as a full 
            # description.
            
            validate_and_normalize_common_property(
                metadata_datatype_dict,
                k,
                base_url,
                default_language
                )    
    
    
    # Applications must raise an error if the @id property has the value 
    # of a built-in datatype and any other property is specified. 
    # In these cases, the other properties are ignored.
    if '@id' in metadata_datatype_dict:
        
        if metadata_datatype_dict['@id'] in datatypes:
            
            if len(metadata_datatype_dict)>1:
                
                message='derived datatype error'
                
                raise CSVWError(message)
                
                
    # Applications must raise an error if both length and minLength are 
    # specified and length is less than minLength. 
    if 'length' in metadata_datatype_dict and 'minLength' in metadata_datatype_dict:
        
        if metadata_datatype_dict['length']<metadata_datatype_dict['minLength']:
            
            message='derived datatype error'
                
            raise CSVWError(message)
                
            
    # Similarly, applications must raise an error if both length and 
    # maxLength are specified and length is greater than maxLength. 
    if 'length' in metadata_datatype_dict and 'maxLength' in metadata_datatype_dict:
        
        if metadata_datatype_dict['length']>metadata_datatype_dict['maxLength']:
            
            message='derived datatype error'
                
            raise CSVWError(message)
    
    
    # Applications must raise an error if minLength and maxLength are 
    # both specified and minLength is greater than maxLength. 
    if 'minLength' in metadata_datatype_dict and 'maxLength' in metadata_datatype_dict:
        
        if metadata_datatype_dict['minLength']>metadata_datatype_dict['maxLength']:
            
            message='derived datatype error'
                
            raise CSVWError(message)
    
    
    # Applications must raise an error if length, maxLength, or minLength 
    # are specified and the base datatype is neither string, a subtype of 
    # string, nor a binary type.
    if 'length' in metadata_datatype_dict or \
        'minLength' in metadata_datatype_dict or \
        'maxLength' in metadata_datatype_dict:
        
        base=metadata_datatype_dict.get('base')
        
        if base in datatypes_strings or \
            base in datatypes_binary:
                
                pass
            
        else:
            
            message='derived datatype error'
                
            raise CSVWError(message)
    
    
    # In all ways, including the errors described below, the minimum 
    # property is equivalent to the minInclusive property and the 
    # maximum property is equivalent to the maxInclusive property. 
    # Applications must raise an error if both minimum and minInclusive 
    # are specified and they do not have the same value. 
    if 'minimum' in metadata_datatype_dict and 'minLength' in metadata_datatype_dict:
        
        if metadata_datatype_dict['minimum']==metadata_datatype_dict['minLength']:
            
            pass
        
        else:
            
            message='derived datatype error'
                
            raise CSVWError(message)
    
    
    # Similarly, applications must raise an error if both maximum and 
    # maxInclusive are specified and they do not have the same value.
    if 'maximum' in metadata_datatype_dict and 'maxLength' in metadata_datatype_dict:
        
        if metadata_datatype_dict['maximum']==metadata_datatype_dict['maxLength']:
            
            pass
        
        else:
            
            message='derived datatype error'
                
            raise CSVWError(message)
    
    
    
    # Applications must raise an error if both minInclusive and 
    # minExclusive are specified, or if both maxInclusive and maxExclusive 
    # are specified. 
    if 'minInclusive' in metadata_datatype_dict \
        and 'minExclusive' in metadata_datatype_dict:
        
        message='derived datatype error'
                
        raise CSVWError(message)
    
    if 'maxInclusive' in metadata_datatype_dict \
        and 'maxExclusive' in metadata_datatype_dict:
        
        message='derived datatype error'
                
        raise CSVWError(message)
    
    
    # Applications must raise an error if both minInclusive and 
    # maxInclusive are specified and maxInclusive is less than 
    # minInclusive, or if both minInclusive and maxExclusive are 
    # specified and maxExclusive is less than or equal to minInclusive. 
    if 'minInclusive' in metadata_datatype_dict \
        and 'maxInclusive' in metadata_datatype_dict:
            
        if metadata_datatype_dict['maxInclusive']<metadata_datatype_dict['minInclusive']:
        
            message='derived datatype error'
                    
            raise CSVWError(message)
    
    if 'minInclusive' in metadata_datatype_dict \
        and 'maxExclusive' in metadata_datatype_dict:
            
        if metadata_datatype_dict['maxExclusive']<=metadata_datatype_dict['minInclusive']:
        
            message='derived datatype error'
                    
            raise CSVWError(message)
    
    
    # Similarly, applications must raise an error if both minExclusive 
    # and maxExclusive are specified and maxExclusive is less than 
    # minExclusive, or if both minExclusive and maxInclusive are specified 
    # and maxInclusive is less than or equal to minExclusive.
    if 'minExclusive' in metadata_datatype_dict \
        and 'maxExclusive' in metadata_datatype_dict:
            
        if metadata_datatype_dict['maxExclusive']<metadata_datatype_dict['minExclusive']:
        
            message='derived datatype error'
                    
            raise CSVWError(message)
    
    if 'minExclusive' in metadata_datatype_dict \
        and 'maxInclusive' in metadata_datatype_dict:
            
        if metadata_datatype_dict['maxInclusive']<=metadata_datatype_dict['minExclusive']:
        
            message='derived datatype error'
                    
            raise CSVWError(message)
    
    
    # Applications must raise an error if minimum, minInclusive, maximum, 
    # maxInclusive, minExclusive, or maxExclusive are specified and the base 
    # datatype is not a numeric, date/time, or duration type.
    if 'minimum' in metadata_datatype_dict or \
        'minInclusive' in metadata_datatype_dict or \
        'maximum' in metadata_datatype_dict or \
        'maxInclusive' in metadata_datatype_dict or \
        'minExclusive' in metadata_datatype_dict or \
        'maxExclusive' in metadata_datatype_dict:
            
        base=metadata_datatype_dict.get('base')
        
        if base in datatypes_numbers or \
            base in datatypes_dates_and_times or \
            base in datatypes_durations:
                
            pass
        
        else:
            
            message='derived datatype error'
                    
            raise CSVWError(message)
            
    
    
    # Validation against these properties is as defined in [xmlschema11-2].
    
    # NOTE
    # The format property is not used when interpreting values for the minimum, maximum, minInclusive, maxInclusive, minExclusive, or maxExclusive values. If these values are strings, they must be specified using a valid representation appropriate for the datatype base as defined in [xmlschema11-2]. For example:
    
    # {
    #   "datatype": "date",
    #   "format": "dd/MM/yyyy",
    #   "minimum": "2000-01-01"
    # }
                
    
    
        
#%% Section 6 - Normalization

def normalize_common_property_or_notes(
        metadata_obj_dict,
        property_name,
        base_url,
        default_language
        ):
    """
    """
    property_value=metadata_obj_dict[property_name]

    # 1. If the property is a common property or notes the value must be 
    #    normalized as follows:

    # 1.1 If the value is an array, each value within the array is normalized 
    #     in place as described here.
    if isinstance(property_value,list):
        
        result=[]
        
        for x in property_value:
            
            y={property_name:x}
        
            normalize_common_property_or_notes(
                y,
                property_name,
                base_url,
                default_language
                )
            
            result.append(y[property_name])
    
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
            
            #print(k)
        
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
                    base_url,
                    default_language     
                    )
            
                property_value[k]=y[k]
    
            
    # 1.5 Otherwise, the value remains as is.
    else:
        
        return
    
    #
    metadata_obj_dict[property_name]=property_value


def normalize_link_property(
        metadata_obj_dict,
        property_name,
        base_url
        ):
    """
    """
    property_value=metadata_obj_dict[property_name]
    #print('-property_name',property_name)
    #print('-property_value',property_value)
    #print('-base_url',base_url)
    
    
    # 3 If the property is a link property the value is turned into an 
    #  absolute URL using the base URL and normalized as described in 
    #  URL Normalization [tabular-data-model].

    if not base_url is None:
    
        property_value=\
            urllib.parse.urljoin(
                base_url,
                urllib.parse.quote(property_value,safe=':/#?%')
                )  
            
        property_value=normalize_url(property_value)
        
        #print('-property_value2',property_value)
            
        metadata_obj_dict[property_name]=property_value


def normalize_object_property(
        metadata_obj_dict,
        property_name,
        base_url
        ):
    """
    """
    property_value=metadata_obj_dict[property_name]
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
        
        metadata_document_location=\
            urllib.parse.urljoin(
                base_url,
                property_value
                )  
        
        #...load metadata document
        
        with urllib.request.urlopen(metadata_document_location) as metadata_response:
        
            metadata_text=metadata_response.read().decode()
        
        referenced_metadata_obj_dict=json.loads(metadata_text)
        
        property_value=referenced_metadata_obj_dict
        
        metadata_obj_dict[property_name]=property_value
        
        #print('-property_value',property_value)
        
    else:
        
        metadata_document_location=None
        
    return metadata_document_location

        
def normalize_natural_language_property(
        metadata_obj_dict,
        property_name,
        default_language
        ):
    """
    """
    property_value=metadata_obj_dict[property_name]
    
    # 6 If the property is a natural language property and the value is 
    #  not already an object, it is turned into an object whose properties 
    #  are language codes and where the values of those properties are arrays. 
    #  The suitable language code for the values is determined through the 
    #  default language; if it can't be determined the language code und 
    #  must be used.
    if not isinstance(property_value,dict):
        
        if isinstance(property_value,str):
            
            property_value={default_language:[property_value]}
            
        else:  # i.e. property value is an array
        
            property_value={default_language:property_value}
            
    else:
        
        for k,v in property_value.items():
            
            if not isinstance(v,list):
                
                property_value[k]=[v]
           
    metadata_obj_dict[property_name]=property_value
            
    

        
def normalize_atomic_property(
        metadata_obj_dict,
        property_name
        ):
    """
    """
    # 7 If the property is an atomic property that can be a string or an 
    #  object, normalize to the object form as described for that property.
    
    if property_name in metadata_obj_dict:
    
        property_value=metadata_obj_dict[property_name]
        
        if property_name=='datatype' and isinstance(property_value,str):
            
            property_value={'base':property_value}
        
            metadata_obj_dict[property_name]=property_value
   

#%% A.1. URL Compaction

def compact_absolute_url(
        absolute_url
        ):
    """
    """
    # When normalizing metadata, prefixed names used in common properties 
    # and notes are expanded to absolute URLs. For some serializations, 
    # these are more appropriately presented using prefixed names or terms. 
    # This algorithm compacts an absolute URL to a prefixed name or term.

    # 1 If the URL exactly matches the absolute IRI associated with a term 
    #   in [csvw-context], replace the URL with that term.
    
    # 2 Otherwise, if the URL starts with the absolute IRI associated with 
    #   a term in [csvw-context], replace the matched part of that URL 
    #   with the term separated with a : (U+0040) to create a prefixed name. 
    #   If the resulting prefixed name is rdf:type, replace with @type.

    
    # assuming 'terms in csvw-context' refers to the prefixes -- NEEDS CHECKING
    for k,v in prefixes.items():
        
        if absolute_url==v:
            
            return k
        
        else:
            
            if absolute_url.startswith(v):
                
                compact_url=k+':'+absolute_url[len(v):]
                
                if compact_url=='rdf:type':
                    
                    return '@type'
                
                else:
                    
                    return compact_url
    
    return absolute_url
    

     
        
#%% ---Generating JSON from Tabular Data on the Web---

#%% 4.2 Generating JSON

def replace_string(
        json,
        value,
        replace_value):
    """
    """
    if isinstance(json,str):
        
        return json.replace(value,replace_value)
    
    elif isinstance(json,dict):
        
        return {k.replace(value,replace_value):replace_string(
                    v,
                    value,
                    replace_value
                    ) for k,v in json.items()}
    
    elif isinstance(json,list):
        
        return [replace_string(
                    item,
                    value,
                    replace_value
                    ) for item in json]
    
    
    else:
        
        return json



def create_json_ld(
        annotated_table_group_dict,
        mode='standard',
        local_path_replacement_url=None,  # replaces local path with provided url
        _replace_strings=None  # used to replace urls for testing purposes, can use a variable {table_name}
        ):
    """
    
    :param mode: Either 'standard' or 'minimal'
    
    """
    
    if mode=='minimal':
        
        json_ld=get_minimal_json_from_annotated_table_group(
                annotated_table_group_dict
                )
    
    elif mode=='standard':
        
        json_ld=get_standard_json_from_annotated_table_group(
                annotated_table_group_dict
                )
        
    else:
        
        raise Exception
    
    # replace strings
    
    if _replace_strings is None:
        
        _replace_strings=[]    
    
    if not local_path_replacement_url is None:
        
        local_path='file:///'+os.getcwd().replace('\\','/')+'/'
        local_path_quoted=urllib.parse.quote(local_path,safe=':/#?')
    
        _replace_strings.append(
            (local_path,
              local_path_replacement_url)
            )
        _replace_strings.append(
            (local_path_quoted,
             local_path_replacement_url)
            )
    
    #    
    if not _replace_strings is None:
        
        for value,replace_value in _replace_strings:
            
            json_ld=\
                replace_string(
                    json_ld,
                    value,
                    replace_value
                    )
     
    # can save the json_ld result if needed    
    if False:
        with open('json_ld.json','w') as f:
            
            json.dump(
                json_ld,
                f,
                indent=4
                )
    
    
    return json_ld
    
    

    
    


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
        
        if annotated_table_dict['suppressOutput']==False:
            
            # 2.1 Each row within the table is processed sequentially in order. 
            #     For each row in the current table:
            
            for annotated_row_dict in annotated_table_dict['rows']:
                
                # 2.1.1 Generate a sequence of objects, S1 to Sn, each of 
                #       which corresponds to a subject described by the 
                #       current row, as described in 4.3 Generating Objects.
                
                sequence_of_objects=generate_objects(
                    annotated_row_dict
                    )
                #print(len(sequence_of_objects))
                #print(sequence_of_objects[0]['@id'])
                #print([x for x in sequence_of_objects[0]])
                #print(sequence_of_objects[1]['@id'])
                #print([x for x in sequence_of_objects[1]])
                #print(sequence_of_objects[2]['@id'])
                #print([x for x in sequence_of_objects[2]])
                
                # 2.1.2 As described in 4.4 Generating Nested Objects, 
                # process the sequence of objects, S1 to Sn, to produce a 
                # new sequence of root objects, SR1 to SRm, that may 
                # include nested objects.
                
                sequence_of_root_objects=\
                    generate_nested_objects(
                        annotated_row_dict,
                        sequence_of_objects 
                        )
                
                # 2.1.3 Insert each root object, SR1 to SRm, into array A.
                
                output.extend(sequence_of_root_objects)
                
    return output


def get_standard_json_from_annotated_table_group(
        annotated_table_group_dict
        ):
    """
    """
    # The steps in the algorithm defined here apply to standard mode.

    # 1. Insert an empty object G into the JSON output which is associated 
    #    with the group of tables.
    
    G={}
    
    
    # 2. If the group of tables has an identifier IG; insert the following 
    #    name-value pair into object G:
    #    name @id
    #    value IG
    
    id_=annotated_table_group_dict['id']
    
    if not id_ is None:
        
        G['@id']=id_
    
    # 3 Insert any notes and non-core annotations specified for the group 
    #   of tables into object G according to the rules provided in 5. 
    #   JSON-LD to JSON.
    
    notes=annotated_table_group_dict['notes']
    
    if not notes is False and len(notes)>0:
        
        x=[]
        G['notes']=x
    
        for note in notes:
            
            x.append(get_json_from_json_ld(note))
            
    table_group_core_properties=\
        ['id','tables','notes']
    
    for k,v in annotated_table_group_dict.items():
        
        if not k in table_group_core_properties:
            
            G[k]=get_json_from_json_ld(v)
    
    # 4 Insert the following name-value pair into object G:
    #   name tables
    #   value AT
    #   where AT is an array into which the objects describing the 
    #   annotated tables will be subsequently inserted.
    
    AT=[]
    G['tables']=AT
    
    # 5 Each table is processed sequentially in the order they are 
    #   referenced in the group of tables.
    
    # For each table where the suppress output annotation is false:
    for annotated_table_dict in annotated_table_group_dict['tables']:
        
        if annotated_table_dict['suppressOutput']==False:
            
            # 5.1 Insert an empty object T into the array AT to represent the 
            #     table.
            
            T={}
            AT.append(T)
                    
            # 5.2 If the table has an identifier IT; insert the following 
            #     name-value pair into object T:
            #     name @id
            #     value IT
            
            id_=annotated_table_dict['id']
            
            if not id_ is None:
                
                T['@id']=id_
            
            # 5.3 Specify the source tabular data file URL for the current 
            #     table based on the url annotation; insert the following 
            #     name-value pair into object T:
            #     name url
            #     value URL
            
            T['url']=annotated_table_dict['url']
            
            # 5.4 Insert any notes and non-core annotations specified for the 
            #     table into object T according to the rules provided in 5. 
            #     JSON-LD to JSON.
            
            #     NOTE
            #     All other core annotations for the table are ignored during 
            #     the conversion; including information about table schemas 
            #     and their columns, foreign keys, table direction, 
            #     transformations, etc.

            notes=annotated_table_dict['notes']
            
            if not notes is False and len(notes)>0:
                
                x=[]
                T['notes']=x
            
                for note in notes:
                    
                    x.append(get_json_from_json_ld(note))
                    
            table_core_properties=\
                ['columns','tableDirection','foreignKeys','id','notes',
                 'rows','schema','suppressOutput','transformations',
                 'url']
            
            for k,v in annotated_table_dict.items():
                
                if not k in table_core_properties:
                    
                    T[k]=get_json_from_json_ld(v)
            
            # 5.5 Insert the following name-value pair into object T:
            #     name row
            #     value AR
            #     where AR is an array into which the objects describing the 
            #     rows will be subsequently inserted.
            
            AR=[]
            T['row']=AR
            
            # 5.6 Each row within the table is processed sequentially in order. 
            #     For each row in the current table:
                
            for annotated_row_dict in annotated_table_dict['rows']:
                
                #print('-annotated_row_dict',list(annotated_row_dict))
                
                
                # 5.6.1 Insert an empty object R into the array AR to 
                #       represent the row.
                
                R={}
                AR.append(R)
                
                # 5.6.2 Specify the row number n for the row; insert the 
                #       following name-value pair into object R:
                #       name rownum
                #       value n
                
                R['rownum']=annotated_row_dict['number']
                
                # 5.6.3 Specify the row source number nsource for the row 
                #       within the source tabular data file URL using a 
                #       fragment-identifier as specified in [RFC7111]; 
                #       if row source number is not null, insert the 
                #       following name-value pair into object R:
                #       name url
                #       value URL#row=nsource
                
                source_number=annotated_row_dict['sourceNumber']
                
                if not source_number is None:
                    
                    url=T['url']
                    
                    R['url']=f'{url}#row={source_number}'
                    
                    
                # 5.6.4 Specify any titles for the row; if row titles is 
                #       not null, insert the following name-value pair into 
                #       object R:
                #       name titles
                #       value t
                #       where t is the single value or array of values 
                #       provided by the row titles annotation.
            
                titles=annotated_row_dict['titles']
                
                if not titles is None and len(titles)>0:
                    
                    x=[interpret_datatype(x) for x in titles]
                    
                    if len(x)==1:
                        
                        x=x[0]
                    
                    R['titles']=x

                # NOTE
                # JSON has no native support for expressing language 
                # information; therefore any such information associated with 
                # the row titles is ignored.
                
                # NO CODE NEEDED
                
                
                # 5.6.5 Insert any non-core annotations specified for the 
                #       row into object R according to the rules provided in 
                #       5. JSON-LD to JSON.
                
                row_core_properties=\
                    ['cells','number','primaryKey','titles',
                     'referencedRows','sourceNumber','table']
                
                for k,v in annotated_row_dict.items():
                    
                    if not k in row_core_properties:
                        
                        R[k]=get_json_from_json_ld(v)
                
                # 5.6.6 Insert the following name-value pair into object R:
                #       name describes
                #       value A
                #       where A is an array. The objects containing the 
                #       name-value pairs associated with the cell values will 
                #       be subsequently inserted into this array.
                
                A=[]
                R['describes']=A
                
                # 5.6.7 Generate a sequence of objects, S1 to Sn, each of which 
                #       corresponds to a subject described by the current row, 
                #       as described in 4.3 Generating Objects.
                #       NOTE
                #       The subject(s) described by each row are determined 
                #       according to the about URL annotation for each cell in 
                #       the current row. Where about URL is undefined, 
                #       a default subject for the row is used.
                
                sequence_of_objects=generate_objects(
                    annotated_row_dict
                    )
                
                # 5.6.8 As described in 4.4 Generating Nested Objects, 
                #       process the sequence of objects, S1 to Sn, to produce 
                #       a new sequence of root objects, SR1 to SRm, that may 
                #       include nested objects.
                #       NOTE
                #       A row may describe multiple interrelated subjects; 
                #       where the value URL annotation on one cell matches 
                #       the about URL annotation on another cell in the same 
                #       row.
                
                sequence_of_root_objects=\
                    generate_nested_objects(
                        annotated_row_dict,
                        sequence_of_objects 
                        )
                
                # 5.6.9 Insert each root object, SR1 to SRm, into array A.
                
                A.extend(sequence_of_root_objects)



    return G
        

#%% 4.3 Generating Objects

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
            
            cell_value_or_valueURL=annotated_cell_dict['value']
            
            if (cell_value_or_valueURL is None
                or cell_value_or_valueURL==[]):
                
                cell_value_or_valueURL=annotated_cell_dict['valueURL']
            
            column_suppress_output=annotated_cell_dict['column']['suppressOutput']
            
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
            
            column_suppress_output=annotated_cell_dict['column']['suppressOutput']

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
                    
                    object_name=compact_absolute_url(property_url)
                    
                else:
                    
                    object_name=\
                        urllib.parse.unquote(
                            annotated_cell_dict['column']['name']
                            )
                    
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
                        
                        object_value=compact_absolute_url(value_url)
                        
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
                    
                    object_value=[interpret_datatype(x) 
                                  for x in cell_value]
                        
                        
                # 2.3.4 Else, if the cell value is not null, then the cell 
                #       value provides a single value V for inclusion within the 
                #       JSON output; insert the following name-value pair into 
                #       object Si:
                #       name:N,value:V
                #       Value V derived from the cell values must be expressed 
                #       in the JSON output according to the datatype of the 
                #       value as defined in section 4.5 Interpreting datatypes.
                        
                elif not cell_value is None and not cell_value==[]:
                    
                    object_value=\
                        interpret_datatype(
                            cell_value
                            )
            
                else:
                    
                    object_value=None
                    
                # 2.4 If name N occurs more than once within object Si, 
                #     the name-value pairs from each occurrence of name N 
                #     must be compacted to form a single name-value pair with 
                #     name N and whose value is an array containing all values 
                #     from each of those name-value pairs. Where the value 
                #     from one or more contributing name-value pairs is of 
                #     type array, the values from contributing arrays are 
                #     included directly to the resulting array (i.e. arrays 
                #     of values are flattened).
                
                if not object_value is None:
                
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
  

#%% 4.4 Generating Nested Objects

def generate_nested_objects(
        annotated_row_dict,
        sequence_of_objects 
        ):
    """
    """
    #print(annotated_row_dict)
    #print(sequence_of_objects)
    
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
        #print(value_url)
        
        if not value_url is None and not value_url in cache:
            
            if value_url in url_list:
                cache.append(value_url)
                url_list.remove(value_url)
            
            else:
                url_list.append(value_url)
        
    #print('url_list',url_list)
        
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
                            
                            roots=[node for node in tree['nodes'] for tree in forest
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
    #print('--len(forest_nodes)',len(forest_nodes))
    
    forest_edges=[edge for tree in forest for edge in tree['edges']]
    #print('--len(forest_edges)',len(forest_edges))
    
    for object_ in sequence_of_objects:
        #print("--object_.get('@id',None)", object_.get('@id',None))
        
        M=[node for node in forest_nodes if node['object_']==object_][0]
        
        #print("--M['id_']", M['id_'])
        #print("--M['object_']", M['object_'])
        
        NM_edges=[edge for edge in forest_edges if edge[1]==M]
    
        if len(NM_edges)>0:
            
            NM_edge=NM_edges[0]
            
            N=NM_edge[0]
            
            #print("--N['id_']", N['id_'])
            
            M['root']=False
            
            for k, v in N['object_'].items():
                
                if v==M['id_']:
                    
                    N['object_'][k]=M['object_']
            
    # 5 Return the sequence of root objects, SR1 to SRm.
    
    sequence_of_root_objects=[]
    
    forest_nodes=[node for tree in forest for node in tree['nodes']]
    roots=[node for node in forest_nodes if node['root']==True]
    
    for root in roots:
        
        sequence_of_root_objects.append(root['object_'])
        
    return sequence_of_root_objects
    
    
                        
#%% 4.5 Interpreting datatypes
   
# This is already done in the annotated table dictionary
# Cell values are stored as JSON objects there.


def interpret_datatype(
        value
        ):
    """
    """
    # NOTE
    # Instances of JSON reserved characters within string values must be 
    # escaped as defined in [RFC7159].
    # JSON has no native support for expressing language information; 
    # therefore the language of a value has no effect on the JSON output.
    
    # NOTE
    # Only the base annotation value is used to determine the primitive 
    # type used within the JSON output. 
    # Additional restrictions to the cell value's datatype, such as the 
    # id annotation, are ignored for the purposes of conversion to JSON.
    
    # NOTE
    # A datatype's format is irrelevant to the conversion procedure defined 
    # in this specification; the cell value has already been parsed from 
    # the contents of the cell according to the format annotation.
    # Cell errors must be recorded by applications where the contents 
    # of a cell cannot be parsed or validated (see Parsing Cells and 
    # Validating Tables in [tabular-data-model] respectively). 
    # In cases where cell errors are recorded, applications may attempt 
    # to determine the appropriate JSON primitive type during the 
    # subsequent conversion process according to local rules.
    
    if isinstance(value,list):
        
        return [x['@value'] for x in value]
    
    else:
    
        return value['@value']
    
    
#%% 5 JSON-LD to JSON

# This section defines a mechanism for transforming the [json-ld] dialect 
# used for non-core annotations and notes originating from the processing of 
# metadata (as defined in [tabular-metadata]) into JSON.
    
def get_json_from_json_ld(
        value
        ):
    """
    """
    
    if isinstance(value,dict):
        
        # 1 Name-value pairs from notes and non-core annotations annotations 
        #   are generally copied verbatim from the metadata description 
        #   subject to the exceptions below:
    
        #   Name-value pairs whose value is an object using the [json-ld] 
        #   keyword @value, for example:
        #    name N
        #    value { "@value": "V" }
        
        #   are transformed to
        #    name N
        #    value V
        
        #   Name-value pairs occurring within the value object that use 
        #   [json-ld] keywords @language and @type are ignored.
    
        if '@value' in value:
            
            return value['@value']
        
        # 2 Name-value pairs whose value is an object using the [json-ld] 
        #   keyword @id to coerce a string-value to be interpreted as an 
        #   IRI, for example:
        #    name N
        #    value { "@id": "Vurl" }
        
        #   are transformed to:
        #    name N
        #    value Vurl
        
        elif '@id' in value:
            
            return value['@id']
        
        # In addition to compacting values of property URLs, URLs which were 
        # the value of objects using the [json-ld] keyword @type are 
        # compacted according to the rules as defined in URL Compaction 
        # in [tabular-metadata].
        
        # TO DO
        
        else: 
            
            return {k:get_json_from_json_ld(v) for k,v in value.items()}


    elif isinstance(value,list):
        
        return [get_json_from_json_ld(x) for x in value]
    
    else:
        
        return value



#%%---Generating RDF from Tabular Data on the Web---

#%% 4.2 Generating RDF
    
def create_rdf(
        annotated_table_group_dict,
        mode='standard',
        #convert_any_uri_to_iri=None,
        local_path_replacement_url=None  # replaces local path with provided url
        ):
    """
    """
    
    if mode=='standard':
        
        standard_mode=True
        
    elif mode=='minimal':
        
        standard_mode=False
        
    else:
        
        raise Exception
    
    
        
    #if convert_any_uri_to_iri is None:
    #    convert_any_uri_to_iri=[]
    
    output=[]
    
    # A conformant RDF conversion application must emit triples conforming 
    # to those described in this algorithm according to the chosen mode of 
    # conversion: standard or minimal.

    # Unless specified otherwise, the steps in the algorithm defined herein 
    # apply to both standard and minimal modes.
    
    # NOTE
    # Where an annotated table is defined in isolation (e.g. in the absence 
    # of a group of tables), a default group of tables is provided with a 
    # single tables annotation that refers to the given table.
    
    # The [tabular-data-model] specifies that string values within tabular 
    # data (such as column titles or cell string values) must contain only 
    # Unicode characters. 
    # No Unicode normalization (as specified in [UAX15]) is applied to 
    # these string values during the conversion to RDF.
    
    # NOTE
    # If a CSV file is originally encoded as UTF-8, it should not go 
    # through Unicode normalization during parsing, nor in conversion to RDF. 
    # This can result in RDF literals that are not in Normal Form C as they 
    # should be according to [rdf11-concepts].
    
    #---
    
    # 1 In standard mode only, establish a new node G. 
    # If the group of tables has an identifier then node G must be identified 
    # accordingly; else if identifier is null, then node G must be a new 
    # blank node.
    
    if standard_mode:
        
        table_group_id=annotated_table_group_dict.get('id')
        
        if not table_group_id is None:
            
            G_iri=f'<{table_group_id}>'
            
        else:
            
            G_blank_node_label=str(uuid4())
            
            G_iri=f'_:{G_blank_node_label}'
        

    # 2 In standard mode only, specify the type of node G as csvw:TableGroup; emit the following triple:
    #   - subject node G
    #   - predicate rdf:type
    #   - object csvw:TableGroup
    
    
    
    if standard_mode:
        
        output.append([
            G_iri,
            '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
            '<http://www.w3.org/ns/csvw#TableGroup>'
            ])
    
    # 3 In standard mode only, emit the triples generated by running the 
    # algorithm specified in section 6. JSON-LD to RDF over any notes and 
    # non-core annotations specified for the group of tables, with node G as 
    # an initial subject, the notes or non-core annotation as property, 
    # and the value of the notes or non-core annotation as value.
    
    if standard_mode:
        
        for k,v in annotated_table_group_dict.items():
            
            if not k in ['id','tables']:
                
                output=\
                    json_ld_to_rdf(
                        output,  
                        G_iri,
                        k,
                        v
                        )
        
        
    # 4 For each table where the suppress output annotation is false:
    
    for annotated_table_dict in annotated_table_group_dict['tables']:
        
        if annotated_table_dict['suppressOutput']==False:
            
            # 4.1 In standard mode only, establish a new node T which 
            #     represents the current table.
            #     If the table has an identifier then node T must be 
            #     identified accordingly; else if identifier is null, 
            #     then node T must be a new blank node.
    
            if standard_mode:
                
                table_id=annotated_table_dict.get('id')
                
                if not table_id is None:
                    
                    T_iri=f'<{table_id}>'
                    
                else:
                    
                    T_iri=f'_:{str(uuid4())}'
                
            # 4.2 In standard mode only, relate the table to the group of 
            #     tables; emit the following triple:
            #     - subject node G
            #     - predicate csvw:table
            #     - object node T
            
            if standard_mode:
                
                output.append([
                    G_iri,
                    '<http://www.w3.org/ns/csvw#table>',
                    T_iri
                    ])
            
            # 4.3 In standard mode only, specify the type of node T as 
            #     csvw:Table; emit the following triple:
            #     - subject node T
            #     - predicate rdf:type
            #     - object csvw:Table
            
            if standard_mode:
                
                output.append([
                    T_iri,
                    '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
                    '<http://www.w3.org/ns/csvw#Table>'
                    ])
            
            # 4.4 In standard mode only, specify the source tabular data 
            #     file URL for the current table based on the url annotation; 
            #     emit the following triple:
            #     - subject node T
            #     - predicate csvw:url
            #     - object a node identified by URL
    
            if standard_mode:
                
                url=annotated_table_dict['url']
                
                output.append([
                    T_iri,
                    '<http://www.w3.org/ns/csvw#url>',
                    f'<{url}>'
                    ])
    
            # 4.5 In standard mode only, emit the triples generated by running 
            #     the algorithm specified in section 6. JSON-LD to RDF over 
            #     any notes and non-core annotations specified for the table, 
            #     with node T as an initial subject, the notes or non-core 
            #     annotation as property, and the value of the notes or 
            #     non-core annotation as value.
            #     NOTE: All other core annotations for the table are ignored 
            #     during the conversion; including information about table 
            #     schemas and their columns, foreign keys, table direction, 
            #     transformations, etc.
            if standard_mode:
                
                for k,v in annotated_table_dict.items():
                    
                    if not k in ['columns','tableDirection','foreignKeys','id',
                                 'rows','schema','suppressOutput',
                                 'transformations','url']:
                    
                        output=\
                            json_ld_to_rdf(
                                output,  
                                T_iri,
                                k,
                                v
                                )
            
            
            # 4.6 For each row in the current table:
                
            for annotated_row_dict in annotated_table_dict['rows']:
                
                # 4.6.1 In standard mode only, establish a new blank node R 
                #       which represents the current row.
                
                if standard_mode:
                    
                    R_iri=f'_:{str(uuid4())}'
                
                # 4.6.2 In standard mode only, relate the row to the table; 
                #       emit the following triple:
                #       - subject node T
                #       - predicate csvw:row
                #       - object node R
                
                if standard_mode:
                    
                    output.append([
                        T_iri,
                        '<http://www.w3.org/ns/csvw#row>',
                        R_iri
                        ])
                
                # 4.6.3 In standard mode only, specify the type of node R 
                #       as csvw:Row; emit the following triple:
                #       - subject node R
                #       - predicate rdf:type
                #       - object csvw:Row
                
                if standard_mode:
                    
                    output.append([
                        R_iri,
                        '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
                        '<http://www.w3.org/ns/csvw#Row>'
                        ])
                
                # 4.6.4 In standard mode only, specify the row number n for 
                #       the row; emit the following triple:
                #       - subject node R
                #       - predicate csvw:rownum
                #       - object a literal n; specified with datatype IRI xsd:integer
                
                if standard_mode:
                    
                    output.append([
                        R_iri,
                        '<http://www.w3.org/ns/csvw#rownum>',
                        f'"{annotated_row_dict["number"]}"^^<http://www.w3.org/2001/XMLSchema#integer>'
                        ])
                
                # 4.6.5 In standard mode only, specify the row source number 
                #       nsource for the row within the source tabular data 
                #       file URL using a fragment-identifier as specified in 
                #       [RFC7111]; if row source number is not null, 
                #       emit the following triple:
                #       - subject node R
                #       - predicate csvw:url
                #       - object a node identified by URL#row=nsource
                
                if standard_mode:
                    
                    output.append([
                        R_iri,
                        '<http://www.w3.org/ns/csvw#url>',
                        f'<{url}#row={annotated_row_dict["sourceNumber"]}>'
                        ])
                
                # 4.6.6 In standard mode only, if row titles is not null, 
                #       insert any titles specified for the row. 
                #       For each value, tv, of the row titles annotation, 
                #       emit the following triple:
                #       - subject node R
                #       - predicate csvw:title
                #       - object a literal tv; specified with the the 
                #         appropriate language tag (as defined in [rdf11-concepts]) 
                #         for that row title annotation value
                
                if standard_mode:
                    
                    titles=annotated_row_dict.get('titles')
                    
                    # if not titles is None:  # TO BE DELETED
                        
                    #     if isinstance(titles,list):  # TO BE DELETED
                            
                    for item in titles:
                        
                        if item['@language']=='und':
                            
                            output.append([
                                R_iri,
                                '<http://www.w3.org/ns/csvw#title>',
                                f'"{item["@value"]}"^^<{item["@type"]}>'
                                ])
                            
                        else:
                            
                            output.append([
                                R_iri,
                                '<http://www.w3.org/ns/csvw#title>',
                                f'"{item["@value"]}"^^<{item["@type"]}>'
                                ])
                                
                            
                        # TO BE DELETED
                        
                        # elif isinstance(titles,dict):
                            
                        #     for lang,v1 in titles.items():
                                
                        #         if isinstance(v1,list):
                                    
                        #             for item in v1:
                                        
                        #                 output.append([
                        #                     R_iri,
                        #                     '<http://www.w3.org/ns/csvw#title>',
                        #                     'f"{item}"@{lang}'
                        #                     ])
                                
                        #         else:
                                    
                        #             output.append([
                        #                 R_iri,
                        #                 '<http://www.w3.org/ns/csvw#title>',
                        #                 'f"{v1}"@{lang}'
                        #                 ])
                            
                        # else:
                            
                        #     output.append([
                        #         R_iri,
                        #         '<http://www.w3.org/ns/csvw#title>',
                        #         'f"{titles}"^^<http://www.w3.org/2001/XMLSchema#string>'
                        #         ])
                            
                
                # 4.6.7 In standard mode only, emit the triples generated by 
                #       running the algorithm specified in section 6. 
                #       JSON-LD to RDF over any non-core annotations 
                #       specified for the row, with node R as an initial 
                #       subject, the non-core annotation as property, and 
                #       the value of the non-core annotation as value.
                
                if standard_mode:
                    
                    for k,v in annotated_row_dict.items():
                        
                        if not k in ['cells','number','primaryKey','titles',
                                     'referencedRows','sourceNumber','table']:
                        
                            output=\
                                json_ld_to_rdf(
                                    output,  
                                    R_iri,
                                    k,
                                    v
                                    )
                
        
                # 4.6.8 Establish a new blank node Sdef to be used as the 
                #       default subject for cells where about URL is undefined.
                
                # NOTE
                # A row may describe multiple interrelated subjects; where 
                # the value URL annotation on one cell matches the about URL 
                # annotation on another cell in the same row.
            
                Sdef_blank_node_label=str(uuid4())
                
                # For each cell in the current row where the suppress output 
                # annotation for the column associated with that cell is false:
                
                for annotated_cell_dict in annotated_row_dict['cells']:
                    
                    if annotated_cell_dict['column']['suppressOutput']==False:
                        
                        # 4.6.8.1 Establish a node S from about URL if set, 
                        #         or from Sdef otherwise as the current subject.
                
                        about_url=annotated_cell_dict['aboutURL']
                        
                        if not about_url is None:
                            
                            S_iri=f'<{about_url}>'
                            
                        else:
                            
                            S_iri=f'_:{Sdef_blank_node_label}'
    
                        # 4.6.8.2 In standard mode only, relate the current 
                        #         subject to the current row; emit the 
                        #         following triple:
                        #         - subject node R
                        #         - predicate csvw:describes
                        #         - object node S 
                        
                        if standard_mode:
                            
                            output.append([
                                R_iri,
                                '<http://www.w3.org/ns/csvw#describes>',
                                S_iri
                                ])
                            
                        # 4.6.8.3 If the value of property URL for the cell 
                        #         is not null, then predicate P takes the 
                        #         value of property URL.
                        #         Else, predicate P is constructed by appending 
                        #         the value of the name annotation for the 
                        #         column associated with the cell to the the 
                        #         tabular data file URL as a fragment identifier.
                        
                        property_url=annotated_cell_dict['propertyURL']
                        
                        if not property_url is None:
                            
                            P_iri=f'<{property_url}>'
                            
                        else:
                            
                            name=annotated_cell_dict['column']['name']
                            
                            url=annotated_table_dict['url']
                            
                            P_iri=f'<{url}#{name}>'
                        
                        # 4.6.8.4 If the value URL for the current cell is not 
                        #         null, then value URL identifies a node Vurl 
                        #         that is related the current subject using 
                        #         the predicate P; emit the following triple:
                        #         - subject node S
                        #         - predicate P
                        #         - object node Vurl
                        
                        value_url=annotated_cell_dict['valueURL']
                        
                        value=annotated_cell_dict['value']
                        
                        datatype=annotated_cell_dict['column']['datatype']
                        
                        if not value_url is None:
                            
                            output.append(
                                [S_iri,
                                 P_iri,
                                 f'<{value_url}>']
                                )
                        
                        # 4.6.8.5 Else, if the cell value is a list and the 
                        #         cell ordered annotation is true, then the 
                        #         cell value provides an ordered sequence of 
                        #         literal nodes for inclusion within the RDF 
                        #         output using an instance of rdf:List Vlist 
                        #         as defined in [rdf-schema]. 
                        #         This instance is related to the subject 
                        #         using the predicate P; emit the triples 
                        #         defining list Vlist plus the following triple:
                        #         - subject node S
                        #         - predicate P
                        #         - object node Vlist
                        
                        elif isinstance(value,list) \
                            and annotated_cell_dict['ordered']==True:
                                
                            blank_node_label=str(uuid4())
                            
                            Vlist_iri=f'_:{blank_node_label}'
                            
                            output.append(
                                [S_iri,
                                 P_iri,
                                 Vlist_iri]
                                )
                                
                            for i,v in enumerate(value):
                                
                                Vliteral=\
                                    get_rdf_lexical_form_from_cell_value(
                                        v,
                                        datatype,
                                        #convert_any_uri_to_iri
                                        )
                            
                                output.append(
                                    [Vlist_iri,
                                     '<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>',
                                     Vliteral
                                     ]
                                    )
                                
                                if i==len(value)-1:
                                    
                                    output.append(
                                        [Vlist_iri,
                                         '<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>',
                                         '<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>']
                                        )
                                
                                else:
                                    
                                    blank_node_label=str(uuid4())
                                    
                                    Vlist_iri_next=f'_:{blank_node_label}'
                                    
                                    output.append(
                                        [Vlist_iri,
                                         '<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>',
                                         Vlist_iri_next]
                                        )
                                
                                    Vlist_iri=Vlist_iri_next
                            
                        # 4.6.8.6. Else, if the cell value is a list, then the 
                        #          cell value provides an unordered sequence 
                        #          of literal nodes for inclusion within the 
                        #          RDF output, each of which is related to the 
                        #          subject using the predicate P. 
                        #          For each value provided in the sequence, 
                        #          add a literal node Vliteral; 
                        #          emit the following triple:
                        #          - subject node S
                        #          - predicate P
                        #          - object literal node Vliteral
                        
                        elif isinstance(value,list):
                            
                            for v in value:
                                
                                Vliteral=\
                                    get_rdf_lexical_form_from_cell_value(
                                        v,
                                        datatype,
                                        #convert_any_uri_to_iri
                                        )
                            
                                output.append(
                                    [S_iri,
                                     P_iri,
                                     Vliteral
                                     ]
                                    )
                          
                        # 4.6.8.7 Else, if the cell value is not null, then 
                        #         the cell value provides a single literal 
                        #         node Vliteral for inclusion within the RDF 
                        #         output that is related the current subject 
                        #         using the predicate P; 
                        #         emit the following triple:
                        #         - subject node S
                        #         - predicate P
                        #         - object literal node Vliteral
    
                        elif value is not None:
                            
                            Vliteral=\
                                get_rdf_lexical_form_from_cell_value(
                                    value,
                                    datatype,
                                    #convert_any_uri_to_iri
                                    )
                                
                            output.append(
                                [S_iri,
                                 P_iri,
                                 Vliteral
                                 ]
                                )
                            
                        # The literal nodes derived from the cell values must 
                        # be expressed according to the cell value's datatype 
                        # as defined below: Interpreting datatypes.

                        # NOTE
                        # In the case when a cell value does not have a 
                        # datatype, the conversion should default to string.
                        
                        # NOTE
                        # In the case where a sequence of values is provided, 
                        # each value in the list has its own datatype; 
                        # the datatype may be different for different items 
                        # in the sequence.
    
    
    output=[' '.join(x) for x in output]
    
    output=' .\n'.join(output)
    
    output+=' .'
    
    
    #
    if not local_path_replacement_url is None: 
        
        local_path='file:///'+os.getcwd().replace('\\','/')+'/'
        #print('local_path',local_path)
        
        output=output.replace(
            local_path,
            local_path_replacement_url
            )
        
        #... and again for a % quoted version
        
        local_path_quoted=urllib.parse.quote(local_path,safe=':/#?')
        #print('local_path_quoted',local_path_quoted)
        
        output=output.replace(
            local_path_quoted,
            local_path_replacement_url
            )
        
      
    return output
    
    
    
#%% 4.3 Interpreting datatypes
    
def get_rdf_lexical_form_from_cell_value(
        value,
        datatype,
        #convert_any_uri_to_iri
        ):
    """
    """
    
    # Cell values are expressed in the RDF output according to the cell 
    # value's datatype. 
    # The relationship between the value of the cell value's datatype and 
    # the datatype IRI used in the RDF output is as follows:

    # if the datatype's id annotation is not null, then its value must be used as the RDF datatype IRI;
    
    if '@id' in datatype:
        
        rdf_datatype_iri=f'<{datatype["@id"]}>'
    
    # else, the datatype's base annotation value must be mapped to the RDF datatype IRI as shown below:
    
    else:
            
        rdf_datatype_iri=f'<{value["@type"]}>' 
    
    # NOTE
    # A datatype's format annotation is irrelevant to the conversion procedure 
    # defined in this specification; the cell value has already been parsed 
    # from the contents of the cell according to the format annotation.
    # Cell errors must be recorded by applications where the contents of a 
    # cell cannot be parsed or validated (see Parsing Cells and Validating 
    # Tables in [tabular-data-model] respectively). 
    # In cases where cell errors are recorded, applications may attempt to 
    # determine the appropriate RDF datatype IRI during the subsequent 
    # conversion process according to local rules.
    
    # In the case of rdf:langString, the appropriate language tag (as 
    # defined in [rdf11-concepts]) must be provided for the string, based on 
    # the value of cell value's language. (See section on Graph Literals 
    # in [rdf11-concepts] for further details on language tagged literals.)
    
    # NOTE
    # According to [rdf11-concepts] language tags cannot be combined with 
    # other xsd datatypes. If a cell has any datatype other than string, 
    # the value of lang must be ignored. Also, all literals have a datatype; 
    # however, specific serializations, like [turtle], may provide a special 
    # syntax for literals with datatype xsd:string or rdf:langString.
    
    lexical_form=value['@value']  #...@value can be str, int etc.
    
    if isinstance(lexical_form,str):
        
        pass
    
    elif isinstance(lexical_form,bool):
        
        if lexical_form:
            
            lexical_form='true'
            
        else:
            
            lexical_form='false'
            
    elif isinstance(lexical_form,int):
        
        lexical_form=str(lexical_form)

    elif isinstance(lexical_form,float):
        
        if int(lexical_form)==lexical_form:
            
            lexical_form=str(int(lexical_form))
            
        else:
        
            lexical_form=str(lexical_form)

    else:
        
        raise Exception
    
    
    #...escaping any double quotes in the value
    lexical_form=lexical_form.replace('"','\\"')
    
    if rdf_datatype_iri=='<http://www.w3.org/2001/XMLSchema#string>':
        
        language=value['@language']
        
        if not language is None and not language=='und':
            
            rdf_literal=f'"{lexical_form}"@{language}'
            
        else:
            
            rdf_literal=f'"{lexical_form}"^^{rdf_datatype_iri}'
        
    elif rdf_datatype_iri=='<http://www.w3.org/2001/XMLSchema#anyURI>':
        
        #for x in convert_any_uri_to_iri:
            
        #    if lexical_form.startswith(x):
                
        #        return f'<{lexical_form}>'
                
        rdf_literal=f'"{lexical_form}"^^{rdf_datatype_iri}'
            
    else:
        
        rdf_literal=f'"{lexical_form}"^^{rdf_datatype_iri}'
        
    return rdf_literal
    

#%% 6 JSON-LD to RDF

def json_ld_to_rdf(
        output,  # output list will be modified by this function
        subject,  # an iri <>
        k,  # key
        v  # value
        ):
    ""
    
    # This section defines a mechanism for transforming the [json-ld] 
    # dialect used for non-core annotations and notes originating from 
    # the processing of metadata (as defined in [tabular-metadata]) into 
    # RDF in a manner consistent with the Deserialize JSON-LD to RDF 
    # Algorithm defined in [json-ld-api]. 
    # Converters may use any algorithm which results in equivalent triples.
    
    # NOTE
    # Conversion applications may have other means to create annotated 
    # tables, e.g. through some application specific APIs. 
    # In such cases the exact format for non-core annotations or notes 
    # may be different. 
    # Specifications for such annotation processes should specify 
    # how these annotations are converted into RDF.

    # Given a subject, property and value in normalized form:

    # 1 Property is a term defined in the [csvw-context], a prefixed name, 
    #   or an absolute URL; expand to an absolute URL by replacing a term 
    #   with the URI from the term definition in [csvw-context] or a 
    #   prefixed name as described in Names of Common Properties in 
    #   [tabular-metadata].
    
    def expand(v):
        ""
        
        #print(v, end=' - ')
        
        if v=='notes':
            
            v='http://www.w3.org/ns/csvw#note'
        
        elif v in csvw_terms:
        
            v=csvw_terms[v]
            
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
        
        #print(expanded_url)
        
        return expanded_url
    
    
    #...expand key (property)
    #... not sure how this applies to csvw 'terms'...
    expanded_url=expand(k)
    
    p_iri=f'<{expanded_url}>'
    
    
    # 2 If value is an array, generate RDF by running this algorithm using 
    #   subject, property using each array member as value.
    
    if isinstance(v,list):
        
        for item in v:
            
            output=\
                json_ld_to_rdf(
                    output, 
                    subject,
                    expanded_url,  
                    item  
                    )
    
    
    # 3 If value is an object containing @value, create an RDF Literal lit 
    # using the string value of @value and language from @language, or 
    # datatype from @type if present, expanding @type as necessary using 
    # the procedure outlined for property, and emit the following triple:
    # - subject node subject
    # - predicate property
    # - object literal node lit

    # NOTE
    # If neither @language nor @type is present, the literal lit has the datatype xsd:string.

    elif isinstance(v,dict) and '@value' in v:
        
        if '@type' in v:
            
            type_=expand(v['@type'])
            
            lit=f'"{v["@value"]}"^^<{type_}>' 
            
        elif '@language' in v:
            
            lit=f'"{v["@value"]}"@{v["@language"]}'
        
        else:
            
            lit=f'"{v["@value"]}"^^<http://www.w3.org/2001/XMLSchema#string>'

        output.append((subject, p_iri, lit))

   
    
    # 4 Else, if value is an object:
        
    elif isinstance(v,dict):
    
        # 4.1 Establish a new node S that is identified with the value of 
        # @id if defined, or else as a blank node, and emit the 
        # following triple:
        # - subject node subject
        # - predicate node property
        # - object node S
        
        if '@id' in v:
            
            S_iri=f'<{v["@id"]}>'
            
        else:
            
            S_iri=f'_:{str(uuid4())}'
            
        output.append((subject, p_iri, S_iri))
        
        # 4.2 For every value of @type, either a term defined in the 
        # [csvw-context], a prefixed name, or an absolute URL; 
        # establish a new node Ti by expanding the value to an absolute 
        # URL by replacing a term with the URI from the term definition 
        # in [csvw-context] or a prefixed name with its expanded value. 
        # For each Ti, emit the following triple:
        # - subject node S
        # - predicate rdf:type
        # - object node Ti
        
        if '@type' in v:
            
            type_=v['@type']
            
            if not isinstance(type_,list):
                
                type_=[type_]
                
            for item in type_:
                
                Ti=expand(item)
                
                output.append([
                    S_iri, 
                    '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
                    f'<{Ti}>'
                    ])
        
        # 4.3 For every key and val from value that does not start 
        # with @ (U+0040) generate RDF by running this algorithm using S 
        # for subject, key for property and val for value.
        
        for k1,v1 in v.items():
        
            if not k1.startswith('@'):
                
                output=\
                    json_ld_to_rdf(
                        output, 
                        S_iri,
                        k1,  
                        v1  
                        )
        
        
    # 5 Else, establish lit as an RDF Literal as follows:
        
    else:
        
        # 5.1 If value is true or false, create an RDF Literal lit using 
        # the strings "true" or "false", accordingly with datatype xsd:boolean.
        
        if isinstance(v,bool):
            
            if v:
                
                lit='"true"^^<http://www.w3.org/2001/XMLSchema#boolean>'
                
            else:
                
                lit='"false"^^<http://www.w3.org/2001/XMLSchema#boolean>'
    
        # 5.2 Else, if value is a JSON number with a non-zero fractional 
        # part, create an RDF Literal lit using the canonical representation 
        # for value with datatype xsd:double.
        
        elif isinstance(v,float):
            
            lit=f'"{v}"^^<http://www.w3.org/2001/XMLSchema#double>'
    
        # 5.3 Else, if value is a JSON number with no non-zero fractional 
        # part, create an RDF Literal lit using the canonical 
        # representation for value with datatype xsd:integer.
        
        elif isinstance(v,int):
            
            lit=f'"{v}"^^<http://www.w3.org/2001/XMLSchema#integer>'
    
        # 5.4 Otherwise, create an RDF Literal lit using the canonical 
        # representation for value with datatype xsd:string.
        
        else:
            
            lit=f'"{v}"^^<http://www.w3.org/2001/XMLSchema#string>'
    
        # Emit the following triple:
        # - subject node subject
        # - predicate property
        # - object literal node lit
        
        output.append((subject, p_iri, lit))


    return output



#%%---EXTRA---

def get_errors(
        annotated_table_group_dict
        ):
    ""
    result=[]
    
    for column in annotated_table_group_dict['tables'][0]['columns']:
        
        for cell in column['cells']:
            
            errors=cell['errors']
            
            if len(errors)>0:
                
                result.append(
                    {
                        'column_name':column['name'],
                        'row_number':cell['row']['number'],
                        'errors':errors
                        }
                    )
                
    return result






        
        
        
        
        
        
        
    
    
    
    