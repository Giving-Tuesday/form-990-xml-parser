# Introduction

This is package was designed to process XML versions of FORM 990 Filings hosted on Amazon Web Services.

**Big picture** This scripts works by:

- Downloading all XML Filings hosted by Amazon Web Services for a given year (i.e. within an yearly index)
- Parse the XML document and process xml data according to a mapping of variables and paths
- Store resulting data into Mongo DB & Elastic Search

## Visual overview of main steps:

**Step 1: Downloading & Storing Index from AWS**

![Alt text](images/Picture1.png "Donwload Index")

**Step 2: Downloading, Parsing, & Converting XML documents to JSON documents**

![Alt text](images/Picture2.png "Convert xml to json")

**Step 3: Inserting JSON documents into Mongo**
![Alt text](images/Picture3.png "Insert Data")

## Main files & folders

| File/Folder                   | Description                                                                                                         |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| xml_parser.py                 | This file is the main script (run from commandline) it initiates downloads, updates, and insertions of files/forms. |
| helpers/index_downloader.py   | This file downloads indexes from AWS.                                                                               |
| helpers/parser/form_parser.py | This file contains all methods necessary to parse xml forms.                                                        |
| helpers/database/interface.py | This file contains all methods necessary to connect to mongo as well as to perform CRUD operations |  | helpers/files | This folder contains 2 files which have mappings for the variables i.e. paths and variables we will be storing |
| helpers/helpers.py            | This file contains general purpose methods used by all other scripts.                                               |

## How to use Script from Command Line

Parser commands that can be passed from command line/terminal:

| Command        | Description                                                            | Default     |
| -------------- | ---------------------------------------------------------------------- | ----------- |
| -i {Year}      | Inserting command with year                                            | ----------- |
| -f             | When processing removes and insert forms versus just inserting         | ----------- |
| -l {Number}    | Number of forms that will be inserted simultaneously                   | 1000        |
| -c {Number}    | Location from an index where you want to continue inserting/processing | ----------- |
| -u             | Update forms                                                           | ----------- |
| -cm            | Check if there are new xpaths for adding it into the mapping           | ----------- |
| --mongodb      | Mongo                                                                  | ----------- |
| --qa           | Specifies the environment QA                                           | ----------- |
| --prod         | Specifies the environment PRODUCTION                                   | ----------- |
| --backup       | Specifies the environment BACKUP (production backup)                   | ----------- |

### Example Use Cases

- Insert 2018 data into mongo in production. Use python 2 and nohup means dont terminate process when we logout of ssh session.

```sh
$ nohup python2 ./xml_parser.py -i 2018 --prod --mongodb
```

- Insert 2011 data into mongo in qa

```sh
$ python xml_parser.py -i 2011 --mongodb --qa
```

- Insert 2011 data into mongodb in qa. Insert only 100 documents at a time.

```sh
$ python xml_parser.py -i 2011 -l 100 --mongodb --qa
```

- Insert 2011 into mongo db using force insert - by deleting and reinserting the data.

```sh
$ python xml_parser.py -i 2011 -f --mongodb
```

- Insert 2011 data into mongo db continue from position 20 in the 2011 index.

```sh
$ python xml_parser.py -i 2011 -c 20 --mongodb
```

- Update forms.

```sh
$ python xml_parser.py -u
```

- Insert multiple years 2011 & 2018 into monogo.

```sh
$ python xml_parser.py -i 2011-2018 --mongo
```

## Communicating with Data Stores - i.e. Mongo

- helpers/ helpers.py - Line 70 contains settings for Mongo
- helpers/database/interface.py - Contains methods for dealing with Mongo

## Error Logging

- General errors are also stored at nohup.out
- Remember error logs are simply stored as .log files at the base level of IRS_XML_Parser
- Creation of Logs handled following location: Line 130 & 183 of xml_parser.py
- Mongo logging is handled following locations Lines: 89, 115, 122 of helpers/database/interface.py

# References

- https://aws.amazon.com/public-datasets/irs-990/ (Irs 990 Filings)
- https://s3.amazonaws.com/irs-form-990/index_2011.json (Example of index.json)
