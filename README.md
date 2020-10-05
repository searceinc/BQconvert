# BigQuery Schema Converter

![](cover.jpg)

BQconvert is a Python based tool that helps convert your existing database schema to BigQuery compatible schema and automatically create the converted tables on the BigQuery dataset. It currently supports AWS RedShift as the source database. Adding support for other widely used databases is progress. 

## Background:

Due to BigQuery's serverless, high performance and BigQuery ML kinds of features attract a lot of companies to migrate their current data warehouse solution to BigQuery. While migrating such data warehouses to BigQuery, we may use `bqload` and `autodetect schema` features to load the data and create the table. But this autodetect will scan 100 lines and finalize the schema. There is no guarantee that all the data types are properly selected. 

This BigQuery Schema converter tool will help you to scan your current database/data warehouse and extract the schema from it. Then for each database there is mapping file which map the source data type with the target data type. Then it'll create the DDL. 

You can either directly create the tables on BigQuery from this tool, or save the converted DDL into a file. So you can review the file, then import the DDL file via BQ console of bqcli.

## How it works:

This BQconvert tool will convert the source database schema to BigQuery in two different ways.

1. Query the `information_schema` and get the tables, columns details. Then it'll generate the BigQurey DDL.
2. Parsing the dump file. We can use `pg_dump`,`mysqldump` to backup the database schema. 
	* If you are using RedShift you can use `pg_dump`, generate_table_ddl` view from RedShift utilities or you can have your own SQL file with all the table's DDL. 
	* BigQuery Schema conversion tool internally using [`sqlparse`](https://github.com/andialbrecht/sqlparse) and [`ddlparse`](https://github.com/shinichi-takii/ddlparse/) libraries to parse the SQL file and then extract the table and column information. 

## Supported Databases (Sources)

- RedShift 🆕
- PostgreSQL(`under development`)
- MySQL (`future release`)
- Oracle (`future release`)
- SQL Server (`future release`)
- Tera Data (`future release`)

## Installation:

### Prerequisite:

- **Supported OS:**
    - Ubuntu `18.04 or higher`
    - CentOS/RedHat `6 or higher`
    - Windows `10`, Windows server `2016 or higher`
- **Python:** 3.6 or higher
- **Database Access:** To extract the schema from the source database
- **BigQuery Admin Service account:** A JSON credential file

```bash
wget https://github.com/searceinc/BQconvert/blob/master/bqconvert-latest.zip
unzip bqconvert-latest.zip
cd bqconvert/setup
pip3 install -r requirements.txt
```

### Export Google Service account key:

```bash
-- Linux
export  GOOGLE_APPLICATION_CREDENTIALS=/path/to/keyfile.json

-- Windows (Powershell)
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\Downloads\keyfile.json"

-- Windows (cmd)
set GOOGLE_APPLICATION_CREDENTIALS="C:\path\Downloads\keyfile.json"
```

## Usage:

Invoke the `bqconverter.py` file and pass all the mandatory parameters. 

```bash
python3 bqconverter.py --help

usage: bqconverter.py [-h DB_HOST] [-u DB_USER] [-p DB_PASSWORD] [-P DB_PORT]
                      [-d DB_NAME] [-s SH_WHITELIST] [-b SH_BLACKLIST]
                      [-t TBL_WHITELIST] [-w TBL_BLACKLIST] -S {redshift} -r
                      BQ_PROJECT [-l BQ_LOCATION] -D BQ_DATASET [-c {yes,no}]
                      [-x {yes,no}] [-a {yes,no}] [-m MAPPING] [-i INFILE]
                      [-o OUTFILE] [-H]

BQconverter:- Convert any database schema to BigQuery Tables. Supported
Databases: AWS RedShift.
```

## Flag Options:

#### ```-h, --host```

Databsase server's IP address or RDS, RedShift's endpoint to connect. The public/private IP address of the server or computer needs to be whitelisted to access the database.

#### ```-u, --user```

Database user name. Generally `Read Only` access on all the tables that needs to be converted. 

#### ```-p, --password```

Database user's password. If your password has any special characters then use it inside a single quote. (`example: 'my@com%4pas'`)

#### ```-P, --port```

Database Server's port number. Not all the database servers are using the default port. So its mandatory parameter.

#### ```-d, --database```

Database name to connect and extract the schema, Some databases won't support the cross database access. So use the database name that you want to convert.

#### ```-s, --sh_whitelist```

Whitelist the list of schema names for the conversion. You can use a single schema or multiple schema. If your schema name contains any special character, then use it in single quotes. 

(`example: schema1,schema2` or `'schema1,schema#3'`)

#### ```-b, --sh_blacklist```

List of schema that needs to skipped from the conversion. Any tables from this schema will be skipped. It is an optional argument.

#### ```-t, --tbl_whitelist```

List of tables that needs to be migrated from the `sh_whitelist` schema. All the tables in the whitelisted schema will be converted. This is optional argument.

#### ```-w, --tbl_blacklist```

List of tables needs to be skipped from the conversion from the whitelisted schema or all the schema. This is optional argument.

#### ```-S, --source```

Mention the database source like `redshift or sqlserver` Right now we support only for **AWS RedShift**. So pick the source from the available options. 
* Available Options: `redshift`

#### ```-r, --bq_project```

Name of your GCP project where you want to apply the converted schema. Make sure you have a service account key file from this project with BigQuery admin access.

#### ```-l, --bq_location```

Location of your BigQuery dataset. This option is only useful if you are going to create a new dataset from this tool.

#### ```-D, --bq_dataset```

Name of your BigQuery dataset that needs to be created or already available on GCP to create the converted table's schema. 

#### ```-c, --create```

If you want to create a new dataset to apply the converted table's DDL then use this option. By default it is set to no. 
* Available Options: `YES and NO`

#### ```-x, --drop```

If you want to add the drop table statement on the DDL file and drop the tables if they are already available on your BigQuery dataset before create the converted tables then use this option.
* Available Options: `YES and NO`

#### ```-a, --apply```

Once the conversion done and you want to create the converted schema on your BigQuery tables then you need use this argument. 
* Available Options: `YES and NO`

#### ```-m, --mapping```

We are using a predefined data type mapping for the source databases as per the most comfortable type on BigQuery. If you want to use your own data type mapping then use this argument to mention the path for the mapping file. Its basically a JSON file and looks like the below format. Take a look at [here](https://github.com/searceinc/BQconvert/blob/master/setup/datatype-mapping/redshift.json)

```json
{
	"source_datatype":"target_datatype",
	"other_source_type":"another_target_type"
}
```
#### ```-i, --infile```

If you the table DDL in a SQL file or a SQL dump, then we can parse that SQL file and convert the schema. Use this flag and give the file path to parse.

#### ```-o, --outfile```

If you want to save the converted schema into a SQL file without creating them on BQ then you have to use this option and give the output file path. If you are not applying the converted schema on BQ and not pushing it to the outfile then it'll print on the screen where you are running this tool.

#### ```-H, --help```

Help page. Just return all the available arguments and their description. 

## Examples:

Convert all tables in public schema and print it on screen

```bash
python3 bqconverter.py \
	-h redshift.endpoint.aws.amazon.com \
	-u awsuser \
	-p postgres \
	-P 6553 \
	-d bhuvi \
	-S redshift  \
	-a no 
```

Convert all tables from `schema1` except`tbl1,tbl2` then save the converted schema into a file.

```sh
python3 bqconverter.py \
	-h redshift.endpoint.aws.amazon.com \
	-u awsuser \
	-p postgres \
	-P 6553 \
	-d bhuvi \
	-S redshift  \
	-s schema1 \
	-b tbl1,tbl2 \
	-o /opt/bqddl.sql
```

Convert using a custom mapping file. Take a look at [here](https://github.com/searceinc/BQconvert/blob/master/setup/datatype-mapping/redshift.json)

```sh
python3 bqconverter.py \
	-h redshift.endpoint.aws.amazon.com \
	-u awsuser \
	-p postgres \
	-P 6553 \
	-d bhuvi \
	-S redshift  \
	-a no \
	-m /tmp/pg-to-bq.json
```

Convert all schema except `schemax` then apply this to BQ and create the dataset `mybq-dataset` before applying.

```sh
python3 bqconverter.py \
	-h redshift.endpoint.aws.amazon.com \
	-u awsuser \
	-p postgres \
	-P 6553 \
	-d bhuvi \
	-S redshift  \
	-b schemax \
	-r mygcp-project \
	-l US \
	-a yes \
	-c yes \
	-D mybq-dataset
```

Convert t1 and t2 tables from public schema from the SQL dump file then apply it to BigQuery.

```sh
python3 bqconverter.py \
	-i /tmp/rs_schema_dump.sql \
	-S redshift  \
	-r mygcp-project \
	-l US \
	-s public \
	-t t1,t2 \
	-a yes \
	-D mybq-dataset
```

## Limitations:

* BigQuery will not support table names and column names with whitespace. So if you have such table or column, then while applying this to BQ, it'll crash.
* Convert from a dump file needs to load the dump file into memory, so we highly recommend to take a schema level dump instead of full database dump with data.
* Only tables can be converted.

## Contributions:

* [Read how to contribute](https://github.com/searceinc/BQconvert/blob/master/CONTRIBUTING.md)
* [Its under Apache 2.0 license](https://github.com/searceinc/BQconvert/LICENSE)
