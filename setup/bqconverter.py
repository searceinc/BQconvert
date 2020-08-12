import sys
import argparse
import json
import operator
import sqlparse
from ddlparse import DdlParse
from google.cloud import bigquery
from dbconn.redshift import redshift_conn
from func_mapping_file import get_datatype_mapping
from func_filter_tables import sql_table_filter
from func_get_tablenames import get_tablenames
from func_write_outfile import write_outfile
from func_create_bqdataset import create_bqdataset

# argparse variables
parser = argparse.ArgumentParser(add_help=False,description='BQconvertor:- Convert any database schema to BigQuery Tables. Supported Databases: AWS RedShift.''')

parser.add_argument('-h', '--host', dest='db_host', action='store', required=False, help="Database server IP/Endpoint")
parser.add_argument('-u', '--user', dest='db_user', action='store', required=False, help="Database username")
parser.add_argument('-p', '--password', dest='db_password', action='store', required=False, help="Database Password")
parser.add_argument('-P', '--port', dest='db_port', action='store', required=False, help="Database Port")
parser.add_argument('-d', '--database', dest='db_name', action='store', required=False, help="Database name")
parser.add_argument('-s', '--sh_whitelist', dest='sh_whitelist', action='store', required=False, help="List of schemas to be migrated")
parser.add_argument('-b', '--sh_blacklist', dest='sh_blacklist', action='store', required=False, help="Exclude the list schema will not be migrated")
parser.add_argument('-t', '--tbl_whitelist', dest='tbl_whitelist', action='store', required=False, help="List of tables to be migrated")
parser.add_argument('-w', '--tbl_blacklist', dest='tbl_blacklist', action='store', required=False, help="Exclude the list tables will not be migrated")
parser.add_argument('-S', '--source', dest='source', action='store', required=True, type = str.lower, choices=['redshift'], help="Source platform (eg:redshift, sqlserver)")
parser.add_argument('-r', '--bq_project', dest='bq_project', action='store', required=True,help="Bigquery project name")
parser.add_argument('-l', '--bq_location', dest='bq_location', action='store', help="Bigquery dataset location")
parser.add_argument('-D', '--bq_dataset', dest='bq_dataset', action='store', required=True, help="Bigquery dataset name")
parser.add_argument('-c', '--create', dest='bq_create', action='store',  default='no', type = str.lower, choices=['yes', 'no'], help="Create Bigquery dataset")
parser.add_argument('-x', '--drop', dest='drop', action='store', required=False, default='no', type = str.lower, choices=['yes', 'no'], help="Drop tables on Bigquery before migreating the schema")
parser.add_argument('-a', '--apply', dest='apply', action='store', required=False, default='no', type = str.lower, choices=['yes', 'no'], help="Create the tables on Bigqurey")
parser.add_argument('-m', '--mapping', dest='mapping', action='store', required=False, help="Path for your custom Datatype mapping file")
parser.add_argument('-i', '--infile', dest='infile', action='store', help='Convert the SQL dump file to BQ')
parser.add_argument('-o', '--outfile', dest='outfile', action='store', help='Save the DDL of the tables in a file')
parser.add_argument('-H', '--help', action='help', default=argparse.SUPPRESS,help='Show this help message and exit.')
args = parser.parse_args()

# Global variables
client = bigquery.Client()
source_platform = str(args.source)
bq_project = str(args.bq_project)
bq_location = str(args.bq_location)
bq_dataset = str(args.bq_dataset)
drop_flag = str(args.drop)
apply = str(args.apply)
outfile = str(args.outfile)
infile = str(args.infile)

# Check condition: apply to BQ or save --Not possible to do both
if apply == 'yes':
    if outfile != 'None':
        print('Not possible to apply to BQ and save DDL together, exit...')
        sys.exit(1)
if  str(args.db_host) != 'None':
    if infile != 'None':
        print('You can\'t convert schema from Database and Dump at the same time, choose --infile or --source')
        sys.exit(1)

if  infile == 'None':
    if str(args.db_host) == 'None':
        print('Please provide the Database server IP or Endpoint or use --infile to convert the dump file')
        sys.exit(1)
    elif str(args.db_user) == 'None':
        print('Please provide the Database User name')
        sys.exit(1)
    elif str(args.db_password) == 'None':
        print('Please provide the Database user\'s password')
        sys.exit(1)
    elif str(args.db_port) == 'None':
        print('Please provide the Database server\'s port')
        sys.exit(1)
    elif str(args.db_name) == 'None':
        print('Please provide the Database name')
        sys.exit(1)
# Get the DB connection
if str(args.db_host) != 'None':
    if source_platform == 'redshift':
        dbcursor = redshift_conn(str(args.db_host),str(args.db_user),str(args.db_password),str(args.db_name),str(args.db_port))
    else:
        print("Supported data sources: redshift, we'll add support for other databases soon \nexit...")
        exit()

def redshift_file_conversion(input):
    with open(infile) as sqlfile:
        statements = sqlparse.split(sqlfile.read())
    schema_name = []
    table_name = []

    for statement in statements:
        if 'create table' in statement.lower():
            line = sqlparse.format(statement, strip_comments=True)
            parser = DdlParse()
            parser.ddl = line
            table = parser.parse()
            schema_name.append(table.schema)
            table_name.append(table.name)
    # print(schema_name)
    # print(table_name)
    if str(args.sh_whitelist) == 'None':
        sh_whitelist = schema_name
    else:
        sh_whitelist = str(args.sh_whitelist).split(',')
    if str(args.sh_blacklist) == 'None':
        sh_blacklist = []
    else:
        sh_blacklist = str(args.sh_blacklist).split(',')
    sh_whitelist = [x for x in sh_whitelist if not x in sh_blacklist]
    # print(sh_whitelist)  

    if str(args.tbl_whitelist) == 'None':
        tbl_whitelist = table_name
    else:
        tbl_whitelist = str(args.tbl_whitelist).split(',')
    if str(args.tbl_blacklist) == 'None':
        tbl_blacklist = []
    else:
        tbl_blacklist = str(args.tbl_blacklist).split(',')
    tbl_whitelist = [x for x in tbl_whitelist if not x in tbl_blacklist]
    # print(tbl_whitelist)  

    data_mapping = get_datatype_mapping(args.mapping,args.source)
    
    for statement in statements:
        if 'create table' in statement.lower():
            line = sqlparse.format(statement, strip_comments=True)
            parser = DdlParse()
            parser.ddl = line
            table = parser.parse()
            t_name = str(table.name)
            table_id = '''{}.{}.{}'''.format(bq_project,bq_dataset,t_name)
            q_info = '''-- Table Name: {}'''.format(t_name)
            q_drop_tbl = '''DROP TABLE if exists `{}.{}.{}`;'''.format(bq_project,bq_dataset,t_name)
            columns_string = ""
            schema = []
            i=1
            if table.schema in sh_whitelist:
                if table.name in tbl_whitelist:
                    # t_name = str(table.name)
                    max_col = len(table.columns.values())
                    table_id = '''{}.{}.{}'''.format(bq_project,bq_dataset,t_name)
                    q_info = '''-- Table Name: {}'''.format(t_name)
                    q_drop_tbl = '''DROP TABLE if exists `{}.{}.{}`;'''.format(bq_project,bq_dataset,t_name)
                    for col in table.columns.values():

                        source_name = col.name.lower()
                        source_type = col.data_type.lower()
                        # print(source_name)
                        # print(source_type)
                        target_type = data_mapping[source_type]
                        if apply.lower() == 'yes':
                            if col.not_null == True:
                                is_nullable = 'REQUIRED'
                            else:
                                is_nullable = 'NULLABLE'
                            schema.append(bigquery.SchemaField(source_name,target_type,is_nullable))
                            
                        else:
                            if col.not_null == True:
                                is_nullable = 'NOT NULL'
                            else:
                                is_nullable = ''
                            column_string = '     {} {} {}'.format(source_name, target_type, is_nullable)  
                            if i < max_col:
                                column_string += ','
                                column_string += '\n'
                            columns_string += column_string
                            i += 1
                    table = bigquery.Table(table_id, schema=schema)
                    # print(columns_string)
                    # print(table)
                    if apply.lower() == 'yes':
                        if drop_flag.lower() == 'yes':
                            print(
                            "Dropping Table(if exists) : {}.{}.{} ".format(table.project, table.dataset_id, table.table_id)
                            )
                            client.query(q_drop_tbl)
                        print('processing_table'+ table.project, table.dataset_id, table.table_id)   
                        table = client.create_table(table)
                        print(
                                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
                            )
                    else:
                        q_create_body = '''CREATE TABLE `{}.{}.{}` \n(\n{}\n); '''.format(bq_project,bq_dataset,t_name, columns_string) 
                        
                        if drop_flag.lower() == 'yes':
                            sql_query = q_info + '\n' + q_drop_tbl + '\n' + q_create_body
                        else:
                            sql_query = q_info + '\n' + q_create_body
                        if outfile != 'None':
                            write_outfile(outfile,sql_query)
                        else:
                            print(sql_query)
                                    

# Get the RedShift data type and Map it with BigQuery
def redshift_conversion():
    data_mapping = get_datatype_mapping(args.mapping,args.source)
    finalized_schema_table_names= get_tablenames(dbcursor,args.sh_whitelist,args.sh_blacklist,args.tbl_whitelist,args.tbl_blacklist,source_platform)
    # Extract values from the get_table_col_list function
    table_names = finalized_schema_table_names[1]
    schema_name_in = "'"+"','".join(finalized_schema_table_names[0])+"'"
    for t_name in table_names:
        q_get_columndetails = '''SELECT column_name, data_type,is_nullable FROM INFORMATION_SCHEMA.COLUMNS WHERE (table_schema not in ('information_schema','pg_catalog') and table_schema not like('pg_temp_%')) and table_name='{}' and table_schema in ({});'''.format(t_name,schema_name_in)
        cursor = dbcursor
        cursor.execute(q_get_columndetails)
        column_details = cursor.fetchall()
        #print(column_details)
        #Save the column details for saving to a file/printing it on screen 
        columns_string = ""
        #Save the column details for executing create table via BQ API 
        schema = []
        i=1

        for mapping in column_details:
            table_id = '''{}.{}.{}'''.format(bq_project,bq_dataset,t_name)
            q_info = '''-- Table Name: {}'''.format(t_name)
            q_drop_tbl = '''DROP TABLE if exists `{}.{}.{}`;'''.format(bq_project,bq_dataset,t_name)
            
            source_name = mapping[0]
            source_type = mapping[1]
            target_type = data_mapping[source_type]
            if apply.lower() == 'yes':
                if mapping[2] == 'NO':
                    is_nullable = 'REQUIRED'
                else:
                    is_nullable = 'NULLABLE'
                schema.append(bigquery.SchemaField(source_name,target_type,is_nullable))
                
            else:
                if mapping[2] == 'NO':
                    is_nullable = 'NOT NULL'
                else:
                    is_nullable = ''
                column_string = '     {} {} {}'.format(source_name, target_type, is_nullable)  
                if i < len(column_details):
                    column_string += ','
                    column_string += '\n'
                columns_string += column_string
                i += 1
            table = bigquery.Table(table_id, schema=schema)
        if apply.lower() == 'yes':
            if drop_flag.lower() == 'yes':
                print(
                "Dropping Table(if exists) : {}.{}.{} ".format(table.project, table.dataset_id, table.table_id)
                )
                client.query(q_drop_tbl)
            print('processing_table'+ table.project, table.dataset_id, table.table_id)   
            table = client.create_table(table)
            print(
                    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
                )
        else:
            q_create_body = '''CREATE TABLE `{}.{}.{}` \n(\n{}\n); '''.format(bq_project,bq_dataset,t_name, columns_string) 
            
            if drop_flag.lower() == 'yes':
                sql_query = q_info + '\n' + q_drop_tbl + '\n' + q_create_body
            else:
                sql_query = q_info + '\n' + q_create_body
            if outfile != 'None':
                write_outfile(outfile,sql_query)
            else:
                print(sql_query)

def main():
    if str(args.bq_create) == 'yes':
        if str(args.apply) == 'yes':
            create_bqdataset(bq_project,bq_dataset,bq_location)
        else:
            print("You are not applying the converted tables to BQ, so we are skipping the create dataset")
    if infile != 'None':
        redshift_file_conversion(infile)
    elif str(args.db_host) != 'None':
        redshift_conversion()
    else:
        print('Please provide a valid source to convert')

if __name__=="__main__":
    main()
