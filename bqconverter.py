import sys
import argparse
import json
import operator
from google.cloud import bigquery
from dbconn.redshift import redshift_conn
from func_mapping_file import get_datatype_mapping
from func_filter_tables import sql_table_filter
from func_get_tablenames import get_tablenames
from func_write_outfile import write_outfile
from func_create_bqdataset import create_bqdataset

# argparse variables
parser = argparse.ArgumentParser(add_help=False,description='BQconvertor:- Convert any database schema to BigQuery Tables. Supported Databases: AWS RedShift.''')

parser.add_argument('-h', '--host', dest='db_host', action='store', required=True, help="Database server IP/Endpoint")
parser.add_argument('-u', '--user', dest='db_user', action='store', required=True, help="Database username")
parser.add_argument('-p', '--password', dest='db_password', action='store', required=True, help="Database Password")
parser.add_argument('-P', '--port', dest='db_port', action='store', required=True, help="Database Port")
parser.add_argument('-d', '--database', dest='db_name', action='store', required=True, help="Database name")
parser.add_argument('-s', '--sh_whitlist', dest='sh_whitlist', action='store', required=False, help="List of schemas to be migrated")
parser.add_argument('-b', '--sh_blocklist', dest='sh_blocklist', action='store', required=False, help="Exclude the list schema will not be migrated")
parser.add_argument('-t', '--tbl_whitelist', dest='tbl_whitelist', action='store', required=False, help="List of tables to be migrated")
parser.add_argument('-w', '--tbl_blocklist', dest='tbl_blocklist', action='store', required=False, help="Exclude the list tables will not be migrated")
parser.add_argument('-S', '--source', dest='source', action='store', required=True, type = str.lower, choices=['redshift'], help="Source platform (eg:redshift, sqlserver)")
parser.add_argument('-r', '--bq_project', dest='bq_project', action='store', required=True, help="Bigquery project name")
parser.add_argument('-l', '--bq_location', dest='bq_location', action='store', required=True, help="Bigquery dataset location")
parser.add_argument('-D', '--bq_dataset', dest='bq_dataset', action='store', required=True, help="Bigquery dataset name")
parser.add_argument('-c', '--create', dest='bq_create', action='store',  default='no', type = str.lower, choices=['yes', 'no'], help="Create Bigquery dataset")
parser.add_argument('-x', '--drop', dest='drop', action='store', required=False, default='no', type = str.lower, choices=['yes', 'no'], help="Drop tables on Bigquery before migreating the schema")
parser.add_argument('-a', '--apply', dest='apply', action='store', required=False, default='no', type = str.lower, choices=['yes', 'no'], help="Create the tables on Bigqurey")
parser.add_argument('-m', '--mapping', dest='mapping', action='store', required=False, help="Path for your custom Datatype mapping file")
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

# Check condition: apply to BQ or save --Not possible to do both
if apply == 'yes':
    if outfile != 'None':
        print('Not possible to apply to BQ and save DDL together, exit...')
        sys.exit(1)
# Get the DB connection
if source_platform == 'redshift':
    dbcursor = redshift_conn(str(args.db_host),str(args.db_user),str(args.db_password),str(args.db_name),str(args.db_port))
else:
    print("Supported data sources: redshift, we'll add support for other databases soon \nexit...")
    exit()



# Get the RedShift data type and Map it with BigQuery
def redshift_conversion():
    data_mapping = get_datatype_mapping(args.mapping,args.source)
    finalized_schema_table_names= get_tablenames(dbcursor,args.sh_whitlist,args.sh_blocklist,args.tbl_whitelist,args.tbl_blocklist,source_platform)
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
    redshift_conversion()

if __name__=="__main__":
    main()
