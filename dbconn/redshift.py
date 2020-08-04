import psycopg2
import sys

def redshift_conn(host,user,password,database,port):
    cursor = ""
    
    try:
    	connection = psycopg2.connect(user = user,
                                    password = password,
                                    host = host,
                                    port = port,
                                    database =database)
    except Exception as error:
    	print(error)
    	sys.exit(1)
    
    cursor = connection.cursor()
    return cursor
