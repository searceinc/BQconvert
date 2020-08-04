from func_filter_tables import sql_table_filter

# Run the query on the Database and get the table names
def get_tablenames(dbcursor,sh_whitlist,sh_blacklist,tbl_whitelist,tbl_blacklist,source):    
    tables_filerted = sql_table_filter(sh_whitlist,sh_blacklist,tbl_whitelist,tbl_blacklist,source)
    q_get_tablename = '''SELECT table_schema,table_name from information_schema.tables WHERE (table_schema not in ('information_schema','pg_catalog') and table_schema not like('pg_temp_%')) {};'''.format(tables_filerted)
    cursor = dbcursor
    cursor.execute(q_get_tablename)
    q_tablename_result = cursor.fetchall()
    schema_names = [i[0] for i in q_tablename_result]
    table_names = [i[1] for i in q_tablename_result]
    return schema_names,table_names