import sys
def write_outfile(path,query):
    try:
        with open(path,"a+") as ddl_file:
            ddl_file.write(str(query))
        ddl_file.close()
    except Exception as error:
        print(error)
        sys.exit(1)