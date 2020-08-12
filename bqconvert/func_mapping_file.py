import sys
import json

# Getting the datatype mapping file
def get_datatype_mapping(map_file,source):
    if str(map_file) == 'None':
        mapping_file = './datatype-mapping/'+str(source)+'.json'
    else:
        mapping_file =str(map_file)
    try:
        with open(mapping_file) as mapping:
            data_mapping = json.load(mapping)
            #print(data_mapping)
    except Exception as error:
    	print(error)
    	sys.exit(1)
    return data_mapping    