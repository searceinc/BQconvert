from google.cloud import bigquery
client = bigquery.Client()

def create_bqdataset(bq_project,bq_dataset,bq_location):
    dataset = bigquery.Dataset(bq_project+'.'+bq_dataset)
    dataset.location = bq_location
    dataset = client.create_dataset(dataset)  
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))