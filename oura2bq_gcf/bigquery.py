from google.cloud import bigquery
from google.cloud import secretmanager


def fetch_secret(project_number, secret_id):
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=f'projects/{project_number}/secrets/{secret_id}/versions/latest')
    return response.payload.data


def fetch_latest_date(table_name, column_date='summary_date'):
    client = bigquery.Client()

    query = f"""
        SELECT 
            {column_date}
        FROM 
            `digitaltwin-49d59.oura.{table_name}` 
        ORDER BY {column_date} DESC
        LIMIT 1
        """

    query_job = client.query(query).result()

    return list(query_job)[0][column_date]


def insert(json_list, table_name):
    bq_client = bigquery.Client()
    table_id = f"digitaltwin-49d59.oura.{table_name}"

    errors = bq_client.insert_rows_json(table_id, json_list)
    if errors != []:
        print("Encountered errors while inserting rows: {}".format(errors))