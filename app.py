from google.cloud import bigquery

client = bigquery.Client()

# Perform a query.
QUERY = (
    'SELECT * FROM `bigquery-public-data.usa_names.us_1910_2013` '
    'WHERE state = "TX" '
    'LIMIT 100')
query_job = client.query(QUERY)  # API request
# rows = query_job.result() # Waits for query to finish
print(dir(query_job))