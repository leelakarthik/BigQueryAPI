from flask import Flask, jsonify,request
from google.cloud import bigquery
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'

app = Flask(__name__)

class BigQueryClientSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(BigQueryClientSingleton, cls).__new__(cls)
            cls._instance.client = bigquery.Client()
        return cls._instance

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def close_instance(cls):
        if cls._instance is not None and hasattr(cls._instance, 'client'):
            cls._instance.client.close()
            cls._instance = None

@app.route("/")
def home():
    BigQueryClientSingleton.get_instance()
    return f"Hello, welcome to bigquery api app on flask! "

@app.route("/list_datasets/<param>")
def list_datasets(param):
    try:
        project_id = param
        datasets = list(BigQueryClientSingleton.get_instance().client.list_datasets(project=project_id))
        if len(datasets) > 0:
            dataset_info_list = [
                {
                    "dataset_id": dataset.dataset_id,
                    "completepath": f'{project_id}.{dataset.dataset_id}'
                }
                for dataset in datasets
            ]

            return jsonify({
                "count": len(datasets),
                "project_id": project_id,
                "results": dataset_info_list
            })
        return jsonify({"error": f"No datasets found in the project: {project_id}"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        close_instance()

@app.route("/list_tables/<param>")
def list_tables(param):
    try:
        project_id, dataset_id = param.split(".")        
        # Construct the dataset reference
        dataset_ref = BigQueryClientSingleton.get_instance().client.dataset(dataset_id, project=project_id)
        # Get a list of tables in the dataset
        tables = list(BigQueryClientSingleton.get_instance().client.list_tables(dataset_ref))
        if len(tables) > 0:
            return jsonify({
                "count": len(tables),
                "project_id": project_id,
                "dataset_id": dataset_id,
                "results": [{"table": table.table_id,"full_table": f'{project_id}.{dataset_id}.{table.table_id}',
                             'table_schema':[{"name": field.name, "type": field.field_type} for field in BigQueryClientSingleton.get_instance().client.get_table(table.reference).schema]} 
                             for table in tables]
            })
        return jsonify({"error": f"No DataSets found in the project: {project_id} under dataset: {dataset_id}"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        close_instance()

@app.route("/<sql>")
def run_query(sql):
    # sql = unquote(sql)
    try:
        query_job = BigQueryClientSingleton.get_instance().client.query(sql)
        # Handle errors
        if query_job.errors:
            return jsonify({"error": query_job.errors}), 404
        query_result = query_job.result()
        result_list = [dict(row) for row in query_result]
        return jsonify({
            "count" : len(result_list),
            "input_query" : query_job.query,
            "results" : result_list
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500        
    finally:
        close_instance()

@app.route("/close")
def close_instance():
    BigQueryClientSingleton.close_instance()
    return f"connection closed"

@app.route("/error")
def error():
    BigQueryClientSingleton.close_instance()
    error_message = request.args.get('err', 'An error occurred.')
    if error_message == 'An error occurred.':
        return f"Unfortunately {error_message}"
    return f"Error Occured, Message: {error_message}"

if  __name__ == '__main__':
    app.run()