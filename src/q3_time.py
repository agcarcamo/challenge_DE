import os
import json
from typing import List, Tuple
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, NotFound
from memory_profiler import profile


@profile
def q3_time(file_path: str) -> List[Tuple[str, int]]:
    dataset_id = "sandbox_agcarcamo"
    table_id = "de_test3"

    result = []

    try:
        # Valida si el archivo existe
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo {file_path} no se encuentra.")

        client = bigquery.Client()

        # Lee el json
        with open(file_path, "r", encoding="utf-8") as file:
            json_data = [json.loads(line) for line in file]

        # Configuracion del Job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,  # Automatically detect the schema
            max_bad_records=10,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Or WRITE_TRUNCATE, based on your needs
        )

        # Job para cargar la data
        load_job = client.load_table_from_json(
            json_data, dataset_id + '.' + table_id, job_config=job_config
        )

        load_job.result()

        print("Datos cargados en Bigquery")

        # Busca la query
        sql_file_path = os.path.join(os.path.dirname(__file__), "queries", "q3_time.sql")
        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"El archivo SQL {sql_file_path} no se encuentra.")

        with open(sql_file_path, "r", encoding="utf-8") as sql_file:
            query_template = sql_file.read()

        #  Setea dataset_id y table_id en la query
        try:
            query = query_template.format(dataset_id=dataset_id, table_id=table_id)
        except KeyError as e:
            print(f"Error formatting the query: {e}")
            return result

        query_job = client.query(query)
        results = query_job.result()

        # Armar lista de salida
        for row in results:
            username = row['username']
            mention_count = row['mention_count']
            result.append((username, mention_count))

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except GoogleAPIError as e:
        print(f"Error en BigQuery: {e}")
    except Exception as e:
        print(f"Error no esperado: {e}")

    return result