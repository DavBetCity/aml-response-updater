import os
from google.cloud import bigquery


class BQ:
    def __init__(self, local=True):
        if local:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./client_secrets.json"

        self.g_client = bigquery.Client()

    def get_table_bq(self, query):
        query_string = query

        dataframe = (
            self.g_client.query(query_string)
            .result()
            .to_dataframe(
                create_bqstorage_client=True,
            )
        )
        return dataframe

    def get_table_names_in_dataset(self, project, dataset1):
        dataset = f"{project}.{dataset1}"
        object_list = list(self.g_client.list_tables(dataset))
        table_name_list = [x.table_id for x in object_list]
        return table_name_list

    def create_replace_table(
        self, df, table_name: str, if_exists: str = "WRITE_TRUNCATE"
    ):
        job_config = bigquery.LoadJobConfig(
            write_disposition=if_exists,
        )

        self.g_client.load_table_from_dataframe(
            df, table_name, job_config=job_config
        ).result()
