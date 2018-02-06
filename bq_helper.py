"""
Helper class to simplify common read-only BigQuery tasks.
"""

__version__ = '0.1.0'


import pandas as pd

from google.cloud import bigquery


class BigQueryHelper(object):
    """
    Helper class to simplify common BigQuery tasks like executing queries,
    showing table schemas, etc without worrying about table or dataset pointers.

    See the BigQuery docs for details of the steps this class lets you skip:
    https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/reference.html
    """

    BYTES_PER_GB = 2**30

    def __init__(self, active_project, dataset_name):
        self.project_name = active_project
        self.dataset_name = dataset_name
        self.client = bigquery.Client()
        self.__dataset_ref = self.client.dataset(self.dataset_name, project=self.project_name)
        self.dataset = None
        self.tables = dict()  # {table name (str): table object}
        self.__table_refs = dict()  # {table name (str): table reference}

    def __fetch_dataset(self):
        # Lazy loading of dataset. For example,
        # if the user only calls `self.query_to_pandas` then the
        # dataset never has to be fetched.
        if self.dataset is None:
            self.dataset = self.client.get_dataset(self.__dataset_ref)

    def __fetch_table(self, table_name):
        # Lazy loading of table
        self.__fetch_dataset()
        if table_name not in self.__table_refs:
            self.__table_refs[table_name] = self.dataset.table(table_name)
        if table_name not in self.tables:
            self.tables[table_name] = self.client.get_table(self.__table_refs[table_name])

    def table_schema(self, table_name):
        """
        Get the schema for a specific table from a dataset
        """
        self.__fetch_table(table_name)
        return(self.tables[table_name].schema)

    def list_tables(self):
        """
        List the names of the tables in a dataset
        """
        self.__fetch_dataset()
        return([x.table_id for x in self.client.list_tables(self.dataset)])

    def estimate_query_size(self, query):
        """
        Estimate gigabytes scanned by query.
        Does not consider if there is a cached query table.
        See https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.dryRun
        """
        my_job_config = bigquery.job.QueryJobConfig()
        my_job_config.dry_run = True
        my_job = self.client.query(query, job_config=my_job_config)
        return my_job.total_bytes_processed / self.BYTES_PER_GB

    def query_to_pandas(self, query):
        """
        Take a SQL query & return a pandas dataframe
        """
        query_job = self.client.query(query)
        rows = list(query_job.result(timeout=30))
        return pd.DataFrame(
            data=[list(x.values()) for x in rows], columns=list(rows[0].keys()))

    def query_to_pandas_safe(self, query, max_gb_scanned=1):
        """
        Execute a query if it's smaller than a certain number of gigabytes
        """
        query_size = self.estimate_query_size(query)
        if query_size <= max_gb_scanned:
            return self.query_to_pandas(query)
        print(f"Query cancelled; estimated size of {query_size} exceeds limit of {max_gb_scanned} GB")

    def head(self, table_name, num_rows=5, start_index=None, selected_columns=None):
        """
        Get the first n rows of a table as a DataFrame
        """
        self.__fetch_table(table_name)
        active_table = self.tables[table_name]
        schema_subset = None
        if selected_columns:
            schema_subset = [col for col in active_table.schema if col.name in selected_columns]
        results = self.client.list_rows(active_table, selected_fields=schema_subset,
            max_results=num_rows, start_index=start_index)
        results = [x for x in results]
        return pd.DataFrame(
            data=[list(x.values()) for x in results], columns=list(results[0].keys()))
