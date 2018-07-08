"""
Tests all public methods of the BigQueryHelper class.

Run from command line with:
python -m unittest test_helper.py


BILLING WARNING:
Running these tests requires a working BigQuery account and MAY CAUSE CHARGES.
However the dataset used for the tests is only ~2 MB, so any charges should
be very minimal. The downside is that this particular dataset is completely
refreshed every hour, so it's not possible to check for any specific return values.

For details on the test dataset, please see:
https://bigquery.cloud.google.com/table/bigquery-public-data:openaq.global_air_quality?tab=details
"""


import unittest


from bq_helper import BigQueryHelper
from google.api_core.exceptions import BadRequest
from pandas.core.frame import DataFrame
from random import random


class TestBQHelper(unittest.TestCase):
    def setUp(self):
        self.my_bq = BigQueryHelper("bigquery-public-data", "openaq")
        self.query = "SELECT location FROM `bigquery-public-data.openaq.global_air_quality`"
        # Query randomized so it won't hit the cache across multiple test runs
        self.randomizable_query = """
            SELECT value FROM `bigquery-public-data.openaq.global_air_quality`
            WHERE value = {0}"""

    def test_list_tables(self):
        self.assertEqual(self.my_bq.list_tables(), ['global_air_quality'])

    def test_list_schema(self):
        self.assertEqual(len(self.my_bq.table_schema('global_air_quality')), 11)

    def test_estimate_query_size(self):
        self.assertIsInstance(self.my_bq.estimate_query_size(self.query), float)

    def test_query_to_pandas(self):
        self.assertIsInstance(self.my_bq.query_to_pandas(self.query), DataFrame)

    def test_query_safe_passes(self):
        self.assertIsInstance(self.my_bq.query_to_pandas_safe(self.query), DataFrame)

    def test_query_safe_fails(self):
        # Different query must be used for this test to ensure we don't hit the
        # cache and end up passing by testing a query that would use zero bytes.
        fail_query = self.randomizable_query.format(random())
        self.assertIsNone(self.my_bq.query_to_pandas_safe(fail_query, 10**-10))

    def test_head(self):
        self.assertIsInstance(self.my_bq.head('global_air_quality'), DataFrame)

    def test_useage_tracker(self):
        self.my_bq.query_to_pandas(self.randomizable_query.format(random()))
        self.assertNotEqual(self.my_bq.total_gb_used_net_cache, 0)

    def test_bad_query_raises_right_error(self):
        with self.assertRaises(BadRequest):
            self.my_bq.query_to_pandas("Not a valid query")

    def test_list_nested_schema(self):
        nested_helper = BigQueryHelper("bigquery-public-data", "github_repos")
        self.assertEqual(len(nested_helper.table_schema('commits')), 33)


if __name__ == '__main__':
    unittest.main()
