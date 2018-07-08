## Summary

BigQuery_Helper is a helper class to simplify common read-only BigQuery tasks. It makes it easy to execute queries while you're learning SQL, and provides a convenient stepping stone on the path to using [the core BigQuery python API](https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/reference.html). You can try it for yourself by forking [this Kaggle kernel](https://www.kaggle.com/sohier/introduction-to-the-bq-helper-package/).

## Installation
You can install BigQuery_Helper with the following command in your console:


`pip install -e git+https://github.com/SohierDane/BigQuery_Helper#egg=bq_helper`

If you aren't running BigQuery_Helper on [Kaggle](http://kaggle.com/), you will also need to go through the [standard BigQuery client setup and authentication process](https://cloud.google.com/bigquery/docs/reference/libraries).

This repo has only been tested on Python 3.6+ and the v0.29+ of the bigquery API.

## Changelog
#### 0.4.0:
- `BigQueryHelper.table_schema` has been overhauled. It now returns a Pandas DataFrame and unrolls nested fields so that the results are in the format expected by queries. For example, the `github_repos.commits` nested field `author` now returns sub-fields names in the format like `author.email`.

#### 0.3.0:
- Each helper instance now logs the total bytes counted towards your quota or bill used across all queries run with that helper instance. You can access it with `BigQueryHelper.total_gb_used_net_cache`. Repeated queries are likely to hit the cache and may show up as 0 GB used.
- Queries that take longer than the maximum wait time, which defaults to 3 minutes, will be cancelled.
- Contributing to bq_helper should be easier now that there is a set of tests.

#### 0.2.0:
- `query_to_pandas` now returns an empty DataFrame when the query returns no results. Previously, this returned `None`.
