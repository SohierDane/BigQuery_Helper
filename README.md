### Summary

BigQuery_Helper provides a helper class to simplify common read-only BigQuery tasks.

You can try it for yourself by forking [this Kaggle kernel](https://www.kaggle.com/sohier/introduction-to-the-bq-helper-package/).

### Installation
You can install BigQuery_Helper with the following pip command in your console:
`pip install -e git+https://github.com/SohierDane/BigQuery_Helper#egg=bq_helper`.

### Changelog

- 0.2.0: `query_to_pandas` now returns an empty DataFrame when the query returns no results. Previously, this returned `None`.
