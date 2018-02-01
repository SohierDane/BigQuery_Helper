from setuptools import setup


setup(name='bq_helper',
      version='0.1.0',
      description='Helper class to simplify common read-only BigQuery tasks.',
      author='Sohier Dane',
      url='https://github.com/SohierDane/BigQuery_Helper',
      license='Apache 2.0',
      install_requires=['pandas', 'google-cloud-bigquery'],
      classifiers=['Programming Language :: Python :: 3'],
      )
