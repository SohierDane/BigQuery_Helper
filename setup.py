from setuptools import setup
from version import __version__ as version


setup(name='bq_helper',
      version=version,
      description='Helper class to simplify common read-only BigQuery tasks.',
      author='Sohier Dane',
      url='https://github.com/SohierDane/BigQuery_Helper',
      license='Apache 2.0',
      install_requires=['pandas', 'google-cloud-bigquery'],
      classifiers=['Programming Language :: Python :: 3'],
      )
