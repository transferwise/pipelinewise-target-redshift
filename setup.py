#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name="pipelinewise-target-redshift",
      version="1.4.0",
      description="Singer.io target for loading data to Amazon Redshift - PipelineWise compatible",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="TransferWise",
      url='https://github.com/transferwise/pipelinewise-target-redshift',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=["target_redshift"],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'boto3==1.12.39',
          'psycopg2-binary==2.8.5',
          'inflection==0.4.0',
          'joblib==0.14.1'
      ],
      extras_require={
          "test": [
                "pylint==2.4.2",
                "pytest==5.3.0",
                "mock==3.0.5",
                "coverage==4.5.4"
            ]
      },
      entry_points="""
          [console_scripts]
          target-redshift=target_redshift:main
      """,
      packages=["target_redshift"],
      package_data = {},
      include_package_data=True,
)
