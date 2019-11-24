# pipelinewise-target-redshift

[![PyPI version](https://badge.fury.io/py/pipelinewise-target-redshift.svg)](https://badge.fury.io/py/pipelinewise-target-redshift)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-target-redshift.svg)](https://pypi.org/project/pipelinewise-target-redshift/)
[![License: Apache2](https://img.shields.io/badge/License-Apache2-yellow.svg)](https://opensource.org/licenses/Apache-2.0)

[Singer](https://www.singer.io/) target that loads data into Amazon Redshift following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible target connector.

## How to use it

The recommended method of running this target is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Target Redshift](https://transferwise.github.io/pipelinewise/connectors/targets/redshift.html)

If you want to run this [Singer Target](https://singer.io) independently please read further.

## Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install pipelinewise-target-redshift
```

or

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
```

### To run

Like any other target that's following the singer specificiation:

`some-singer-tap | target-redshift --config [config.json]`

It's reading incoming messages from STDIN and using the properites in `config.json` to upload data into Amazon Redshift.

**Note**: To avoid version conflicts run `tap` and `targets` in separate virtual environments.

### Configuration settings

Running the the target connector requires a `config.json` file. Example with the minimal settings:

   ```json
   {

     "host": "xxxxxx.redshift.amazonaws.com",
     "port": 5439,
     "user": "my_user",
     "password": "password",
     "dbname": "database_name",
     "aws_access_key_id": "secret",
     "aws_secret_access_key": "secret",
     "s3_bucket": "bucket_name",
     "default_target_schema": "my_target_schema"
   }
   ```

Full list of options in `config.json`:

| Property                            | Type    | Required?  | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| host                                | String  | Yes        | Redshift Host                                                 |
| port                                | Integer | Yes        | Redshift Port                                                 |
| user                                | String  | Yes        | Redshift User                                                 |
| password                            | String  | Yes        | Redshift Password                                             |
| dbname                              | String  | Yes        | Redshift Database name                                        |
| aws_access_key_id                   | String  | No         | S3 Access Key Id. Used for S3 and Redshfit copy operations. If not provided, credentials will be collected from the environment                                              |
| aws_secret_access_key               | String  | No         | S3 Secret Access Key. Used for S3 and Redshfit copy operations. If not provided, credentials will be collected from the environment                                          |
| aws_session_token                   | String  | No         | S3 AWS STS token for temporary credentials                |
| aws_redshift_copy_role_arn          | String  | No         | AWS Role ARN to be used for the Redshift COPY operation. Used instead of the given AWS keys for the COPY operation if provided - the keys are still used for other S3 operations |
| s3_bucket                           | String  | Yes        | S3 Bucket name                                                |
| s3_key_prefix                       | String  |            | (Default: None) A static prefix before the generated S3 key names. Using prefixes you can upload files into specific directories in the S3 bucket. |
| copy_options                        | String  |            | (Default: `EMPTYASNULL BLANKSASNULL TRIMBLANKS TRUNCATECOLUMNS TIMEFORMAT 'auto' COMPUPDATE OFF STATUPDATE OFF`). Parameters to use in the COPY command when loading data to Redshift. Some basic file formatting parameters are fixed values and not recommended overriding them by custom values. They are like: `CSV GZIP DELIMITER ',' REMOVEQUOTES ESCAPE` |
| batch_size_rows                      | Integer |            | (Default: 100000) Maximum number of rows in each batch. At the end of each batch, the rows in the batch are loaded into Redshift. |
| flush_all_streams                   | Boolean |            | (Default: False) Flush and load every stream into Redshift when one batch is full. Warning: This may trigger the COPY command to use files with low number of records, and may cause performance problems. |
| parallelism                         | Integer |            | (Default: 0) The number of threads used to flush tables. 0 will create a thread for each stream, up to parallelism_max. -1 will create a thread for each CPU core. Any other positive number will create that number of threads, up to parallelism_max. |
| max_parallelism                     | Integer |            | (Default: 16) Max number of parallel threads to use when flushing tables. |
| default_target_schema               | String  |            | Name of the schema where the tables will be created. If `schema_mapping` is not defined then every stream sent by the tap is loaded into this schema.    |
| default_target_schema_select_permissions | String  |            | Grant USAGE privilege on newly created schemas and grant SELECT privilege on newly created tables to a specific list of users or groups. Example: `{"users": ["user_1","user_2"], "groups": ["group_1", "group_2"]}` If `schema_mapping` is not defined then every stream sent by the tap is granted accordingly.   |
| schema_mapping                      | Object  |            | Useful if you want to load multiple streams from one tap to multiple Redshift schemas.<br><br>If the tap sends the `stream_id` in `<schema_name>-<table_name>` format then this option overwrites the `default_target_schema` value. Note, that using `schema_mapping` you can overwrite the `default_target_schema_select_permissions` value to grant SELECT permissions to different groups per schemas or optionally you can create indices automatically for the replicated tables.<br><br> **Note**: This is an experimental feature and recommended to use via PipelineWise YAML files that will generate the object mapping in the right JSON format. For further info check a [PipelineWise YAML Example]
| disable_table_cache                 | Boolean |            | (Default: False) By default the connector caches the available table structures in Redshift at startup. In this way it doesn't need to run additional queries when ingesting data to check if altering the target tables is required. With `disable_table_cache` option you can turn off this caching. You will always see the most recent table structures but will cause an extra query runtime. |
| add_metadata_columns                | Boolean |            | (Default: False) Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in redshift etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. The metadata columns are documented at https://transferwise.github.io/pipelinewise/data_structure/sdc-columns.html. Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recongisable in Redshift. |
| hard_delete                         | Boolean |            | (Default: False) When `hard_delete` option is true then DELETE SQL commands will be performed in Redshift to delete rows in tables. It's achieved by continuously checking the  `_SDC_DELETED_AT` metadata column sent by the singer tap. Due to deleting rows requires metadata columns, `hard_delete` option automatically enables the `add_metadata_columns` option as well. |
| data_flattening_max_level           | Integer |            | (Default: 0) Object type RECORD items from taps can be loaded into VARIANT columns as JSON (default) or we can flatten the schema by creating columns automatically.<br><br>When value is 0 (default) then flattening functionality is turned off. |
| primary_key_required                | Boolean |            | (Default: True) Log based and Incremental replications on tables with no Primary Key cause duplicates when merging UPDATE events. When set to true, stop loading data if no Primary Key is defined. |

### To run tests:

1. Install python dependencies in a virtual env:


```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
  pip install pytest coverage
```

1. To run unit tests:


```bash
  coverage run -m pytest --disable-pytest-warnings tests/unit && coverage report
```

1. To run integration tests define environment variables first:

```bash
  export TARGET_REDSHIFT_HOST=<redshift-host>
  export TARGET_REDSHIFT_PORT=<redshift-port>
  export TARGET_REDSHIFT_USER=<redshift-user>
  export TARGET_REDSHIFT_PASSWORD=<redshift-password>
  export TARGET_REDSHIFT_DBNAME=<redshift-database-name>
  export TARGET_REDSHIFT_SCHEMA=<redshift-target-schema>
  export TARGET_REDSHIFT_AWS_ACCESS_KEY=<aws-access-key-id>
  export TARGET_REDSHIFT_AWS_SECRET_ACCESS_KEY=<aws-access-secret-access-key>
  export TARGET_REDSHIFT_S3_BUCKET=<s3-bucket>
  export TARGET_REDSHIFT_S3_KEY_PREFIX=<s3-bucket-directory>

  coverage run -m pytest --disable-pytest-warnings tests/integration && coverage report
```

### To run pylint:

1. Install python dependencies and run python linter


```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
  pip install pylint
  pylint target_redshift -d C,W,unexpected-keyword-arg,duplicate-code
```

## License

Apache License Version 2.0

See [LICENSE](LICENSE) to see the full text.
