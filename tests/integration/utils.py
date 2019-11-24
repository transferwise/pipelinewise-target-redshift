import os
import json


def get_db_config():
    config = {}

    # --------------------------------------------------------------------------
    # Default configuration settings for integration tests.
    # --------------------------------------------------------------------------
    # The following values needs to be defined in environment variables with
    # valid details to a Redshift cluster, AWS IAM role and an S3 bucket
    # --------------------------------------------------------------------------
    # Redshift cluster
    config['host'] = os.environ.get('TARGET_REDSHIFT_HOST')
    config['port'] = os.environ.get('TARGET_REDSHIFT_PORT')
    config['user'] = os.environ.get('TARGET_REDSHIFT_USER')
    config['password'] = os.environ.get('TARGET_REDSHIFT_PASSWORD')
    config['dbname'] = os.environ.get('TARGET_REDSHIFT_DBNAME')
    config['default_target_schema'] = os.environ.get("TARGET_REDSHIFT_SCHEMA")

    # AWS IAM and S3 bucket
    config['aws_access_key_id'] = os.environ.get('TARGET_REDSHIFT_AWS_ACCESS_KEY')
    config['aws_secret_access_key'] = os.environ.get('TARGET_REDSHIFT_AWS_SECRET_ACCESS_KEY')
    config['s3_bucket'] = os.environ.get('TARGET_REDSHIFT_S3_BUCKET')
    config['s3_key_prefix'] = os.environ.get('TARGET_REDSHIFT_S3_KEY_PREFIX')


    # --------------------------------------------------------------------------
    # The following variables needs to be empty.
    # The tests cases will set them automatically whenever it's needed
    # --------------------------------------------------------------------------
    config['disable_table_cache'] = None
    config['schema_mapping'] = None
    config['add_metadata_columns'] = None
    config['hard_delete'] = None
    config['aws_redshift_copy_role_arn'] = None
    config['flush_all_streams'] = None


    return config


def get_test_config():
    db_config = get_db_config()

    return db_config


def get_test_tap_lines(filename):
    lines = []
    with open('{}/resources/{}'.format(os.path.dirname(__file__), filename)) as tap_stdout:
        for line in tap_stdout.readlines():
            lines.append(line)

    return lines

