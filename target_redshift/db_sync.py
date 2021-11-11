import collections
import itertools
import json
import os
import re
import sys
import time

import boto3
import psycopg2
import psycopg2.extras

import inflection
from singer import get_logger


DEFAULT_VARCHAR_LENGTH = 10000
SHORT_VARCHAR_LENGTH = 256
LONG_VARCHAR_LENGTH = 65535


def validate_config(config):
    errors = []
    required_config_keys = [
        'host',
        'port',
        'user',
        'password',
        'dbname',
        's3_bucket'
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append("Required key is missing from config: [{}]".format(k))

    # Check target schema config
    config_default_target_schema = config.get('default_target_schema', None)
    config_schema_mapping = config.get('schema_mapping', None)
    if not config_default_target_schema and not config_schema_mapping:
        errors.append("Neither 'default_target_schema' (string) nor 'schema_mapping' (object) keys set in config.")

    return errors


def column_type(schema_property, with_length=True):
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    column_type = 'character varying'
    varchar_length = DEFAULT_VARCHAR_LENGTH
    if schema_property.get('maxLength', 0) > varchar_length:
        varchar_length = LONG_VARCHAR_LENGTH
    if 'object' in property_type or 'array' in property_type:
        column_type = 'character varying'
        varchar_length = LONG_VARCHAR_LENGTH

    # Every date-time JSON value is currently mapped to TIMESTAMP WITHOUT TIME ZONE
    #
    # TODO: Detect if timezone postfix exists in the JSON and find if TIMESTAMP WITHOUT TIME ZONE or
    # TIMESTAMP WITH TIME ZONE is the better column type
    elif property_format == 'date-time':
        column_type = 'timestamp without time zone'
    elif property_format == 'time':
        column_type = 'character varying'
        varchar_length = SHORT_VARCHAR_LENGTH
    elif 'number' in property_type:
        column_type = 'double precision'
    elif 'integer' in property_type and 'string' in property_type:
        column_type = 'character varying'
        varchar_length = LONG_VARCHAR_LENGTH
    elif 'integer' in property_type:
        column_type = 'bigint'
    elif 'boolean' in property_type:
        column_type = 'boolean'

    # Add max length to column type if required
    if with_length:
        if column_type == 'character varying' and varchar_length > 0:
            column_type = '{}({})'.format(column_type, varchar_length)

    return column_type


def column_trans(schema_property):
    property_type = schema_property['type']
    column_trans = ''
    if 'object' in property_type or 'array' in property_type:
        column_trans = 'parse_json'

    return column_trans


def safe_column_name(name):
    return '"{}"'.format(name).upper()


def column_clause(name, schema_property):
    return '{} {}'.format(safe_column_name(name), column_type(schema_property))


def flatten_key(k, parent_key, sep):
    full_key = parent_key + [k]
    inflected_key = full_key.copy()
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 127 and reducer_index < len(inflected_key):
        reduced_key = re.sub(r'[a-z]', '', inflection.camelize(inflected_key[reducer_index]))
        inflected_key[reducer_index] = \
            (reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]).lower()
        reducer_index += 1

    return sep.join(inflected_key)


def flatten_schema(d, parent_key=[], sep='__', level=0, max_level=0):
    items = []

    if 'properties' not in d:
        return {}

    for k, v in d['properties'].items():
        new_key = flatten_key(k, parent_key, sep)
        if 'type' in v.keys():
            if 'object' in v['type'] and 'properties' in v and level < max_level:
                items.extend(flatten_schema(v, parent_key + [k], sep=sep, level=level+1, max_level=max_level).items())
            else:
                items.append((new_key, v))
        else:
            if len(v.values()) > 0:
                if list(v.values())[0][0]['type'] == 'string':
                    list(v.values())[0][0]['type'] = ['null', 'string']
                    items.append((new_key, list(v.values())[0][0]))
                elif list(v.values())[0][0]['type'] == 'array':
                    list(v.values())[0][0]['type'] = ['null', 'array']
                    items.append((new_key, list(v.values())[0][0]))
                elif list(v.values())[0][0]['type'] == 'object':
                    list(v.values())[0][0]['type'] = ['null', 'object']
                    items.append((new_key, list(v.values())[0][0]))

    key_func = lambda item: item[0]
    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError('Duplicate column name produced in schema: {}'.format(k))

    return dict(sorted_items)


def _should_json_dump_value(key, value, flatten_schema=None):
    if isinstance(value, (dict, list)):
        return True

    if flatten_schema and key in flatten_schema and 'type' in flatten_schema[key] and set(flatten_schema[key]['type']) == {'null', 'object', 'array'}:
        return True

    return False


#pylint: disable-msg=too-many-arguments
def flatten_record(d, flatten_schema=None, parent_key=[], sep='__', level=0, max_level=0):
    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.MutableMapping) and level < max_level:
            items.extend(flatten_record(v, flatten_schema, parent_key + [k], sep=sep, level=level + 1, max_level=max_level).items())
        else:
            items.append((new_key, json.dumps(v) if _should_json_dump_value(k, v, flatten_schema) else v))
    return dict(items)


def primary_column_names(stream_schema_message):
    return [safe_column_name(p) for p in stream_schema_message['key_properties']]


def stream_name_to_dict(stream_name, separator='-'):
    catalog_name = None
    schema_name = None
    table_name = stream_name

    # Schema and table name can be derived from stream if it's in <schema_nama>-<table_name> format
    s = stream_name.split(separator)
    if len(s) == 2:
        schema_name = s[0]
        table_name = s[1]
    if len(s) > 2:
        catalog_name = s[0]
        schema_name = s[1]
        table_name = '_'.join(s[2:])

    return {
        'catalog_name': catalog_name,
        'schema_name': schema_name,
        'table_name': table_name
    }


# pylint: disable=too-many-public-methods,too-many-instance-attributes
class DbSync:
    def __init__(self, connection_config, stream_schema_message=None, table_cache=None):
        """
            connection_config:      Redshift connection details

            stream_schema_message:  An instance of the DbSync class is typically used to load
                                    data only from a certain singer tap stream.

                                    The stream_schema_message holds the destination schema
                                    name and the JSON schema that will be used to
                                    validate every RECORDS messages that comes from the stream.
                                    Schema validation happening before creating CSV and before
                                    uploading data into Redshift.

                                    If stream_schema_message is not defined then we can use
                                    the DbSync instance as a generic purpose connection to
                                    Redshift and can run individual queries. For example
                                    collecting catalog informations from Redshift for caching
                                    purposes.
        """
        self.connection_config = connection_config
        self.stream_schema_message = stream_schema_message
        self.table_cache = table_cache

        # logger to be used across the class's methods
        self.logger = get_logger('target_redshift')

        # Validate connection configuration
        config_errors = validate_config(connection_config)

        # Exit if config has errors
        if len(config_errors) != 0:
            self.logger.error("Invalid configuration:\n   * {}".format('\n   * '.join(config_errors)))
            sys.exit(1)

        aws_profile = self.connection_config.get('aws_profile') or os.environ.get('AWS_PROFILE')
        aws_access_key_id = self.connection_config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = self.connection_config.get('aws_secret_access_key') or os.environ.get('AWS_SECRET_ACCESS_KEY')
        aws_session_token = self.connection_config.get('aws_session_token') or os.environ.get('AWS_SESSION_TOKEN')

        # Init S3 client
        # Conditionally pass keys as this seems to affect whether instance credentials are correctly loaded if the keys are None
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token
            )
            credentials = aws_session.get_credentials().get_frozen_credentials()

            # Explicitly set credentials to those fetched from Boto so we can re-use them in COPY SQL if necessary
            self.connection_config['aws_access_key_id'] = credentials.access_key
            self.connection_config['aws_secret_access_key'] = credentials.secret_key
            self.connection_config['aws_session_token'] = credentials.token
        else:
            aws_session = boto3.session.Session(profile_name=aws_profile)

        self.s3 = aws_session.client('s3')
        self.skip_updates = self.connection_config.get('skip_updates', False)

        self.schema_name = None
        self.grantees = None

        # Init stream schema
        if self.stream_schema_message is not None:
            #  Define target schema name.
            #  --------------------------
            #  Target schema name can be defined in multiple ways:
            #
            #   1: 'default_target_schema' key  : Target schema is the same for every incoming stream if
            #                                     not specified explicitly for a given stream in
            #                                     the `schema_mapping` object
            #   2: 'schema_mapping' key         : Target schema defined explicitly for a given stream.
            #                                     Example config.json:
            #                                           "schema_mapping": {
            #                                               "my_tap_stream_id": {
            #                                                   "target_schema": "my_redshift_schema",
            #                                                   "target_schema_select_permissions": {
            #                                                       "users": [ "user_1", "user_2" ],
            #                                                       "groups": [ "group_1", "group_2" ]
            #                                                   }
            #                                               }
            #                                           }
            config_default_target_schema = self.connection_config.get('default_target_schema', '').strip()
            config_schema_mapping = self.connection_config.get('schema_mapping', {})

            stream_name = stream_schema_message['stream']
            stream_schema_name = stream_name_to_dict(stream_name)['schema_name']
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.schema_name = config_schema_mapping[stream_schema_name].get('target_schema')
            elif config_default_target_schema:
                self.schema_name = config_default_target_schema

            if not self.schema_name:
                raise Exception("Target schema name not defined in config. Neither 'default_target_schema' (string) nor 'schema_mapping' (object) defines target schema for {} stream.".format(stream_name))

            #  Define grantees
            #  ---------------
            #  Grantees can be defined in multiple ways:
            #
            #   1: 'default_target_schema_select_permissions' key  : USAGE and SELECT privileges will be granted on every table to a given role
            #                                                       for every incoming stream if not specified explicitly
            #                                                       in the `schema_mapping` object
            #   2: 'target_schema_select_permissions' key          : Roles to grant USAGE and SELECT privileges defined explicitly
            #                                                       for a given stream.
            #                                                       Example config.json:
            #                                                           "schema_mapping": {
            #                                                               "my_tap_stream_id": {
            #                                                                   "target_schema": "my_redshift_schema",
            #                                                                   "target_schema_select_permissions": {
            #                                                                       "users": [ "user_1", "user_2" ],
            #                                                                       "groups": [ "group_1", "group_2" ]
            #                                                                   }
            #                                                               }
            #                                                           }
            self.grantees = self.connection_config.get('default_target_schema_select_permissions')
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.grantees = config_schema_mapping[stream_schema_name].get('target_schema_select_permissions', self.grantees)

            self.data_flattening_max_level = self.connection_config.get('data_flattening_max_level', 0)
            self.flatten_schema = flatten_schema(stream_schema_message['schema'], max_level=self.data_flattening_max_level)

    def open_connection(self):
        conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(
            self.connection_config['host'],
            self.connection_config['dbname'],
            self.connection_config['user'],
            self.connection_config['password'],
            self.connection_config['port']
        )

        return psycopg2.connect(conn_string)

    def query(self, query, params=None):
        self.logger.debug("Running query: {}".format(query))
        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    query,
                    params
                )

                if cur.rowcount > 0 and cur.description:
                    return cur.fetchall()

                return []

    def table_name(self, stream_name, is_stage=False, without_schema=False):
        stream_dict = stream_name_to_dict(stream_name)
        table_name = stream_dict['table_name']
        rs_table_name = table_name.replace('.', '_').replace('-', '_').lower()

        if is_stage:
            rs_table_name = 'stg_{}'.format(rs_table_name)

        if without_schema:
            return f'"{rs_table_name.upper()}"'

        return f'{self.schema_name}."{rs_table_name.upper()}"'

    def record_primary_key_string(self, record):
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        flatten = flatten_record(record, self.flatten_schema, max_level=self.data_flattening_max_level)
        try:
            key_props = [str(flatten[p]) for p in self.stream_schema_message['key_properties']]
        except Exception as exc:
            self.logger.info("Cannot find {} primary key(s) in record: {}".format(self.stream_schema_message['key_properties'], flatten))
            raise exc
        return ','.join(key_props)

    def record_to_csv_line(self, record):
        flatten = flatten_record(record, self.flatten_schema, max_level=self.data_flattening_max_level)
        return ','.join(
            [
                json.dumps(flatten[name], ensure_ascii=False) if name in flatten and (flatten[name] == 0 or flatten[name]) else ''
                for name in self.flatten_schema
            ]
        )

    def put_to_s3(self, file, stream, count, suffix = ""):
        self.logger.info("Uploading {} rows to S3".format(count))

        # Generating key in S3 bucket
        bucket = self.connection_config['s3_bucket']
        s3_acl = self.connection_config.get('s3_acl')
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')
        s3_key = "{}pipelinewise_{}{}".format(s3_key_prefix, stream, suffix)

        self.logger.info("Target S3 bucket: {}, local file: {}, S3 key: {}".format(bucket, file, s3_key))

        extra_args = {'ACL': s3_acl} if s3_acl else None
        self.s3.upload_file(file, bucket, s3_key, ExtraArgs=extra_args)

        return s3_key

    def delete_from_s3(self, s3_key):
        self.logger.info("Deleting {} from S3".format(s3_key))
        bucket = self.connection_config['s3_bucket']
        self.s3.delete_object(Bucket=bucket, Key=s3_key)

    # pylint: disable=too-many-locals
    def load_csv(self, s3_key, count, size_bytes, compression=False):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        stage_table = self.table_name(stream, is_stage=True)
        target_table = self.table_name(stream, is_stage=False)

        self.logger.info("Loading {} rows into '{}'".format(count, self.table_name(stream, is_stage=True)))

        # Get list if columns with types
        columns_with_trans = [
            {
                "name": safe_column_name(name),
                "trans": column_trans(schema)
            }
            for (name, schema) in self.flatten_schema.items()
        ]

        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                inserts = 0
                updates = 0

                # Step 1: Create stage table if not exists
                cur.execute(self.drop_table_query(is_stage=True))
                cur.execute(self.create_table_query(is_stage=True))

                # Step 2: Generate copy credentials - prefer role if provided, otherwise use access and secret keys
                copy_credentials = """
                    iam_role '{aws_role_arn}'
                """.format(aws_role_arn=self.connection_config['aws_redshift_copy_role_arn']) if self.connection_config.get("aws_redshift_copy_role_arn") else """
                    ACCESS_KEY_ID '{aws_access_key_id}'
                    SECRET_ACCESS_KEY '{aws_secret_access_key}'
                    {aws_session_token}
                """.format(
                    aws_access_key_id=self.connection_config['aws_access_key_id'],
                    aws_secret_access_key=self.connection_config['aws_secret_access_key'],
                    aws_session_token="SESSION_TOKEN '{}'".format(self.connection_config['aws_session_token']) if self.connection_config.get('aws_session_token') else '',
                )

                # Step 3: Generate copy options - Override defaults from config.json if defined
                copy_options = self.connection_config.get('copy_options',"""
                    EMPTYASNULL BLANKSASNULL TRIMBLANKS TRUNCATECOLUMNS
                    TIMEFORMAT 'auto'
                    COMPUPDATE OFF STATUPDATE OFF
                """)

                if compression == "gzip":
                    compression_option = " GZIP"
                elif compression == "bzip2":
                    compression_option = " BZIP2"
                else:
                    compression_option = ""

                # Step 4: Load into the stage table
                copy_sql = """COPY {table} ({columns}) FROM 's3://{s3_bucket}/{s3_key}'
                    {copy_credentials}
                    {copy_options}
                    DELIMITER ',' REMOVEQUOTES ESCAPE{compression_option}
                """.format(
                    table=stage_table,
                    columns=', '.join([c['name'] for c in columns_with_trans]),
                    s3_bucket=self.connection_config['s3_bucket'],
                    s3_key=s3_key,
                    copy_credentials=copy_credentials,
                    copy_options=copy_options,
                    compression_option=compression_option
                )
                self.logger.debug("Running query: {}".format(copy_sql))
                cur.execute(copy_sql)

                # Step 5/a: Insert or Update if primary key defined
                #           Do UPDATE first and second INSERT to calculate
                #           the number of affected rows correctly
                if len(stream_schema_message['key_properties']) > 0:
                    # Step 5/a/1: Update existing records
                    if not self.skip_updates:
                        update_sql = """UPDATE {}
                            SET {}
                            FROM {} s
                            WHERE {}
                        """.format(
                            target_table,
                            ', '.join(['{} = s.{}'.format(c['name'], c['name']) for c in columns_with_trans]),
                            stage_table,
                            self.primary_key_merge_condition()
                        )
                        self.logger.debug("Running query: {}".format(update_sql))
                        cur.execute(update_sql)
                        updates = cur.rowcount

                    # Step 5/a/2: Insert new records
                    insert_sql = """INSERT INTO {} ({})
                        SELECT {}
                        FROM {} s LEFT JOIN {}
                        ON {}
                        WHERE {}
                    """.format(
                        target_table,
                        ', '.join([c['name'] for c in columns_with_trans]),
                        ', '.join(['s.{}'.format(c['name']) for c in columns_with_trans]),
                        stage_table,
                        target_table,
                        self.primary_key_merge_condition(),
                        ' AND '.join(['{}.{} IS NULL'.format(target_table, c) for c in primary_column_names(stream_schema_message)])
                    )
                    self.logger.debug("Running query: {}".format(insert_sql))
                    cur.execute(insert_sql)
                    inserts = cur.rowcount

                # Step 5/b: Insert only if no primary key
                else:
                    insert_sql = """INSERT INTO {} ({})
                        SELECT {}
                        FROM {} s
                    """.format(
                        target_table,
                        ', '.join([c['name'] for c in columns_with_trans]),
                        ', '.join(['s.{}'.format(c['name']) for c in columns_with_trans]),
                        stage_table
                    )
                    self.logger.debug("Running query: {}".format(insert_sql))
                    cur.execute(insert_sql)
                    inserts = cur.rowcount

                # Step 6: Drop stage table
                cur.execute(self.drop_table_query(is_stage=True))

                self.logger.info('Loading into {}: {}'.format(
                    self.table_name(stream, False),
                    json.dumps({'inserts': inserts, 'updates': updates, 'size_bytes': size_bytes})))

    def primary_key_merge_condition(self):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['{}.{} = s.{}'.format(
            self.table_name(stream_schema_message['stream'], False), c, c) for c in names])

    def column_names(self):
        return [safe_column_name(name) for name in self.flatten_schema]

    def create_table_query(self, is_stage=False):
        stream_schema_message = self.stream_schema_message
        columns = [
            column_clause(
                name,
                schema
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        primary_key = ["PRIMARY KEY ({})".format(', '.join(primary_column_names(stream_schema_message)))] \
            if len(stream_schema_message['key_properties']) else []

        return 'CREATE TABLE IF NOT EXISTS {} ({})'.format(
            self.table_name(stream_schema_message['stream'], is_stage),
            ', '.join(columns + primary_key)
        )

    def drop_table_query(self, is_stage=False):
        stream_schema_message = self.stream_schema_message
        return 'DROP TABLE IF EXISTS {}'.format(self.table_name(stream_schema_message['stream'], is_stage))

    def grant_usage_on_schema(self, schema_name, grantee, to_group=False):
        query = "GRANT USAGE ON SCHEMA {} TO {} {}".format(schema_name, 'GROUP' if to_group else '', grantee)
        self.logger.info("Granting USAGE privilege on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)

    def grant_select_on_all_tables_in_schema(self, schema_name, grantee, to_group=False):
        query = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {} {}".format(schema_name, 'GROUP' if to_group else '', grantee)
        self.logger.info(
            "Granting SELECT ON ALL TABLES privilegue on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)

    @classmethod
    def grant_privilege(self, schema, grantees, grant_method, to_group=False):
        if isinstance(grantees, list):
            for grantee in grantees:
                grant_method(schema, grantee,to_group)
        elif isinstance(grantees, str):
            grant_method(schema, grantees, to_group)
        elif isinstance(grantees, dict):
            users = grantees.get('users')
            groups = grantees.get('groups')

            self.grant_privilege(schema, users, grant_method)
            self.grant_privilege(schema, groups, grant_method, to_group=True)

    def delete_rows(self, stream):
        table = self.table_name(stream, is_stage=False)
        query = "DELETE FROM {} WHERE _sdc_deleted_at IS NOT NULL".format(table)
        self.logger.info("Deleting rows from '{}' table... {}".format(table, query))
        self.logger.info("DELETE {}".format(len(self.query(query))))

    def create_schema_if_not_exists(self):
        schema_name = self.schema_name
        schema_rows = 0

        # table_columns_cache is an optional pre-collected list of available objects in redshift
        if self.table_cache:
            schema_rows = list(filter(lambda x: x['table_schema'] == schema_name.lower(), self.table_cache))
        # Query realtime if not pre-collected
        else:
            schema_rows = self.query(
                'SELECT LOWER(schema_name) schema_name FROM information_schema.schemata WHERE LOWER(schema_name) = %s',
                (schema_name.lower(),)
            )

        if len(schema_rows) == 0:
            query = "CREATE SCHEMA IF NOT EXISTS {}".format(schema_name)
            self.logger.info("Schema '{}' does not exist. Creating... {}".format(schema_name, query))
            self.query(query)

            self.grant_privilege(schema_name, self.grantees, self.grant_usage_on_schema)

            # Refresh columns cache if required
            if self.table_cache:
                self.table_cache = self.get_table_columns(filter_schemas=[self.schema_name])

    def get_tables(self, table_schema=None):
        return self.query("""SELECT LOWER(table_schema) table_schema, LOWER(table_name) table_name
            FROM information_schema.tables
            WHERE LOWER(table_schema) = {}""".format(
                "LOWER(table_schema)" if table_schema is None else "'{}'".format(table_schema.lower())
        ))

    def get_table_columns(self, table_schema=None, table_name=None, filter_schemas=None):
        sql = """SELECT LOWER(c.table_schema) table_schema, LOWER(c.table_name) table_name, c.column_name, c.data_type
            FROM information_schema.columns c
            WHERE 1=1"""
        if table_schema is not None: sql = sql + " AND LOWER(c.table_schema) = '" + table_schema.lower() + "'"
        if table_name is not None: sql = sql + " AND LOWER(c.table_name) = '" + table_name.replace("\"", "").lower() + "'"
        if filter_schemas is not None: sql = sql + " AND LOWER(c.table_schema) IN (" + ', '.join("'{}'".format(s).lower() for s in filter_schemas) + ")"
        return self.query(sql)

    def update_columns(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, is_stage=False, without_schema=True)

        if self.table_cache:
            columns = list(filter(lambda x: x['table_schema'] == self.schema_name.lower() and
                                            f'"{x["table_name"].upper()}"' == table_name,
                                  self.table_cache))
        else:
            columns = self.get_table_columns(self.schema_name, table_name)

        columns_dict = {column['column_name'].lower(): column for column in columns}

        columns_to_add = [
            column_clause(
                name,
                properties_schema
            )
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() not in columns_dict
        ]

        for column in columns_to_add:
            self.add_column(column, stream)

        columns_to_replace = [
            (safe_column_name(name), column_clause(
                name,
                properties_schema
            ))
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() in columns_dict and
               columns_dict[name.lower()]['data_type'].lower() != column_type(properties_schema, with_length=False).lower() and

               # Don't alter table if 'timestamp without time zone' detected as the new required column type
               #
               # Target-redshift maps every data-time JSON types to 'timestamp without time zone' but sometimes
               # a 'timestamp with time zone' column is alrady available in the target table
               # (i.e. created by fastsync initial load)
               # We need to exclude this conversion otherwise we loose the data that is already populated
               # in the column
               #
               # TODO: Support both timestamp with/without time zone in target-redshift
               # when extracting data-time values from JSON
               # (Check the column_type function for further details)
               column_type(properties_schema).lower() != 'timestamp without time zone'
        ]

        for (column_name, column) in columns_to_replace:
            self.version_column(column_name, stream)
            self.add_column(column, stream)

        # Refresh table cache if required
        if self.table_cache and (len(columns_to_add) > 0 or len(columns_to_replace)):
            self.table_cache = self.get_table_columns(filter_schemas=[self.schema_name])

    def drop_column(self, column_name, stream):
        drop_column = "ALTER TABLE {} DROP COLUMN {}".format(self.table_name(stream, is_stage=False), column_name)
        self.logger.info('Dropping column: {}'.format(drop_column))
        self.query(drop_column)

    def version_column(self, column_name, stream):
        version_column = "ALTER TABLE {} RENAME COLUMN {} TO \"{}_{}\"".format(self.table_name(stream, is_stage=False),
                                                                               column_name,
                                                                               column_name.replace("\"", ""),
                                                                               time.strftime("%Y%m%d_%H%M"))
        self.logger.info('Versioning column: {}'.format(version_column))
        self.query(version_column)

    def add_column(self, column, stream):
        add_column = "ALTER TABLE {} ADD COLUMN {}".format(self.table_name(stream, is_stage=False), column)
        self.logger.info('Adding column: {}'.format(add_column))
        self.query(add_column)

    def create_table(self, is_stage=False):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        self.logger.info("(Re)creating {} table...".format(self.table_name(stream, is_stage)))

        self.query(self.drop_table_query(is_stage=is_stage))
        self.query(self.create_table_query(is_stage=is_stage))

    def create_table_and_grant_privilege(self, is_stage=False):
        self.create_table(is_stage=is_stage)
        self.grant_privilege(self.schema_name, self.grantees, self.grant_select_on_all_tables_in_schema)

    def sync_table(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, is_stage=False, without_schema=True)
        table_name_with_schema = self.table_name(stream, is_stage=False, without_schema=False)

        if self.table_cache:
            found_tables = list(filter(lambda x: x['table_schema'] == self.schema_name.lower() and
                                                 f'"{x["table_name"].upper()}"' == table_name,
                                       self.table_cache))
        else:
            found_tables = [table for table in (self.get_tables(self.schema_name.lower()))
                            if f'"{table["table_name"].upper()}"' == table_name]

        # Create target table if not exists
        if len(found_tables) == 0:
            self.logger.info("Table '{}' does not exist. Creating...".format(table_name_with_schema))
            self.create_table_and_grant_privilege()
        else:
            self.logger.info("Table '{}' exists".format(self.schema_name))
            self.update_columns()
