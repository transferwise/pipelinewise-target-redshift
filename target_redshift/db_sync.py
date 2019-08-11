import os
import json
import psycopg2
import psycopg2.extras
import boto3
import singer
import collections
import inflection
import re
import itertools
import time
import datetime

logger = singer.get_logger()


def validate_config(config):
    errors = []
    required_config_keys = [
        'host',
        'port',
        'user',
        'password',
        'dbname',
        'aws_access_key_id',
        'aws_secret_access_key',
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


def column_type(schema_property):
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    column_type = 'character varying'
    if 'object' in property_type or 'array' in property_type:
        column_type = 'character varying'

    # Every date-time JSON value is currently mapped to TIMESTAMP WITHOUT TIME ZONE
    #
    # TODO: Detect if timezone postfix exists in the JSON and find if TIMESTAMP WITHOUT TIME ZONE or
    # TIMESTAMP WITH TIME ZONE is the better column type
    elif property_format == 'date-time':
        column_type = 'timestamp without time zone'
    elif property_format == 'time':
        column_type = 'character varying'
    elif 'number' in property_type:
        column_type = 'float'
    elif 'integer' in property_type and 'string' in property_type:
        column_type = 'character varying'
    elif 'integer' in property_type:
        column_type = 'numeric'
    elif 'boolean' in property_type:
        column_type = 'boolean'

    return column_type


def column_trans(schema_property):
    property_type = schema_property['type']
    column_trans = ''
    if 'object' in property_type or 'array' in property_type:
        column_trans = 'parse_json'

    return column_trans


def safe_table_name(name):
    return name.replace('.', '_').replace('-', '_').lower()


def safe_column_name(name):
    return '"{}"'.format(name).upper()


def column_clause(name, schema_property):
    return '{} {}'.format(safe_column_name(name), column_type(schema_property))


def flatten_key(k, parent_key, sep):
    full_key = parent_key + [k]
    inflected_key = [n for n in full_key]
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


def flatten_record(d, parent_key=[], sep='__', level=0, max_level=0):
    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.MutableMapping) and level < max_level:
            items.extend(flatten_record(v, parent_key + [k], sep=sep, level=level+1, max_level=max_level).items())
        else:
            items.append((new_key, json.dumps(v) if type(v) is list or type(v) is dict else v))
    return dict(items)


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
    def __init__(self, connection_config, stream_schema_message=None):
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
        # Validate connection
        config_errors = validate_config(connection_config)
        if len(config_errors) != 0:
            logger.error("Invalid configuration:\n   * {}".format('\n   * '.join(config_errors)))
            exit(1)

        # Set basic properties
        self.connection_config = connection_config
        self.stream_schema_message = stream_schema_message

        # Init S3 client
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=self.connection_config['aws_access_key_id'],
            aws_secret_access_key=self.connection_config['aws_secret_access_key']
        )

        # Set further properties by singer SCHEMA message
        if self.stream_schema_message is not None:
            self.stream_name = stream_schema_message['stream']
            self.stream_schema_name = stream_name_to_dict(self.stream_name)['schema_name']
            self.primary_column_names = [safe_column_name(p) for p in self.stream_schema_message['key_properties']]

            # Set table schema for redshift
            self.data_flattening_max_level = self.connection_config.get('data_flattening_max_level', 0)
            self.flatten_schema = flatten_schema(stream_schema_message['schema'], max_level=self.data_flattening_max_level)

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
            #                                                   "target_schema_select_permissions": [ "role_with_select_privs" ]
            #                                               }
            #                                           }
            config_default_target_schema = self.connection_config.get('default_target_schema', '').strip()
            config_schema_mapping = self.connection_config.get('schema_mapping', {})

            if config_schema_mapping and self.stream_schema_name in config_schema_mapping:
                self.target_schema = config_schema_mapping[self.stream_schema_name].get('target_schema')
            elif config_default_target_schema:
                self.target_schema = config_default_target_schema
            else:
                raise Exception("Target schema name not defined in config. Neither 'default_target_schema' (string) nor 'schema_mapping' (object) defines target schema for {} stream.".format(self.stream_name))

            # Set target tables
            self.target_table_without_schema = safe_table_name(stream_name_to_dict(self.stream_name)['table_name'])
            self.stage_table_without_schema = 'stg_{}'.format(self.target_table_without_schema)
            self.target_table = '{}.{}'.format(self.target_schema, self.target_table_without_schema)
            self.stage_table = '{}.{}'.format(self.target_schema, self.stage_table_without_schema)

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
            #                                                                   "target_schema_select_permissions": [ "role_with_select_privs" ]
            #                                                               }
            #                                                           }
            self.grantees = self.connection_config.get('default_target_schema_select_permissions')
            if config_schema_mapping and self.stream_schema_name in config_schema_mapping:
                self.grantees = config_schema_mapping[self.stream_schema_name].get('target_schema_select_permissions', self.grantees)


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
        logger.info("REDSHIFT - Running query: {}".format(query))
        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    query,
                    params
                )

                if cur.rowcount > 0 and cur.description:
                    return cur.fetchall()

                return []


    def record_primary_key_string(self, record):
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        flatten = flatten_record(record, max_level=self.data_flattening_max_level)
        try:
            key_props = [str(flatten[p]) for p in self.stream_schema_message['key_properties']]
        except Exception as exc:
            logger.info("Cannot find {} primary key(s) in record: {}".format(self.stream_schema_message['key_properties'], flatten))
            raise exc
        return ','.join(key_props)


    def record_to_csv_line(self, record):
        flatten = flatten_record(record, max_level=self.data_flattening_max_level)
        return ','.join(
            [
                json.dumps(flatten[name], ensure_ascii=False) if name in flatten and (flatten[name] == 0 or flatten[name]) else ''
                for name in self.flatten_schema
            ]
        )


    def put_to_s3(self, file, stream, count):
        logger.info("Uploading {} rows to S3".format(count))

        # Generating key in S3 bucket 
        bucket = self.connection_config['s3_bucket']
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')
        s3_key = "{}pipelinewise_{}_{}.csv".format(s3_key_prefix, stream, datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f"))

        logger.info("Target S3 bucket: {}, local file: {}, S3 key: {}".format(bucket, file, s3_key))

        self.s3.upload_file(file, bucket, s3_key)

        return s3_key


    def delete_from_s3(self, s3_key):
        logger.info("Deleting {} from S3".format(s3_key))
        bucket = self.connection_config['s3_bucket']
        self.s3.delete_object(Bucket=bucket, Key=s3_key)


    def load_csv(self, s3_key, count):
        logger.info("Loading {} rows into '{}'".format(count, self.stage_table))

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

                # Step 1: Create stage table if not exists
                cur.execute(self.drop_table_query(is_stage=True))
                cur.execute(self.create_table_query(is_stage=True))

                # Step 2: Load into the stage table
                copy_sql = """COPY {} ({}) FROM 's3://{}/{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    DELIMITER ',' REMOVEQUOTES ESCAPE
                    BLANKSASNULL TIMEFORMAT 'auto'
                    COMPUPDATE OFF STATUPDATE OFF
                """.format(
                    self.stage_table,
                    ', '.join([c['name'] for c in columns_with_trans]),
                    self.connection_config['s3_bucket'],
                    s3_key,
                    self.connection_config['aws_access_key_id'],
                    self.connection_config['aws_secret_access_key']
                )
                logger.info("REDSHIFT - {}".format(copy_sql))
                cur.execute(copy_sql)

                # Step 3/a: Insert or Update if primary key defined
                if len(self.stream_schema_message['key_properties']) > 0:
                    # Step 3/a/1: Insert new records
                    insert_sql = """INSERT INTO {} ({})
                        SELECT {}
                        FROM {} s LEFT JOIN {}
                        ON {}
                        WHERE {}
                    """.format(
                        self.target_table,
                        ', '.join([c['name'] for c in columns_with_trans]),
                        ', '.join(['s.{}'.format(c['name']) for c in columns_with_trans]),
                        self.stage_table,
                        self.target_table,
                        self.primary_key_merge_condition(),
                        ' AND '.join(['{}.{} IS NULL'.format(self.target_table, c) for c in self.primary_column_names])
                    )
                    logger.info("REDSHIFT - {}".format(insert_sql))
                    cur.execute(insert_sql)

                    # Step 3/a/2: Update existing records
                    update_sql = """UPDATE {}
                        SET {}
                        FROM {} s
                        WHERE {}
                    """.format(
                        self.target_table,
                        ', '.join(['{} = s.{}'.format(c['name'], c['name']) for c in columns_with_trans]),
                        self.stage_table,
                        self.primary_key_merge_condition()
                    )
                    logger.info("REDSHIFT - {}".format(update_sql))
                    cur.execute(update_sql)

                # Step 3/b: Insert only if no primary key
                else:
                    insert_sql = """INSERT INTO {} ({})
                        SELECT {}
                        FROM {} s
                    """.format(
                        self.target_table,
                        ', '.join([c['name'] for c in columns_with_trans]),
                        ', '.join(['s.{}'.format(c['name']) for c in columns_with_trans]),
                        self.stage_table
                    )
                    logger.info("REDSHIFT - {}".format(insert_sql))
                    cur.execute(insert_sql)

                # Step 4: Drop stage table
                cur.execute(self.drop_table_query(is_stage=True))


    def primary_key_merge_condition(self):
        return ' AND '.join(['{}.{} = s.{}'.format(self.target_table, c, c) for c in self.primary_column_names])


    def column_names(self):
        return [safe_column_name(name) for name in self.flatten_schema]


    def create_table_query(self, is_stage=False):
        columns = [
            column_clause(
                name,
                schema
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        primary_key = ["PRIMARY KEY ({})".format(', '.join(self.primary_column_names))] \
            if len(self.stream_schema_message['key_properties']) else []

        return 'CREATE TABLE IF NOT EXISTS {} ({})'.format(
            self.stage_table if is_stage else self.target_table,
            ', '.join(columns + primary_key)
        )


    def drop_table_query(self, is_stage=False):
        return 'DROP TABLE IF EXISTS {}'.format(self.stage_table if is_stage else self.target_table)


    def grant_usage_on_schema(self, schema_name, grantee):
        query = "GRANT USAGE ON SCHEMA {} TO ROLE {}".format(schema_name, grantee)
        logger.info("Granting USAGE privilegue on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)


    def grant_select_on_all_tables_in_schema(self, schema_name, grantee):
        query = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO ROLE {}".format(schema_name, grantee)
        logger.info("Granting SELECT ON ALL TABLES privilegue on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)


    @classmethod
    def grant_privilege(self, schema, grantees, grant_method):
        if isinstance(grantees, list):
            for grantee in grantees:
                grant_method(schema, grantee)
        elif isinstance(grantees, str):
            grant_method(schema, grantees)


    def delete_rows(self, stream):
        query = "DELETE FROM {} WHERE _sdc_deleted_at IS NOT NULL".format(self.target_table)
        logger.info("Deleting rows from '{}' table... {}".format(self.target_table, query))
        logger.info("DELETE {}".format(len(self.query(query))))


    def create_schema_if_not_exists(self, table_columns_cache=None):
        schema_rows = 0

        # table_columns_cache is an optional pre-collected list of available objects in redshift
        if table_columns_cache:
            schema_rows = list(filter(lambda x: x['table_schema'] == self.target_schema, table_columns_cache))
        # Query realtime if not pre-collected
        else:
            schema_rows = self.query(
                'SELECT LOWER(schema_name) schema_name FROM information_schema.schemata WHERE LOWER(schema_name) = %s',
                (self.target_schema.lower(),)
            )

        if len(schema_rows) == 0:
            query = "CREATE SCHEMA IF NOT EXISTS {}".format(self.target_schema)
            logger.info("Schema '{}' does not exist. Creating... {}".format(self.target_schema, query))
            self.query(query)

            self.grant_privilege(self.target_schema, self.grantees, self.grant_usage_on_schema)


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
        if table_name is not None: sql = sql + " AND LOWER(c.table_name) = '" + table_name.lower() + "'"
        if filter_schemas is not None: sql = sql + " AND LOWER(c.table_schema) IN (" + ', '.join("'{}'".format(s).lower() for s in filter_schemas) + ")"
        return self.query(sql)


    def update_columns(self, table_columns_cache=None):
        columns = []
        if table_columns_cache:
            columns = list(filter(lambda x: x['table_schema'] == self.target_schema.lower() and x['table_name'].lower() == self.target_table_without_schema, table_columns_cache))
        else:
            columns = self.get_table_columns(self.target_schema, self.target_table_without_schema)
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
            self.add_column(column)

        columns_to_replace = [
            (safe_column_name(name), column_clause(
                name,
                properties_schema
            ))
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() in columns_dict and
               columns_dict[name.lower()]['data_type'].lower() != column_type(properties_schema).lower() and

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
            self.version_column(column_name)
            self.add_column(column)


    def drop_column(self, column_name, stream):
        drop_column = "ALTER TABLE {} DROP COLUMN {}".format(self.target_table, column_name)
        logger.info('Dropping column: {}'.format(drop_column))
        self.query(drop_column)


    def version_column(self, column_name):
        version_column = "ALTER TABLE {} RENAME COLUMN {} TO \"{}_{}\"".format(self.target_table, column_name, column_name.replace("\"",""), time.strftime("%Y%m%d_%H%M"))
        logger.info('Versioning column: {}'.format(version_column))
        self.query(version_column)


    def add_column(self, column):
        add_column = "ALTER TABLE {} ADD COLUMN {}".format(self.target_table, column)
        logger.info('Adding column: {}'.format(add_column))
        self.query(add_column)


    def create_table(self, is_stage=False):
        logger.info("(Re)creating {} table...".format(self.stage_table))

        self.query(self.drop_table_query(is_stage=is_stage))
        self.query(self.create_table_query(is_stage=is_stage))


    def create_table_and_grant_privilege(self, is_stage=False):
        self.create_table(is_stage=is_stage)
        self.grant_privilege(self.target_schema, self.grantees, self.grant_select_on_all_tables_in_schema)


    def sync_table(self, table_columns_cache=None):
        found_tables = []

        if table_columns_cache:
            found_tables = list(filter(lambda x: x['table_schema'] == self.target_schema.lower() and x['table_name'].lower() == self.target_table_without_schema, table_columns_cache))
        else:
            found_tables = [table for table in (self.get_tables(self.target_schema.lower())) if table['table_name'].lower() == self.target_table_without_schema]

        # Create target table if not exists
        if len(found_tables) == 0:
            self.create_table_and_grant_privilege()
        else:
            logger.info("Table '{}' exists".format(self.target_table))
            self.update_columns(table_columns_cache)

