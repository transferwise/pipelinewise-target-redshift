import pytest
import os
import psycopg2
import psycopg2.extras
import json
import datetime

import target_redshift
from target_redshift.db_sync import DbSync

try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils


METADATA_COLUMNS = [
    '_sdc_extracted_at',
    '_sdc_batched_at',
    '_sdc_deleted_at'
]


class TestTargetRedshift(object):
    """
    UnIntegrationit Tests for PipelineWise Target Redshift
    """
    def setup_method(self):
        self.config = test_utils.get_test_config()
        redshift = DbSync(self.config)

        # Drop target schema
        if self.config['default_target_schema']:
            redshift.query("DROP SCHEMA IF EXISTS {} CASCADE".format(self.config['default_target_schema']))


    def teardown_method(self):
        pass


    def remove_metadata_columns_from_rows(self, rows):
        """Removes metadata columns from a list of rows"""
        d_rows = []
        for r in rows:
            # Copy the original row to a new dict to keep the original dict
            # and remove metadata columns
            d_row = r.copy()
            for md_c in METADATA_COLUMNS:
                d_row.pop(md_c, None)

            # Add new row without metadata columns to the new list
            d_rows.append(d_row)

        return d_rows


    def assert_metadata_columns_exist(self, rows):
        """This is a helper assertion that checks if every row in a list has metadata columns"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                assert md_c in r


    def assert_metadata_columns_not_exist(self, rows):
        """This is a helper assertion that checks metadata columns don't exist in any row"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                assert md_c not in r


    def assert_three_streams_are_loaded_in_redshift(self, should_metadata_columns_exist=False, should_hard_deleted_rows=False):
        """
        This is a helper assertion that checks if every data from the message-with-three-streams.json
        file is available in Redshift tables correctly.
        Useful to check different loading methods without duplicating assertions
        """
        redshift = DbSync(self.config)
        default_target_schema = self.config.get('default_target_schema', '')
        schema_mapping = self.config.get('schema_mapping', {})

        # Identify target schema name
        target_schema = None
        if default_target_schema is not None and default_target_schema.strip():
            target_schema = default_target_schema
        elif schema_mapping:
            target_schema = "tap_mysql_test"

        # Get loaded rows from tables
        table_one = redshift.query("SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema))
        table_two = redshift.query("SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema))
        table_three = redshift.query("SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema))


        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'c_int': 1, 'c_pk': 1, 'c_varchar': '1'}
        ]

        assert self.remove_metadata_columns_from_rows(table_one) == expected_table_one

        # ----------------------------------------------------------------------
        # Check rows in table_tow
        # ----------------------------------------------------------------------
        expected_table_two = []
        if not should_hard_deleted_rows:
            expected_table_two = [
                {'c_int': 1, 'c_pk': 1, 'c_varchar': '1', 'c_date': datetime.datetime(2019, 2, 1, 15, 12, 45)},
                {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 10, 2, 0, 0)}
            ]
        else:
            expected_table_two = [
                {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 10, 2, 0, 0)}
            ]

        assert self.remove_metadata_columns_from_rows(table_two) == expected_table_two

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = []
        if not should_hard_deleted_rows:
            expected_table_three = [
                    {'c_int': 1, 'c_pk': 1, 'c_varchar': '1', 'c_time': '04:00:00'},
                    {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_time': '07:15:00'},
                    {'c_int': 3, 'c_pk': 3, 'c_varchar': '3', 'c_time': '23:00:03'}
            ]
        else:
            expected_table_three = [
                {'c_int': 1, 'c_pk': 1, 'c_varchar': '1', 'c_time': '04:00:00'},
                {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_time': '07:15:00'}
            ]

        assert self.remove_metadata_columns_from_rows(table_three) == expected_table_three

        # ----------------------------------------------------------------------
        # Check if metadata columns exist or not
        # ----------------------------------------------------------------------
        if should_metadata_columns_exist:
            self.assert_metadata_columns_exist(table_one)
            self.assert_metadata_columns_exist(table_two)
            self.assert_metadata_columns_exist(table_three)
        else:
            self.assert_metadata_columns_not_exist(table_one)
            self.assert_metadata_columns_not_exist(table_two)
            self.assert_metadata_columns_not_exist(table_three)


    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-json.json')
        with pytest.raises(json.decoder.JSONDecodeError):
            target_redshift.persist_lines(self.config, tap_lines)


    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-message-order.json')
        with pytest.raises(Exception):
            target_redshift.persist_lines(self.config, tap_lines)


    def test_loading_tables(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning off client-side encryption and load
        self.config['client_side_encryption_master_key'] = ''
        target_redshift.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_loaded_in_redshift()


    def test_loading_tables_with_metadata_columns(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on adding metadata columns
        self.config['add_metadata_columns'] = True
        target_redshift.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_loaded_in_redshift(should_metadata_columns_exist=True)


    def test_loading_tables_with_hard_delete(self):
        """Loading multiple tables from the same input tap with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        target_redshift.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_loaded_in_redshift(
            should_metadata_columns_exist=True,
            should_hard_deleted_rows=True
        )


    def test_loading_with_multiple_schema(self):
        """Loading table with multiple SCHEMA messages"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multi-schemas.json')

        # Load with default settings
        target_redshift.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly
        self.assert_three_streams_are_loaded_in_redshift(
            should_metadata_columns_exist=False,
            should_hard_deleted_rows=False
        )


    def test_loading_unicode_characters(self):
        """Loading unicode encoded characters"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-unicode-characters.json')

        # Load with default settings
        target_redshift.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        redshift = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_unicode = redshift.query("SELECT * FROM {}.test_table_unicode ORDER BY c_pk".format(target_schema))

        assert \
            self.remove_metadata_columns_from_rows(table_unicode) == \
            [
                    {'c_int': 1, 'c_pk': 1, 'c_varchar': 'Hello world, Καλημέρα κόσμε, コンニチハ'},
                    {'c_int': 2, 'c_pk': 2, 'c_varchar': 'Chinese: 和毛泽东 <<重上井冈山>>. 严永欣, 一九八八年.'},
                    {'c_int': 3, 'c_pk': 3, 'c_varchar': 'Russian: Зарегистрируйтесь сейчас на Десятую Международную Конференцию по'},
                    {'c_int': 4, 'c_pk': 4, 'c_varchar': 'Thai: แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช'},
                    {'c_int': 5, 'c_pk': 5, 'c_varchar': 'Arabic: لقد لعبت أنت وأصدقاؤك لمدة وحصلتم علي من إجمالي النقاط'},
                    {'c_int': 6, 'c_pk': 6, 'c_varchar': 'Special Characters: [",\'!@£$%^&*()]'}
            ]


    def test_non_db_friendly_columns(self):
        """Loading non-db friendly columns like, camelcase, minus signs, etc."""
        tap_lines = test_utils.get_test_tap_lines('messages-with-non-db-friendly-columns.json')

        # Load with default settings
        target_redshift.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        redshift = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_non_db_friendly_columns = redshift.query("SELECT * FROM {}.test_table_non_db_friendly_columns ORDER BY c_pk".format(target_schema))

        assert \
            self.remove_metadata_columns_from_rows(table_non_db_friendly_columns) == \
            [
                    {'c_pk': 1, 'camelcasecolumn': 'Dummy row 1', 'minus-column': 'Dummy row 1'},
                    {'c_pk': 2, 'camelcasecolumn': 'Dummy row 2', 'minus-column': 'Dummy row 2'},
                    {'c_pk': 3, 'camelcasecolumn': 'Dummy row 3', 'minus-column': 'Dummy row 3'},
                    {'c_pk': 4, 'camelcasecolumn': 'Dummy row 4', 'minus-column': 'Dummy row 4'},
                    {'c_pk': 5, 'camelcasecolumn': 'Dummy row 5', 'minus-column': 'Dummy row 5'},
            ]


    def test_nested_schema_unflattening(self):
        """Loading nested JSON objects into VARIANT columns without flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Load with default settings - Flattening disabled
        target_redshift.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables - Transform JSON to string at query time
        redshift = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        unflattened_table = redshift.query("""
            SELECT c_pk
                  ,c_array
                  ,c_object
                  ,c_object_with_props
                  ,c_nested_object
              FROM {}.test_table_nested_schema
             ORDER BY c_pk""".format(target_schema))

        # Should be valid nested JSON strings
        assert \
            self.remove_metadata_columns_from_rows(unflattened_table) == \
            [{
                'c_pk': 1,
                'c_array': '[1, 2, 3]',
                'c_object': '{"key_1": "value_1"}',
                'c_object_with_props': '{"key_1": "value_1"}',
                'c_nested_object': '{"nested_prop_1": "nested_value_1", "nested_prop_2": "nested_value_2", "nested_prop_3": {"multi_nested_prop_1": "multi_value_1", "multi_nested_prop_2": "multi_value_2"}}'
            }]


    def test_nested_schema_flattening(self):
        """Loading nested JSON objects with flattening and not not flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Turning on data flattening
        self.config['data_flattening_max_level'] = 10

        # Load with default settings - Flattening disabled
        target_redshift.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        redshift = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        flattened_table = redshift.query("SELECT * FROM {}.test_table_nested_schema ORDER BY c_pk".format(target_schema))

        # Should be flattened columns
        assert \
            self.remove_metadata_columns_from_rows(flattened_table) == \
            [{
                'c_pk': 1,
                'c_array': '[1, 2, 3]',
                'c_object': None,   # Cannot map RECORD to SCHEMA. SCHEMA doesn't have properties that requires for flattening
                'c_object_with_props__key_1': 'value_1',
                'c_nested_object__nested_prop_1': 'nested_value_1',
                'c_nested_object__nested_prop_2': 'nested_value_2',
                'c_nested_object__nested_prop_3__multi_nested_prop_1': 'multi_value_1',
                'c_nested_object__nested_prop_3__multi_nested_prop_2': 'multi_value_2',
            }]


    def test_column_name_change(self):
        """Tests correct renaming of redshift columns after source change"""
        tap_lines_before_column_name_change = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        tap_lines_after_column_name_change = test_utils.get_test_tap_lines('messages-with-three-streams-modified-column.json')

        # Load with default settings
        target_redshift.persist_lines(self.config, tap_lines_before_column_name_change)
        target_redshift.persist_lines(self.config, tap_lines_after_column_name_change)

        # Get loaded rows from tables
        redshift = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = redshift.query("SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema))
        table_two = redshift.query("SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema))
        table_three = redshift.query("SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema))

        # Get the previous column name from information schema in test_table_two
        previous_column_name = redshift.query("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_catalog = '{}'
               AND table_schema = '{}'
               AND table_name = 'test_table_two'
             ORDER BY ordinal_position
             LIMIT 1
            """.format(
                self.config.get('dbname', '').lower(),
                target_schema.lower()))[0]["column_name"]

        # Table one should have no changes
        assert \
            self.remove_metadata_columns_from_rows(table_one) == \
            [{'c_int': 1, 'c_pk': 1, 'c_varchar': '1'}]

        # Table two should have versioned column
        assert \
            self.remove_metadata_columns_from_rows(table_two) == \
            [
                {previous_column_name: datetime.datetime(2019, 2, 1, 15, 12, 45), 'c_int': 1, 'c_pk': 1, 'c_varchar': '1', 'c_date': None},
                {previous_column_name: datetime.datetime(2019, 2, 10, 2), 'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_date': '2019-02-12 02:00:00'},
                {previous_column_name: None, 'c_int': 3, 'c_pk': 3, 'c_varchar': '2', 'c_date': '2019-02-15 02:00:00'}
            ]

        # Table three should have renamed columns
        assert \
            self.remove_metadata_columns_from_rows(table_three) == \
            [
                {'c_int': 1, 'c_pk': 1, 'c_time': '04:00:00', 'c_varchar': '1', 'c_time_renamed': None},
                {'c_int': 2, 'c_pk': 2, 'c_time': '07:15:00', 'c_varchar': '2', 'c_time_renamed': None},
                {'c_int': 3, 'c_pk': 3, 'c_time': '23:00:03', 'c_varchar': '3', 'c_time_renamed': '08:15:00'},
                {'c_int': 4, 'c_pk': 4, 'c_time': None, 'c_varchar': '4', 'c_time_renamed': '23:00:03'}
            ]

