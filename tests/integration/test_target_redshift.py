import pytest
import os
import json
import mock
import datetime

import target_redshift
from target_redshift.db_sync import DbSync

try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils


METADATA_COLUMNS = ["_sdc_extracted_at", "_sdc_batched_at", "_sdc_deleted_at"]


class TestTargetRedshift(object):
    """
    Integration Tests for PipelineWise Target Redshift
    """

    def setup_method(self):
        self.config = test_utils.get_test_config()
        redshift = DbSync(self.config)

        # Drop target schema
        if self.config["default_target_schema"]:
            redshift.query(
                "DROP SCHEMA IF EXISTS {} CASCADE".format(
                    self.config["default_target_schema"]
                )
            )

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

    def assert_three_streams_are_loaded_in_redshift(self,
                                                    should_metadata_columns_exist=False,
                                                    should_hard_deleted_rows=False,
                                                    should_primary_key_required=True,
                                                    should_skip_updates=False):
        """
        This is a helper assertion that checks if every data from the message-with-three-streams.json
        file is available in Redshift tables correctly.
        Useful to check different loading methods without duplicating assertions
        """
        redshift = DbSync(self.config)
        default_target_schema = self.config.get("default_target_schema", "")
        schema_mapping = self.config.get("schema_mapping", {})

        # Identify target schema name
        target_schema = None
        if default_target_schema is not None and default_target_schema.strip():
            target_schema = default_target_schema
        elif schema_mapping:
            target_schema = "tap_mysql_test"

        # Get loaded rows from tables
        table_one = redshift.query(
            "SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema)
        )
        table_two = redshift.query(
            "SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema)
        )
        table_three = redshift.query(
            "SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema)
        )

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [{"c_int": 1, "c_pk": 1, "c_varchar": "1"}]

        assert self.remove_metadata_columns_from_rows(table_one) == expected_table_one

        # ----------------------------------------------------------------------
        # Check rows in table_two
        # ----------------------------------------------------------------------
        expected_table_two = []
        if not should_hard_deleted_rows:
            expected_table_two = [
                {
                    "c_int": 1,
                    "c_pk": 1,
                    "c_varchar": "1",
                    "c_date": datetime.datetime(2019, 2, 1, 15, 12, 45),
                },
                {
                    "c_int": 2,
                    "c_pk": 2,
                    "c_varchar": "2",
                    "c_date": datetime.datetime(2019, 2, 10, 2, 0, 0),
                },
            ]
        else:
            expected_table_two = [
                {
                    "c_int": 2,
                    "c_pk": 2,
                    "c_varchar": "2",
                    "c_date": datetime.datetime(2019, 2, 10, 2, 0, 0),
                }
            ]

        assert self.remove_metadata_columns_from_rows(table_two) == expected_table_two

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = []
        if should_hard_deleted_rows:
            expected_table_three = [
                {"c_int": 1, "c_pk": 1, "c_varchar": "1", "c_time": "04:00:00"},
                {"c_int": 2, "c_pk": 2, "c_varchar": "2", "c_time": "07:15:00"},
            ]
        elif should_skip_updates:
            expected_table_three = [
                {"c_int": 1, "c_pk": 1, "c_varchar": "1", "c_time": "04:00:00"},
                {"c_int": 2, "c_pk": 2, "c_varchar": "2", "c_time": "07:15:00"},
                {"c_int": 3, "c_pk": 3, "c_varchar": "3", "c_time": "23:00:03"},
                {"c_int": 4, "c_pk": 4, "c_varchar": "4_NEW", "c_time": "18:00:10"},
            ]
        else:
            expected_table_three = [
                {"c_int": 1, "c_pk": 1, "c_varchar": "1", "c_time": "04:00:00"},
                {"c_int": 2, "c_pk": 2, "c_varchar": "2", "c_time": "07:15:00"},
                {"c_int": 3, "c_pk": 3, "c_varchar": "3", "c_time": "23:00:03"},
            ]

        assert (
            self.remove_metadata_columns_from_rows(table_three) == expected_table_three
        )

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

    def assert_logical_streams_are_in_redshift(self, should_metadata_columns_exist=False):
        # Get loaded rows from tables
        redshift = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = redshift.query("SELECT * FROM {}.logical1_table1 ORDER BY cid".format(target_schema))
        table_two = redshift.query("SELECT * FROM {}.logical1_table2 ORDER BY cid".format(target_schema))
        table_three = redshift.query("SELECT * FROM {}.logical2_table1 ORDER BY cid".format(target_schema))
        table_four = redshift.query("SELECT cid, ctimentz, ctimetz FROM {}.logical1_edgydata WHERE cid IN(1,2,3,4,5,6,8,9) ORDER BY cid".format(target_schema))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'cid': 1, 'cvarchar': "inserted row", 'cvarchar2': None},
            {'cid': 2, 'cvarchar': 'inserted row', "cvarchar2": "inserted row"},
            {'cid': 3, 'cvarchar': "inserted row", 'cvarchar2': "inserted row"},
            {'cid': 4, 'cvarchar': "inserted row", 'cvarchar2': "inserted row"}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_tow
        # ----------------------------------------------------------------------
        expected_table_two = [
            {'cid': 1, 'cvarchar': "updated row"},
            {'cid': 2, 'cvarchar': 'updated row'},
            {'cid': 3, 'cvarchar': "updated row"},
            {'cid': 5, 'cvarchar': "updated row"},
            {'cid': 7, 'cvarchar': "updated row"},
            {'cid': 8, 'cvarchar': 'updated row'},
            {'cid': 9, 'cvarchar': "updated row"},
            {'cid': 10, 'cvarchar': 'updated row'}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = [
            {'cid': 1, 'cvarchar': "updated row"},
            {'cid': 2, 'cvarchar': 'updated row'},
            {'cid': 3, 'cvarchar': "updated row"},
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_four
        # ----------------------------------------------------------------------
        expected_table_four = [
            {'cid': 1, 'ctimentz': None, 'ctimetz': None},
            {'cid': 2, 'ctimentz': '23:00:15', 'ctimetz': '23:00:15'},
            {'cid': 3, 'ctimentz': '12:00:15', 'ctimetz': '12:00:15'},
            {'cid': 4, 'ctimentz': '12:00:15', 'ctimetz': '09:00:15'},
            {'cid': 5, 'ctimentz': '12:00:15', 'ctimetz': '15:00:15'},
            {'cid': 6, 'ctimentz': '00:00:00', 'ctimetz': '00:00:00'},
            {'cid': 8, 'ctimentz': '00:00:00', 'ctimetz': '01:00:00'},
            {'cid': 9, 'ctimentz': '00:00:00', 'ctimetz': '00:00:00'}
        ]

        # Check if metadata columns replicated correctly
        if should_metadata_columns_exist:
            self.assert_metadata_columns_exist(table_one)
            self.assert_metadata_columns_exist(table_two)
            self.assert_metadata_columns_exist(table_three)
        else:
            self.assert_metadata_columns_not_exist(table_one)
            self.assert_metadata_columns_not_exist(table_two)
            self.assert_metadata_columns_not_exist(table_three)

        # Check if data replicated correctly
        assert self.remove_metadata_columns_from_rows(table_one) == expected_table_one
        assert self.remove_metadata_columns_from_rows(table_two) == expected_table_two
        assert self.remove_metadata_columns_from_rows(table_three) == expected_table_three
        assert self.remove_metadata_columns_from_rows(table_four) == expected_table_four

    def assert_logical_streams_are_in_redshift_and_are_empty(self):
        # Get loaded rows from tables
        redshift = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")
        table_one = redshift.query("SELECT * FROM {}.logical1_table1 ORDER BY cid".format(target_schema))
        table_two = redshift.query("SELECT * FROM {}.logical1_table2 ORDER BY cid".format(target_schema))
        table_three = redshift.query("SELECT * FROM {}.logical2_table1 ORDER BY cid".format(target_schema))
        table_four = redshift.query("SELECT cid, ctimentz, ctimetz FROM {}.logical1_edgydata WHERE cid IN(1,2,3,4,5,6,8,9) ORDER BY cid".format(target_schema))

        assert table_one == []
        assert table_two == []
        assert table_three == []
        assert table_four == []

    def assert_binary_data_are_in_snowflake(self, table_name, should_metadata_columns_exist=False):
        # Redshift doesn't have binary type. Binary formatted singer values loaded into VARCHAR columns
        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = snowflake.query('SELECT * FROM {}.{} ORDER BY "new"'.format(target_schema, table_name))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'new': '706b32', 'data': '6461746132', 'created_at': datetime.datetime(2019, 12, 17, 16, 2, 55)},
            {'new': '706b34', 'data': '6461746134', 'created_at': datetime.datetime(2019, 12, 17, 16, 32, 22)},
        ]

        if should_metadata_columns_exist:
            assert self.remove_metadata_columns_from_rows(table_one) == expected_table_one
        else:
            assert table_one == expected_table_one

    #################################
    #           TESTS               #
    #################################

    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("invalid-json.json")
        with pytest.raises(json.decoder.JSONDecodeError):
            target_redshift.persist_lines(self.config, tap_lines)

    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("invalid-message-order.json")
        with pytest.raises(Exception):
            target_redshift.persist_lines(self.config, tap_lines)

    def test_loading_tables(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        # Turning off client-side encryption and load
        self.config["client_side_encryption_master_key"] = ""
        target_redshift.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_loaded_in_redshift()

    def test_loading_tables_with_metadata_columns(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        # Turning on adding metadata columns
        self.config["add_metadata_columns"] = True
        target_redshift.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_loaded_in_redshift(
            should_metadata_columns_exist=True
        )

    def test_loading_tables_with_defined_parallelism(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        # Using fixed 1 thread parallelism
        self.config["parallelism"] = 1
        target_redshift.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_loaded_in_redshift()

    def test_loading_tables_with_defined_slice_number(self):
        """Loading multiple tables from the same input tap with various columns types with a defined slice number"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        self.config["slices"] = 4
        target_redshift.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_loaded_in_redshift()

    def test_loading_tables_with_gzip_compression(self):
        """Loading multiple tables from the same input tap with various columns types and gzip compression"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        self.config["compression"] = "gzip"
        target_redshift.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_loaded_in_redshift()

    def test_loading_tables_with_bz2_compression(self):
        """Loading multiple tables from the same input tap with various columns types and bz2 compression"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        self.config["compression"] = "bz2"
        target_redshift.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_loaded_in_redshift()

    def test_loading_tables_with_hard_delete(self):
        """Loading multiple tables from the same input tap with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        # Turning on hard delete mode
        self.config["hard_delete"] = True
        target_redshift.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_loaded_in_redshift(
            should_metadata_columns_exist=True, should_hard_deleted_rows=True
        )

    def test_loading_with_multiple_schema(self):
        """Loading table with multiple SCHEMA messages"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-multi-schemas.json")

        # Load with default settings
        target_redshift.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly
        self.assert_three_streams_are_loaded_in_redshift(
            should_metadata_columns_exist=False, should_hard_deleted_rows=False
        )

    def test_loading_table_with_reserved_word_as_name_and_hard_delete(self):
        """Loading a table where the name is a reserved word with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-reserved-name-as-table-name.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        target_redshift.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_binary_data_are_in_snowflake(
            table_name='"ORDER"',
            should_metadata_columns_exist=True
        )

    def test_loading_unicode_characters(self):
        """Loading unicode encoded characters"""
        tap_lines = test_utils.get_test_tap_lines(
            "messages-with-unicode-characters.json"
        )

        # Load with default settings
        target_redshift.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        redshift = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")
        table_unicode = redshift.query(
            "SELECT * FROM {}.test_table_unicode ORDER BY c_pk".format(target_schema)
        )

        assert self.remove_metadata_columns_from_rows(table_unicode) == [
            {"c_int": 1, "c_pk": 1, "c_varchar": "Hello world, Καλημέρα κόσμε, コンニチハ"},
            {"c_int": 2, "c_pk": 2, "c_varchar": "Chinese: 和毛泽东 <<重上井冈山>>. 严永欣, 一九八八年."},
            {"c_int": 3, "c_pk": 3, "c_varchar":
                "Russian: Зарегистрируйтесь сейчас на Десятую Международную Конференцию по"},
            {"c_int": 4, "c_pk": 4, "c_varchar": "Thai: แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช"},
            {"c_int": 5, "c_pk": 5, "c_varchar": "Arabic: لقد لعبت أنت وأصدقاؤك لمدة وحصلتم علي من إجمالي النقاط"},
            {"c_int": 6, "c_pk": 6, "c_varchar": "Special Characters: [\"\\,'!@£$%^&*()]\\\\"},
        ]

    def test_loading_long_text(self):
        """Loading long texts"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-long-texts.json")

        # Load with default settings
        target_redshift.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        redshift = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")
        table_long_texts = redshift.query(
            "SELECT * FROM {}.test_table_long_texts ORDER BY c_pk".format(target_schema)
        )

        # Test not very long texts by exact match
        assert self.remove_metadata_columns_from_rows(table_long_texts)[:3] == [
            {
                "c_int": 1,
                "c_pk": 1,
                "c_varchar": "Up to 128 characters: Lorem ipsum dolor sit amet, consectetuer adipiscing elit.",
            },
            {
                "c_int": 2,
                "c_pk": 2,
                "c_varchar": "Up to 256 characters: Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies.",
            },
            {
                "c_int": 3,
                "c_pk": 3,
                "c_varchar": "Up to 1024 characters: Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum.",
            },
        ]

        # Test very long texts by string length
        record_4k = table_long_texts[3]
        record_32k = table_long_texts[4]
        assert [
            {
                "c_int": int(record_4k.get("c_int")),
                "c_pk": int(record_4k.get("c_pk")),
                "len": len(record_4k.get("c_varchar")),
            },
            {
                "c_int": int(record_32k.get("c_int")),
                "c_pk": int(record_32k.get("c_pk")),
                "len": len(record_32k.get("c_varchar")),
            },
        ] == [
            {"c_int": 4, "c_pk": 4, "len": 4017},
            {"c_int": 5, "c_pk": 5, "len": 32002},
        ]

    def test_non_db_friendly_columns(self):
        """Loading non-db friendly columns like, camelcase, minus signs, etc."""
        tap_lines = test_utils.get_test_tap_lines(
            "messages-with-non-db-friendly-columns.json"
        )

        # Load with default settings
        target_redshift.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        redshift = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")
        table_non_db_friendly_columns = redshift.query(
            "SELECT * FROM {}.test_table_non_db_friendly_columns ORDER BY c_pk".format(
                target_schema
            )
        )

        assert self.remove_metadata_columns_from_rows(
            table_non_db_friendly_columns
        ) == [
            {"c_pk": 1, "camelcasecolumn": "Dummy row 1", "minus-column": "Dummy row 1"},
            {"c_pk": 2, "camelcasecolumn": "Dummy row 2", "minus-column": "Dummy row 2"},
            {"c_pk": 3, "camelcasecolumn": "Dummy row 3", "minus-column": "Dummy row 3"},
            {"c_pk": 4, "camelcasecolumn": "Dummy row 4", "minus-column": "Dummy row 4"},
            {"c_pk": 5, "camelcasecolumn": "Dummy row 5", "minus-column": "Dummy row 5"},
        ]

    def test_nested_schema_unflattening(self):
        """Loading nested JSON objects into VARIANT columns without flattening"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-nested-schema.json")

        # Load with default settings - Flattening disabled
        target_redshift.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables - Transform JSON to string at query time
        redshift = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")
        unflattened_table = redshift.query(
            """
            SELECT c_pk
                  ,c_array
                  ,c_object
                  ,c_object_with_props
                  ,c_nested_object
              FROM {}.test_table_nested_schema
             ORDER BY c_pk""".format(
                target_schema
            )
        )

        # Should be valid nested JSON strings
        assert self.remove_metadata_columns_from_rows(unflattened_table) == [
            {
                "c_pk": 1,
                "c_array": "[1, 2, 3]",
                "c_object": '{"key_1": "value_1"}',
                "c_object_with_props": '{"key_1": "value_1"}',
                "c_nested_object": '{"nested_prop_1": "nested_value_1", "nested_prop_2": "nested_value_2", "nested_prop_3": {"multi_nested_prop_1": "multi_value_1", "multi_nested_prop_2": "multi_value_2"}}',
            }
        ]

    def test_nested_schema_flattening(self):
        """Loading nested JSON objects with flattening and not not flattening"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-nested-schema.json")

        # Turning on data flattening
        self.config["data_flattening_max_level"] = 10

        # Load with default settings - Flattening disabled
        target_redshift.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        redshift = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")
        flattened_table = redshift.query(
            "SELECT * FROM {}.test_table_nested_schema ORDER BY c_pk".format(
                target_schema
            )
        )

        # Should be flattened columns
        assert self.remove_metadata_columns_from_rows(flattened_table) == [
            {
                "c_pk": 1,
                "c_array": "[1, 2, 3]",
                "c_object": None,  # Cannot map RECORD to SCHEMA. SCHEMA doesn't have properties that requires for flattening
                "c_object_with_props__key_1": "value_1",
                "c_nested_object__nested_prop_1": "nested_value_1",
                "c_nested_object__nested_prop_2": "nested_value_2",
                "c_nested_object__nested_prop_3__multi_nested_prop_1": "multi_value_1",
                "c_nested_object__nested_prop_3__multi_nested_prop_2": "multi_value_2",
            }
        ]

    def test_column_name_change(self):
        """Tests correct renaming of redshift columns after source change"""
        tap_lines_before_column_name_change = test_utils.get_test_tap_lines(
            "messages-with-three-streams.json"
        )
        tap_lines_after_column_name_change = test_utils.get_test_tap_lines(
            "messages-with-three-streams-modified-column.json"
        )

        # Load with default settings
        target_redshift.persist_lines(self.config, tap_lines_before_column_name_change)
        target_redshift.persist_lines(self.config, tap_lines_after_column_name_change)

        # Get loaded rows from tables
        redshift = DbSync(self.config)
        target_schema = self.config.get("default_target_schema", "")
        table_one = redshift.query(
            "SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema)
        )
        table_two = redshift.query(
            "SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema)
        )
        table_three = redshift.query(
            "SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema)
        )

        # Get the previous column name from information schema in test_table_two
        previous_column_name = redshift.query(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_catalog = '{}'
               AND table_schema = '{}'
               AND table_name = 'test_table_two'
             ORDER BY ordinal_position
             LIMIT 1
            """.format(
                self.config.get("dbname", "").lower(), target_schema.lower()
            )
        )[0]["column_name"]

        # Table one should have no changes
        assert self.remove_metadata_columns_from_rows(table_one) == [
            {"c_int": 1, "c_pk": 1, "c_varchar": "1"}
        ]

        # Table two should have versioned column
        assert self.remove_metadata_columns_from_rows(table_two) == [
            {
                previous_column_name: datetime.datetime(2019, 2, 1, 15, 12, 45),
                "c_int": 1,
                "c_pk": 1,
                "c_varchar": "1",
                "c_date": None,
            },
            {
                previous_column_name: datetime.datetime(2019, 2, 10, 2),
                "c_int": 2,
                "c_pk": 2,
                "c_varchar": "2",
                "c_date": "2019-02-12 02:00:00",
            },
            {
                previous_column_name: None,
                "c_int": 3,
                "c_pk": 3,
                "c_varchar": "2",
                "c_date": "2019-02-15 02:00:00",
            },
        ]

        # Table three should have renamed columns
        assert self.remove_metadata_columns_from_rows(table_three) == [
            {
                "c_int": 1,
                "c_pk": 1,
                "c_time": "04:00:00",
                "c_varchar": "1",
                "c_time_renamed": None,
            },
            {
                "c_int": 2,
                "c_pk": 2,
                "c_time": "07:15:00",
                "c_varchar": "2",
                "c_time_renamed": None,
            },
            {
                "c_int": 3,
                "c_pk": 3,
                "c_time": "23:00:03",
                "c_varchar": "3",
                "c_time_renamed": "08:15:00",
            },
            {
                "c_int": 4,
                "c_pk": 4,
                "c_time": None,
                "c_varchar": "4",
                "c_time_renamed": "23:00:03",
            },
        ]

    def test_grant_privileges(self):
        """Tests GRANT USAGE and SELECT privileges on newly created tables"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        # Create test users and groups
        redshift = DbSync(self.config)
        redshift.query("DROP USER IF EXISTS user_1")
        redshift.query("DROP USER IF EXISTS user_2")
        try:
            redshift.query("DROP GROUP group_1")  # DROP GROUP has no IF EXISTS
        except:
            pass
        try:
            redshift.query("DROP GROUP group_2")
        except:
            pass
        redshift.query("CREATE USER user_1 WITH PASSWORD 'Abcdefgh1234'")
        redshift.query("CREATE USER user_2 WITH PASSWORD 'Abcdefgh1234'")
        redshift.query("CREATE GROUP group_1 WITH USER user_1, user_2")
        redshift.query("CREATE GROUP group_2 WITH USER user_2")

        # When grantees is a string then privileges should be granted to single user
        redshift.query(
            "DROP SCHEMA IF EXISTS {} CASCADE".format(
                self.config["default_target_schema"]
            )
        )
        self.config["default_target_schema_select_permissions"] = "user_1"
        target_redshift.persist_lines(self.config, tap_lines)

        # When grantees is a list then privileges should be granted to list of user
        redshift.query(
            "DROP SCHEMA IF EXISTS {} CASCADE".format(
                self.config["default_target_schema"]
            )
        )
        self.config["default_target_schema_select_permissions"] = ["user_1", "user_2"]
        target_redshift.persist_lines(self.config, tap_lines)

        # Grant privileges to list of users
        redshift.query(
            "DROP SCHEMA IF EXISTS {} CASCADE".format(
                self.config["default_target_schema"]
            )
        )
        self.config["default_target_schema_select_permissions"] = {
            "users": ["user_1", "user_2"]
        }
        target_redshift.persist_lines(self.config, tap_lines)

        # Grant privileges to list of groups
        redshift.query(
            "DROP SCHEMA IF EXISTS {} CASCADE".format(
                self.config["default_target_schema"]
            )
        )
        self.config["default_target_schema_select_permissions"] = {
            "groups": ["group_1", "group_2"]
        }
        target_redshift.persist_lines(self.config, tap_lines)

        # Grant privileges to mix of list of users and groups
        redshift.query(
            "DROP SCHEMA IF EXISTS {} CASCADE".format(
                self.config["default_target_schema"]
            )
        )
        self.config["default_target_schema_select_permissions"] = {
            "users": ["user_1", "user_2"],
            "groups": ["group_1", "group_2"],
        }
        target_redshift.persist_lines(self.config, tap_lines)

        # Granting not existing user should raise exception
        redshift.query(
            "DROP SCHEMA IF EXISTS {} CASCADE".format(
                self.config["default_target_schema"]
            )
        )
        with pytest.raises(Exception):
            self.config["default_target_schema_select_permissions"] = {
                "users": ["user_not_exists_1", "user_not_exists_2"]
            }
            target_redshift.persist_lines(self.config, tap_lines)

        # Granting not existing group should raise exception
        redshift.query(
            "DROP SCHEMA IF EXISTS {} CASCADE".format(
                self.config["default_target_schema"]
            )
        )
        with pytest.raises(Exception):
            self.config["default_target_schema_select_permissions"] = {
                "groups": ["group_not_exists_1", "group_not_exists_2"]
            }
            target_redshift.persist_lines(self.config, tap_lines)

    def test_custom_copy_options(self):
        """Test loading data with custom copy options"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        # Loading with identical copy option should pass
        self.config[
            "copy_options"
        ] = "EMPTYASNULL TRIMBLANKS FILLRECORD TRUNCATECOLUMNS"
        target_redshift.persist_lines(self.config, tap_lines)

    def test_copy_using_aws_environment(self):
        """Test loading data with aws in the environment rather than explicitly provided access keys"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        try:
            os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get(
                "TARGET_REDSHIFT_AWS_ACCESS_KEY"
            )
            os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get(
                "TARGET_REDSHIFT_AWS_SECRET_ACCESS_KEY"
            )
            self.config["aws_access_key_id"] = None
            self.config["aws_secret_access_key"] = None

            target_redshift.persist_lines(self.config, tap_lines)
        finally:
            del os.environ["AWS_ACCESS_KEY_ID"]
            del os.environ["AWS_SECRET_ACCESS_KEY"]

    def test_copy_using_role_arn(self):
        """Test loading data with aws role arn rather than aws access keys"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        self.config["aws_redshift_copy_role_arn"] = os.environ.get(
            "TARGET_REDSHIFT_AWS_REDSHIFT_COPY_ROLE_ARN"
        )
        target_redshift.persist_lines(self.config, tap_lines)

    def test_invalid_custom_copy_options(self):
        """Tests loading data with custom copy options"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        # Loading with invalid custom copy option should raise exception
        self.config["copy_options"] = "_INVALID_COPY_OPTION_"
        with pytest.raises(Exception):
            target_redshift.persist_lines(self.config, tap_lines)

    def test_logical_streams_from_pg_with_hard_delete_and_default_batch_size_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        target_redshift.persist_lines(self.config, tap_lines)

        self.assert_logical_streams_are_in_redshift(should_metadata_columns_exist=True)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        target_redshift.persist_lines(self.config, tap_lines)

        self.assert_logical_streams_are_in_redshift(should_metadata_columns_exist=True)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_and_no_records_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams-no-records.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        target_redshift.persist_lines(self.config, tap_lines)

        self.assert_logical_streams_are_in_redshift_and_are_empty()

    @mock.patch('target_redshift.emit_state')
    def test_flush_streams_with_no_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when no intermediate flush required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size big enough to never has to flush in the middle
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 1000
        target_redshift.persist_lines(self.config, tap_lines)

        # State should be emitted only once with the latest received STATE message
        assert mock_emit_state.mock_calls == \
            [
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}})
            ]

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_redshift(should_metadata_columns_exist=True)

    @mock.patch('target_redshift.emit_state')
    def test_flush_streams_with_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when intermediate flushes required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        target_redshift.persist_lines(self.config, tap_lines)

        # State should be emitted multiple times, updating the positions only in the stream which got flushed
        assert mock_emit_state.call_args_list == \
            [
                # Flush #1 - Flushed edgydata until lsn: 108197216
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #2 - Flushed logical1-logical1_table2 until lsn: 108201336
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #3 - Flushed logical1-logical1_table2 until lsn: 108237600
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #4 - Flushed logical1-logical1_table2 until lsn: 108238768
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #5 - Flushed logical1-logical1_table2 until lsn: 108239704,
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #6 - Last flush, update every stream lsn: 108240872,
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
            ]

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_redshift(should_metadata_columns_exist=True)

    @mock.patch('target_redshift.emit_state')
    def test_flush_streams_with_intermediate_flushes_on_all_streams(self, mock_emit_state):
        """Test emitting states when intermediate flushes required and flush_all_streams is enabled"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        self.config['flush_all_streams'] = True
        target_redshift.persist_lines(self.config, tap_lines)

        # State should be emitted 6 times, flushing every stream and updating every stream position
        assert mock_emit_state.call_args_list == \
            [
                # Flush #1 - Flush every stream until lsn: 108197216
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #2 - Flush every stream until lsn 108201336
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #3 - Flush every stream until lsn: 108237600
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #4 - Flush every stream until lsn: 108238768
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #5 - Flush every stream until lsn: 108239704,
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #6 - Last flush, update every stream until lsn: 108240872,
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
            ]

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_redshift(should_metadata_columns_exist=True)

    def test_loading_tables_with_skip_updates(self):
        """Loading records with existing primary keys but skip updates"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        # Turn on skip_updates mode
        self.config["skip_updates"] = True
        target_redshift.persist_lines(self.config, tap_lines)
        self.assert_three_streams_are_loaded_in_redshift()

        # Load some new records with upserts
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams-upserts.json")
        target_redshift.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_loaded_in_redshift(should_skip_updates=True)

    def test_loading_tables_with_custom_temp_dir(self):
        """Loading multiple tables from the same input tap using custom temp directory"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Setting custom temp_dir
        self.config['temp_dir'] = ('~/.pipelinewise/tmp')
        target_redshift.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_loaded_in_redshift()
