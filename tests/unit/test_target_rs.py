import pytest
import mock
import os

import target_redshift


class TestTargetRedshift(object):

    def setup_method(self):
        self.config = {}

    @mock.patch('target_redshift.NamedTemporaryFile')
    @mock.patch('target_redshift.flush_streams')
    @mock.patch('target_redshift.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(self, dbSync_mock, flush_streams_mock, temp_file_mock):
        self.config['batch_size_rows'] = 20
        self.config['flush_all_streams'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_redshift.persist_lines(self.config, lines)

        flush_streams_mock.assert_called_once()
