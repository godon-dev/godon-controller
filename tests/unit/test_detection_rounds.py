#
# Copyright (c) 2019 Matthias Tafelmeier.
#
# This file is part of godon
#
# godon is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
#
# godon is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this godon. If not, see <http://www.gnu.org/licenses/>.
#

import pytest
import sys
import os
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from controller.database import ArchiveDatabaseRepository, execute_query


class TestDetectionRoundsTable:
    """Test detection_rounds table creation and row insertion"""

    def setup_method(self):
        self.base_config = {
            'host': 'localhost',
            'port': '5432',
            'user': 'test_user',
            'password': 'test_pass',
            'database': 'archive_db',
        }
        self.repo = ArchiveDatabaseRepository(self.base_config)

    @patch('controller.database.execute_query')
    def test_ensure_detection_rounds_table_creates_table(self, mock_exec):
        """ensure_detection_rounds_table issues CREATE TABLE IF NOT EXISTS"""
        self.repo.ensure_detection_rounds_table()

        assert mock_exec.call_count == 1
        call_args = mock_exec.call_args

        # Verify it targets archive_db
        db_config = call_args[0][0]
        assert db_config['database'] == 'archive_db'

        # Verify SQL contains table creation
        sql = call_args[0][1]
        assert 'CREATE TABLE IF NOT EXISTS detection_rounds' in sql
        assert 'round_id' in sql
        assert 'sender_id' in sql
        assert 'status' in sql
        assert 'created_at' in sql
        assert 'completed_at' in sql

    @patch('controller.database.execute_query')
    def test_ensure_detection_rounds_table_creates_index(self, mock_exec):
        """ensure_detection_rounds_table creates active status index"""
        self.repo.ensure_detection_rounds_table()

        sql = mock_exec.call_args[0][1]
        assert 'idx_detection_rounds_active' in sql
        assert "WHERE status = 'active'" in sql

    @patch('controller.database.execute_query')
    def test_insert_detection_round_inserts_sender(self, mock_exec):
        """insert_detection_round inserts a row with the sender UUID"""
        sender_id = 'abc-123-def'
        self.repo.insert_detection_round(sender_id)

        assert mock_exec.call_count == 1
        call_args = mock_exec.call_args

        db_config = call_args[0][0]
        assert db_config['database'] == 'archive_db'

        sql = call_args[0][1]
        assert 'INSERT INTO detection_rounds' in sql
        assert 'sender_id' in sql
        assert sender_id in sql

    @patch('controller.database.execute_query')
    def test_insert_detection_round_uses_default_status(self, mock_exec):
        """Inserted rows should rely on the DEFAULT 'active' status"""
        self.repo.insert_detection_round('some-sender')

        sql = mock_exec.call_args[0][1]
        assert "VALUES ('some-sender')" in sql

    @patch('controller.database.execute_query')
    def test_ensure_table_idempotent(self, mock_exec):
        """Calling ensure twice should not error (IF NOT EXISTS)"""
        self.repo.ensure_detection_rounds_table()
        self.repo.ensure_detection_rounds_table()

        assert mock_exec.call_count == 2
        # Both calls use IF NOT EXISTS — second is a no-op
        sql = mock_exec.call_args[0][1]
        assert 'IF NOT EXISTS' in sql
