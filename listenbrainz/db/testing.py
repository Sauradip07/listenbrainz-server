
import os
import unittest

import sqlalchemy
import uuid

from sqlalchemy import text

from listenbrainz import config
from listenbrainz import db
from listenbrainz.db import timescale as ts
import listenbrainz.db.user as db_user

ADMIN_SQL_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'admin', 'sql')
TEST_DATA_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'testdata')
TIMESCALE_SQL_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'admin', 'timescale')


class DatabaseTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db.init_db_connection(config.SQLALCHEMY_DATABASE_URI)

    def setUp(self) -> None:
        self.conn = db.engine.connect()
        self.trans = self.conn.begin()

    def tearDown(self):
        self.trans.rollback()
        self.conn.close()

    def path_to_data_file(self, file_name):
        """ Returns the path of the test data file relative to listenbrainz/db/testing.py.

            Args:
                file_name: the name of the data file
        """
        return os.path.join(TEST_DATA_PATH, file_name)

    def create_user_with_id(self, lb_id: int, musicbrainz_row_id: int, musicbrainz_id: str):
        """ Create a new user with the specified LB id. """
        user = db_user.get(self.conn, lb_id)
        if user is None:
            self.conn.execute(sqlalchemy.text("""
                INSERT INTO "user" (id, musicbrainz_id, musicbrainz_row_id, auth_token)
                     VALUES (:id, :mb_id, :mb_row_id, :token)
            """), {
                "id": lb_id,
                "mb_id": musicbrainz_id,
                "token": str(uuid.uuid4()),
                "mb_row_id": musicbrainz_row_id,
            })
            user = db_user.get(self.conn, lb_id)
        return user


class ResetDatabaseTestCase(unittest.TestCase):

    def setUp(self):
        db.init_db_connection(config.SQLALCHEMY_DATABASE_URI)
        self.conn = db.engine.connect()

    def tearDown(self):
        self.reset_db()

    def reset_db(self):
        self.conn.close()
        self.drop_tables()
        self.init_db()
        self.conn = db.engine.connect()

    def init_db(self):
        db.run_sql_script(os.path.join(ADMIN_SQL_DIR, 'create_schema.sql'))
        db.run_sql_script(os.path.join(ADMIN_SQL_DIR, 'create_tables.sql'))
        db.run_sql_script(os.path.join(ADMIN_SQL_DIR, 'create_primary_keys.sql'))
        db.run_sql_script(os.path.join(ADMIN_SQL_DIR, 'create_foreign_keys.sql'))
        db.run_sql_script(os.path.join(ADMIN_SQL_DIR, 'create_indexes.sql'))

    def drop_tables(self):
        db.run_sql_script(os.path.join(ADMIN_SQL_DIR, 'drop_schema.sql'))
        db.run_sql_script(os.path.join(ADMIN_SQL_DIR, 'drop_tables.sql'))

    def create_user_with_id(self, lb_id: int, musicbrainz_row_id: int, musicbrainz_id: str):
        """ Create a new user with the specified LB id. """
        with db.engine.connect() as connection:
            connection.execute(sqlalchemy.text("""
                INSERT INTO "user" (id, musicbrainz_id, musicbrainz_row_id, auth_token)
                     VALUES (:id, :mb_id, :mb_row_id, :token)
            """), {
                "id": lb_id,
                "mb_id": musicbrainz_id,
                "token": str(uuid.uuid4()),
                "mb_row_id": musicbrainz_row_id,
            })
            return db_user.get(connection, lb_id)

    def path_to_data_file(self, file_name):
        """ Returns the path of the test data file relative to listenbrainz/db/testing.py.

            Args:
                file_name: the name of the data file
        """
        return os.path.join(TEST_DATA_PATH, file_name)


class TimescaleTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        ts.init_db_connection(config.SQLALCHEMY_TIMESCALE_URI)

    def setUp(self):
        self.ts_conn = ts.engine.connect()
        self.ts_trans = self.ts_conn.begin()

    def tearDown(self):
        self.ts_trans.rollback()
        self.ts_conn.close()


class ResetTimescaleTestCase(unittest.TestCase):

    def setUp(self):
        ts.init_db_connection(config.SQLALCHEMY_TIMESCALE_URI)
        self.ts_conn = ts.engine.connect()

    def tearDown(self):
        self.reset_ts_db(reconnect=False)

    def reset_ts_db(self, reconnect=True):
        self.ts_conn.close()

        ts.init_db_connection(config.TIMESCALE_ADMIN_URI)
        ts.run_sql_script_without_transaction(os.path.join(TIMESCALE_SQL_DIR, 'drop_db.sql'))
        ts.run_sql_script_without_transaction(os.path.join(TIMESCALE_SQL_DIR, 'create_db.sql'))
        ts.engine.dispose()

        ts.init_db_connection(config.TIMESCALE_ADMIN_LB_URI)
        ts.run_sql_script_without_transaction(os.path.join(TIMESCALE_SQL_DIR, 'create_extensions.sql'))
        ts.engine.dispose()

        ts.init_db_connection(config.SQLALCHEMY_TIMESCALE_URI)
        ts.run_sql_script(os.path.join(TIMESCALE_SQL_DIR, 'create_schemas.sql'))
        ts.run_sql_script(os.path.join(TIMESCALE_SQL_DIR, 'create_types.sql'))
        ts.run_sql_script(os.path.join(TIMESCALE_SQL_DIR, 'create_tables.sql'))
        ts.run_sql_script(os.path.join(TIMESCALE_SQL_DIR, 'create_functions.sql'))
        ts.run_sql_script(os.path.join(TIMESCALE_SQL_DIR, 'create_indexes.sql'))
        ts.run_sql_script(os.path.join(TIMESCALE_SQL_DIR, 'create_primary_keys.sql'))
        ts.run_sql_script(os.path.join(TIMESCALE_SQL_DIR, 'create_foreign_keys.sql'))
        ts.engine.dispose()

        if reconnect:
            self.ts_conn = ts.engine.connect()
