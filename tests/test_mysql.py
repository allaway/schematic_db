"""
Testing for  MYSQL.

For testing locally there should be a mysql server running.
A config yaml should exist at 'tests/data/local_mysql_config.yml'.
This config should look like:
username: "root"
password: "root"
host: "localhost"
schema: "test_schema"
The schema should not exist on the database at the beginning of the test.
This file is ignored by git.

If the the config doesn't exist, the file at 'tests/data/mysql_config.yml'
will be used.
"""
# pylint: disable=redefined-outer-name
# pylint: disable=too-many-arguments
# pylint: disable=W0212
# pylint: disable=E0401
import os
import pytest # type: ignore
import sqlalchemy as sa # type: ignore
from rdb_type import MySQL
from db_object_config import DBObjectConfig, DBAttributeConfig, DBDatatype

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "local_mysql_config.yml")
if not os.path.exists(CONFIG_PATH):
    CONFIG_PATH = os.path.join(DATA_DIR, "mysql_config.yml")

@pytest.fixture
def mysql():
    """
    Yields a MYSQL object
    """
    obj = MySQL(config_yaml_path=CONFIG_PATH)
    yield obj

@pytest.fixture
def table_config_no_keys():
    """
    Yields a DBObjectConfig object with no primary or foreign keys
    """
    table_config = DBObjectConfig(
        name = "test_table1",
        attributes = [
            DBAttributeConfig(name="string", datatype=DBDatatype.Text),
            DBAttributeConfig(name="int", datatype=DBDatatype.Int)
        ],
        primary_keys = [],
        foreign_keys = []
    )
    yield table_config

@pytest.fixture
def table_config_one_primary_key():
    """
    Yields a DBObjectConfig object with one primary and no foreign keys
    """
    table_config = DBObjectConfig(
        name = "test_table2",
        attributes = [
            DBAttributeConfig(name="p_key", datatype=DBDatatype.Text),
            DBAttributeConfig(name="string", datatype=DBDatatype.Text),
            DBAttributeConfig(name="int", datatype=DBDatatype.Int)
        ],
        primary_keys = ["p_key"],
        foreign_keys = []
    )
    yield table_config

@pytest.fixture
def row_dicts():
    """
    Yields a list of dicts, each one corresponding to a row in a test table.
    These are used as inputs to update and upsert methods.
    """
    dicts = [
        {"p_key": "key1", "string": "a", "int": 1},
        {"p_key": "key2", "string": "b", "int": 2},
        {"p_key": "key3", "string": "c", "int": 3}
    ]
    yield dicts

@pytest.fixture
def row_tuples():
    """
    Yields a list of tuples, each one corresponding to a row in a test table.
    These are the output form querying a table.
    """
    tuples = [
        ("key1", "a", 1),
        ("key2", "b", 2),
        ("key3", "c", 3)
    ]
    yield tuples

@pytest.fixture
def row_tuples2():
    """
    Yields a list of tuples, each one corresponding to a row in a test table.
    These are the output form querying a table.
    """
    tuples = [
        ("key1", "x", 1),
        ("key2", "y", 2),
        ("key3", "z", 3)
    ]
    yield tuples


class TestMYSQL:
    """
    Testing for MYSQL
    """
    def test_execute_sql_statement(self, mysql):
        """
        Testing for MYSQL.execute_sql_statement()
        """
        result = mysql.execute_sql_statement("SHOW DATABASES;")
        assert isinstance(result, sa.engine.cursor.LegacyCursorResult)

    def test_execute_sql_query(self, mysql):
        """
        Testing for MYSQL.execute_sql_query()
        """
        result = mysql.execute_sql_query("SHOW DATABASES;")
        assert isinstance(result, list)

    def test_get_table_names(self, mysql):
        """
        Testing for MYSQL.get_table_names()
        """
        assert isinstance(mysql.get_table_names(), list)

    def test_get_columns_from_table(self, mysql, table_config_one_primary_key):
        """
        Testing for MYSQL.get_columns_from_table()
        """
        mysql.add_table("test_table2", table_config_one_primary_key)
        columns = mysql._get_columns_from_table(mysql.get_table_names()[0])
        assert isinstance(columns, list)
        assert isinstance(columns[0], dict)
        mysql.drop_table('test_table2')

    def test_get_column_names_from_table(self, mysql, table_config_one_primary_key):
        """
        Testing for MYSQL.get_column_names_from_table()
        """
        mysql.add_table("test_table2", table_config_one_primary_key)
        names = mysql.get_column_names_from_table(mysql.get_table_names()[0])
        assert isinstance(names, list)
        assert isinstance(names[0], str)
        mysql.drop_table('test_table2')

    def test_get_add_drop_table(self, mysql, table_config_no_keys, table_config_one_primary_key):
        """
        Testing for MYSQL.add_table(), and MYSQL.drop_table()
        """
        assert mysql.get_table_names() == []
        mysql.add_table("test_table1", table_config_no_keys)
        assert mysql.get_table_names() == ['test_table1']
        mysql.add_table("test_table2", table_config_one_primary_key)
        assert mysql.get_table_names() == ['test_table1', 'test_table2']
        mysql.drop_table('test_table1')
        mysql.drop_table('test_table2')
        assert mysql.get_table_names() == []

    def test_add_drop_table_column(self, mysql, table_config_no_keys):
        """
        Testing for MYSQL.add_table_column(), and MYSQL.drop_table_column()
        """
        assert mysql.get_table_names() == []
        mysql.add_table("test_table1", table_config_no_keys)
        assert mysql.get_column_names_from_table("test_table1") == ["string", "int"]
        mysql.add_table_column("test_table1", "name", "varchar(100)")
        assert mysql.get_column_names_from_table("test_table1") == ["string", "int", "name"]
        mysql.drop_table_column("test_table1", "name")
        assert mysql.get_column_names_from_table("test_table1") == ["string", "int"]
        mysql.drop_table('test_table1')
        assert mysql.get_table_names() == []

    def test_insert_table_rows(self, mysql, table_config_one_primary_key, row_dicts, row_tuples):
        """
        Testing for MYSQL.insert_table_rows()
        """
        assert mysql.get_table_names() == []
        mysql.add_table("test_table2", table_config_one_primary_key)
        mysql.insert_table_rows("test_table2", [row_dicts[0], row_dicts[1]])
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            == [row_tuples[0], row_tuples[1]]
        mysql.insert_table_rows("test_table2", [row_dicts[2]])
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            == row_tuples
        mysql.drop_table('test_table2')
        assert mysql.get_table_names() == []

    def test_delete_table_rows(self, mysql, table_config_one_primary_key, row_dicts, row_tuples):
        """
        Testing for MYSQL.delete_table_rows()
        """
        assert mysql.get_table_names() == []
        mysql.add_table("test_table2", table_config_one_primary_key)
        assert mysql.get_table_names() == ["test_table2"]
        mysql.insert_table_rows("test_table2", [row_dicts[0]])
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            ==  [row_tuples[0]]
        mysql.insert_table_rows("test_table2", [row_dicts[1]])
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            ==  [row_tuples[0], row_tuples[1]]
        mysql.delete_table_rows("test_table2", "p_key", ["key1"])
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            ==  [row_tuples[1]]
        mysql.delete_table_rows("test_table2", "p_key", ["key2"])
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            ==  []
        mysql.drop_table('test_table2')
        assert mysql.get_table_names() == []

    def test_upsert_table_rows1(
        self, mysql, table_config_one_primary_key, row_dicts, row_tuples, row_tuples2):
        """
        Testing for MYSQL.update_table_rows()
        """
        assert mysql.get_table_names() == []
        mysql.add_table("test_table2", table_config_one_primary_key)
        mysql.insert_table_rows("test_table2", [row_dicts[0]])
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            ==  [row_tuples[0]]
        mysql.upsert_table_rows("test_table2", [row_dicts[0]])
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            ==  [row_tuples[0]]
        mysql.upsert_table_rows("test_table2", [row_dicts[1]])
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            ==  [row_tuples[0], row_tuples[1]]
        mysql.upsert_table_rows("test_table2", [{"p_key": "key1", "string": "x", "int": 1}])
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            ==  [row_tuples2[0], row_tuples[1]]
        mysql.drop_table('test_table2')
        assert mysql.get_table_names() == []

    def test_upsert_table_rows2(
        self, mysql, table_config_one_primary_key, row_dicts, row_tuples, row_tuples2):
        """
        Testing for MYSQL.update_table_rows()
        """
        assert mysql.get_table_names() == []
        mysql.add_table("test_table2", table_config_one_primary_key)
        mysql.insert_table_rows("test_table2", [row_dicts[0]])
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            ==  [row_tuples[0]]
        mysql.upsert_table_rows(
            "test_table2",
            [{"p_key": "key1", "string": "x", "int": 1}, row_dicts[1]]
        )
        assert mysql.execute_sql_query("SELECT * FROM test_table2;") \
            ==  [row_tuples2[0], row_tuples[1]]
        mysql.drop_table('test_table2')
        assert mysql.get_table_names() == []

    def test_get_schemas(self, mysql):
        """
        Testing for MYSQL.get_schemas()
        """
        assert isinstance(mysql._get_schemas(), list)

    def test_get_current_schema(self, mysql):
        """
        Testing for MYSQL.get_current_schema()
        """
        assert mysql._get_current_schema() == "test_schema"
