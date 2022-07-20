import logging
import pytest
from rdb_type import MySQL
from db_object_config import DBObjectConfig, DBAttributeConfig, DBDatatype


@pytest.fixture
def mysql1():
    obj = MySQL(
            config_yaml_path="/home/alamb/schematic/sql_config.yml",
            schema_name = "mysql"
        )
    yield obj

@pytest.fixture
def mysql2():
    obj = MySQL(
            config_yaml_path="/home/alamb/schematic/sql_config.yml",
            schema_name = "test"
        )
    yield obj

@pytest.fixture
def table_config_no_keys():
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
    row_dicts = [
        {"p_key": "key1", "string": "a", "int": 1},
        {"p_key": "key2", "string": "b", "int": 2},
        {"p_key": "key3", "string": "c", "int": 3}
    ]
    yield row_dicts

@pytest.fixture
def row_tuples():
    row_tuples = [
        ("key1", "a", 1),
        ("key2", "b", 2),
        ("key3", "c", 3)
    ]
    yield row_tuples


class TestMYSQL:

    def test_init1(self):
        obj = MySQL(
            config_yaml_path="/home/alamb/schematic/sql_config.yml",
            schema_name = "test"
        )
        assert ("test",) in obj.engine.execute("SHOW DATABASES;").fetchall()
        obj.engine.execute("DROP DATABASE test;")
        assert ("test",) not in obj.engine.execute("SHOW DATABASES;").fetchall()

    def test_init2(self):
        MySQL(
            config_yaml_path="/home/alamb/schematic/sql_config.yml",
            schema_name = "mysql"
        )

    def test_execute_sql_query(self, mysql1):
        result = mysql1.execute_sql_query("SELECT * FROM " + mysql1.get_tables()[0])
        assert isinstance(result, list)

    def test_add_drop_table(self, mysql2, table_config_no_keys, table_config_one_primary_key):
        assert mysql2.get_tables() == []
        mysql2.add_table("test_table1", table_config_no_keys)
        assert mysql2.get_tables() == ['test_table1']
        mysql2.add_table("test_table2", table_config_one_primary_key)
        assert mysql2.get_tables() == ['test_table1', 'test_table2']
        mysql2.drop_table('test_table1')
        mysql2.drop_table('test_table2')
        assert mysql2.get_tables() == []

    def test_add_drop_table_column(self, mysql2, table_config_no_keys):
        assert mysql2.get_tables() == []
        mysql2.add_table("test_table1", table_config_no_keys)
        assert mysql2.get_column_names_from_table("test_table1") == ["string", "int"]
        mysql2.add_table_column("test_table1", "name", "varchar(100)")
        assert mysql2.get_column_names_from_table("test_table1") == ["string", "int", "name"]
        mysql2.drop_table_column("test_table1", "name")
        assert mysql2.get_column_names_from_table("test_table1") == ["string", "int"]
        mysql2.drop_table('test_table1')
        assert mysql2.get_tables() == []

    def test_insert_table_rows(self, mysql2, table_config_one_primary_key, row_dicts, row_tuples):
        assert mysql2.get_tables() == []
        mysql2.add_table("test_table2", table_config_one_primary_key)
        mysql2.insert_table_rows("test_table2", [row_dicts[0], row_dicts[1]])
        assert mysql2.execute_sql_query("SELECT * FROM test_table2;") == [row_tuples[0], row_tuples[1]]
        mysql2.insert_table_rows("test_table2", [row_dicts[2]])
        assert mysql2.execute_sql_query("SELECT * FROM test_table2;") == row_tuples
        mysql2.drop_table('test_table2')
        assert mysql2.get_tables() == []

    def test_delete_table_rows(self, mysql2, table_config_one_primary_key, row_dicts, row_tuples):
        assert mysql2.get_tables() == []
        mysql2.add_table("test_table2", table_config_one_primary_key)
        assert mysql2.get_tables() == ["test_table2"]
        mysql2.insert_table_rows("test_table2", [row_dicts[0]])
        assert mysql2.execute_sql_query("SELECT * FROM test_table2;") ==  [row_tuples[0]]
        mysql2.insert_table_rows("test_table2", [row_dicts[1]])
        assert mysql2.execute_sql_query("SELECT * FROM test_table2;") ==  [row_tuples[0], row_tuples[1]]
        mysql2.delete_table_rows("test_table2", "p_key", ["key1"])
        assert mysql2.execute_sql_query("SELECT * FROM test_table2;") ==  [row_tuples[1]]
        mysql2.delete_table_rows("test_table2", "p_key", ["key2"])
        assert mysql2.execute_sql_query("SELECT * FROM test_table2;") ==  []
        mysql2.drop_table('test_table2')
        assert mysql2.get_tables() == []

    def test_get_tables(self, mysql1):
        assert isinstance(mysql1.get_tables(), list)

    def test_get_columns_from_table(self, mysql1):
        columns = mysql1.get_columns_from_table(mysql1.get_tables()[0])
        assert isinstance(columns, list)
        assert isinstance(columns[0], dict)

    def test_get_column_names_from_table(self, mysql1):
        names = mysql1.get_column_names_from_table(mysql1.get_tables()[0])
        assert isinstance(names, list)
        assert isinstance(names[0], str)

    def test_get_schemas(self, mysql1):
        assert isinstance(mysql1._get_schemas(), list)

    def test_get_current_schema(self, mysql1):
        assert mysql1._get_current_schema() == "mysql"

    def test_change_current_schema(self, mysql1):
        assert mysql1._get_current_schema() == "mysql"
        mysql1._change_current_schema("sys")
        assert mysql1._get_current_schema() == "sys"
        mysql1._change_current_schema("mysql")
        assert mysql1._get_current_schema() == "mysql"
