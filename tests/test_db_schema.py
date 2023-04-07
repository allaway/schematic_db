"""
Testing for DatabaseSchema.
"""
from typing import Generator
import pytest
from schematic_db.db_schema.db_schema import (
    DatabaseSchema,
    TableSchema,
    ColumnSchema,
    ColumnDatatype,
    ForeignKeySchema,
    TableColumnError,
    ConfigForeignKeyMissingObjectError,
    TableKeyError,
    ConfigForeignKeyMissingAttributeError,
)


@pytest.fixture(name="pk_col1_attribute", scope="module")
def fixture_pk_col1_attribute() -> Generator:
    """
    Yields a ColumnSchema
    """
    att = ColumnSchema(name="pk_col1", datatype=ColumnDatatype.TEXT, required=True)
    yield att


@pytest.fixture(name="pk_col1b_attribute", scope="module")
def fixture_pk_col1b_attribute() -> Generator:
    """
    Yields a ColumnSchema
    """
    att = ColumnSchema(
        name="pk_col1", datatype=ColumnDatatype.TEXT, required=True, index=True
    )
    yield att


@pytest.fixture(name="pk_col2_attribute", scope="module")
def fixture_pk_col2_attribute() -> Generator:
    """
    Yields a ColumnSchema
    """
    att = ColumnSchema(name="pk_col2", datatype=ColumnDatatype.TEXT, required=True)
    yield att


@pytest.fixture(name="pk_col3_attribute", scope="module")
def fixture_pk_col3_attribute() -> Generator:
    """
    Yields a ColumnSchema
    """
    att = ColumnSchema(name="pk_col3", datatype=ColumnDatatype.TEXT, required=True)
    yield att


@pytest.mark.fast
class TestForeignKeySchema:  # pylint: disable=too-few-public-methods
    """Testing for ForeignKeySchema"""

    def test_get_attribute_dict(self) -> None:
        """Testing for ForeignKeySchema.get_attribute_dict"""
        obj = ForeignKeySchema(
            name="test",
            foreign_table_name="test_object",
            foreign_column_name="test_name",
        )
        assert isinstance(obj, ForeignKeySchema)
        assert obj.get_column_dict() == {
            "name": "test",
            "foreign_table_name": "test_object",
            "foreign_column_name": "test_name",
        }


@pytest.mark.fast
class TestTableSchema:
    """Testing for TableSchema"""

    def test_is_equivalent(
        self,
        pk_col1_attribute: ColumnSchema,
        pk_col1b_attribute: ColumnSchema,
    ) -> None:
        """Testing for TableSchema.is_equivalent"""
        obj1 = TableSchema(
            name="table",
            columns=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[],
        )

        obj2 = TableSchema(
            name="table",
            columns=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[],
        )

        obj3 = TableSchema(
            name="table",
            columns=[pk_col1b_attribute],
            primary_key="pk_col1",
            foreign_keys=[],
        )

        # obj1 and 2 are the same
        assert obj1 == obj2
        assert obj1.is_equivalent(obj2)

        # obj1 and 3 are the same except the index
        assert obj1 != obj3

    def test_get_foreign_key_dependencies(
        self, pk_col1_attribute: ColumnSchema
    ) -> None:
        """Testing for TableSchema.get_foreign_key_dependencies()"""
        obj1 = TableSchema(
            name="table",
            columns=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[],
        )
        assert obj1.get_foreign_key_dependencies() == []

        obj2 = TableSchema(
            name="table",
            columns=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[
                ForeignKeySchema(
                    name="pk_col1",
                    foreign_table_name="table_two",
                    foreign_column_name="pk_two_col",
                )
            ],
        )
        assert obj2.get_foreign_key_dependencies() == ["table_two"]

    def test_db_object_config_success(self, pk_col1_attribute: ColumnSchema) -> None:
        """Successful tests for TableSchema()"""
        obj1 = TableSchema(
            name="table",
            columns=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[],
        )
        assert isinstance(obj1, TableSchema)

        obj2 = TableSchema(
            name="table",
            columns=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[
                ForeignKeySchema(
                    name="pk_col1",
                    foreign_table_name="table_two",
                    foreign_column_name="pk_two_col",
                )
            ],
        )
        assert isinstance(obj2, TableSchema)

    def test_db_object_config_exceptions(self, pk_col1_attribute: ColumnSchema) -> None:
        """Tests for TableSchema() that raise exceptions"""
        # test columns
        with pytest.raises(
            TableColumnError, match="There are no columns: table_name"
        ):
            TableSchema(
                name="table_name",
                columns=[],
                primary_key="pk_col1",
                foreign_keys=[],
            )

        with pytest.raises(
            TableKeyError,
            match="Primary key is missing from columns: table_name; pk_col2",
        ):
            TableSchema(
                name="table_name",
                columns=[pk_col1_attribute],
                primary_key="pk_col2",
                foreign_keys=[],
            )
        # test foreign_keys
        with pytest.raises(
            TableKeyError,
            match="Foreign key is missing from columns: table_name",
        ):
            TableSchema(
                name="table_name",
                columns=[pk_col1_attribute],
                primary_key="pk_col1",
                foreign_keys=[
                    ForeignKeySchema(
                        name="pk_col2",
                        foreign_table_name="table_two",
                        foreign_column_name="pk_one_col",
                    )
                ],
            )

        with pytest.raises(
            TableKeyError,
            match="Foreign key references its own table: table_name",
        ):
            TableSchema(
                name="table_name",
                columns=[pk_col1_attribute],
                primary_key="pk_col1",
                foreign_keys=[
                    ForeignKeySchema(
                        name="pk_col1",
                        foreign_table_name="table_name",
                        foreign_column_name="pk_one_col",
                    )
                ],
            )


@pytest.mark.fast
class TestDatabaseSchema:
    """Testing for DatabaseSchema"""

    def test_equality(
        self,
        pk_col1_attribute: ColumnSchema,
        pk_col2_attribute: ColumnSchema,
    ) -> None:
        """Testing for DatabaseSchema.__eq__"""
        obj1 = DatabaseSchema(
            [
                TableSchema(
                    name="table1",
                    columns=[pk_col1_attribute],
                    primary_key="pk_col1",
                    foreign_keys=[],
                ),
                TableSchema(
                    name="table2",
                    columns=[pk_col2_attribute],
                    primary_key="pk_col2",
                    foreign_keys=[],
                ),
            ]
        )
        obj2 = DatabaseSchema(
            [
                TableSchema(
                    name="table2",
                    columns=[pk_col2_attribute],
                    primary_key="pk_col2",
                    foreign_keys=[],
                ),
                TableSchema(
                    name="table1",
                    columns=[pk_col1_attribute],
                    primary_key="pk_col1",
                    foreign_keys=[],
                ),
            ]
        )
        assert obj1 == obj2
        assert obj1.is_equivalent(obj2)

    def test_db_object_config_list_success(
        self, pk_col1_attribute: ColumnSchema, pk_col2_attribute: ColumnSchema
    ) -> None:
        """Successful tests for DatabaseSchema()"""
        obj1 = DatabaseSchema(
            [
                TableSchema(
                    name="table",
                    columns=[pk_col1_attribute],
                    primary_key="pk_col1",
                    foreign_keys=[],
                )
            ]
        )
        assert isinstance(obj1, DatabaseSchema)

        obj2 = DatabaseSchema(
            [
                TableSchema(
                    name="table",
                    columns=[pk_col1_attribute],
                    primary_key="pk_col1",
                    foreign_keys=[],
                ),
                TableSchema(
                    name="table2",
                    columns=[pk_col2_attribute],
                    primary_key="pk_col2",
                    foreign_keys=[
                        ForeignKeySchema(
                            name="pk_col2",
                            foreign_table_name="table",
                            foreign_column_name="pk_col1",
                        )
                    ],
                ),
            ]
        )
        assert isinstance(obj2, DatabaseSchema)

    def test_db_object_config_list_exceptions(
        self, pk_col1_attribute: ColumnSchema, pk_col2_attribute: ColumnSchema
    ) -> None:
        """Tests for DatabaseSchema() that raise exceptions"""

        with pytest.raises(
            ConfigForeignKeyMissingObjectError,
            match=(
                "Foreign key 'pk_col2' in object 'table2' references "
                "object 'table' which does not exist in config."
            ),
        ):
            DatabaseSchema(
                [
                    TableSchema(
                        name="table2",
                        columns=[pk_col2_attribute],
                        primary_key="pk_col2",
                        foreign_keys=[
                            ForeignKeySchema(
                                name="pk_col2",
                                foreign_table_name="table",
                                foreign_column_name="pk_col1",
                            )
                        ],
                    )
                ]
            )

        with pytest.raises(
            ConfigForeignKeyMissingAttributeError,
            match=(
                "Foreign key 'pk_col2' in object 'table2' references attribute "
                "'pk_col3' which does not exist in object'table'"
            ),
        ):
            DatabaseSchema(
                [
                    TableSchema(
                        name="table",
                        columns=[pk_col1_attribute],
                        primary_key="pk_col1",
                        foreign_keys=[],
                    ),
                    TableSchema(
                        name="table2",
                        columns=[pk_col2_attribute],
                        primary_key="pk_col2",
                        foreign_keys=[
                            ForeignKeySchema(
                                name="pk_col2",
                                foreign_table_name="table",
                                foreign_column_name="pk_col3",
                            )
                        ],
                    ),
                ]
            )
