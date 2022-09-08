"""
Testing for  DBObjectConfig.
"""
import pytest
from db_object_config import (
    DBObjectConfigList,
    DBObjectConfig,
    DBAttributeConfig,
    DBDatatype,
    DBForeignKey,
    ConfigAttributeError,
    ConfigForeignKeyObjectError,
    ConfigKeyError,
    ConfigForeignKeyObjectError2,
)


class TestDBAttributeConfig:
    """
    Testing for DBAttributeConfig
    """

    def test_db_attribute_config_success(self):
        """
        Successful tests for DBAttributeConfig()
        """
        obj = DBAttributeConfig(name="col", datatype=DBDatatype.TEXT)
        assert isinstance(obj, DBAttributeConfig)


class TestDBObjectConfig:
    """
    Testing for DBObjectConfig
    """

    def test_db_object_config_success(self):
        """
        Successful tests for DBObjectConfig()
        """
        obj1 = DBObjectConfig(
            name="table",
            manifest_ids=["syn1"],
            attributes=[
                DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT),
            ],
            primary_keys=["pk_col"],
            foreign_keys=[],
        )
        assert isinstance(obj1, DBObjectConfig)

        obj2 = DBObjectConfig(
            name="table",
            manifest_ids=["syn1"],
            attributes=[
                DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT),
            ],
            primary_keys=["pk_col"],
            foreign_keys=[
                DBForeignKey(
                    name="pk_col",
                    foreign_object_name="table_two",
                    foreign_attribute_name="pk_two_col",
                )
            ],
        )
        assert isinstance(obj2, DBObjectConfig)

    def test_db_object_config_exceptions(self):
        """
        Tests for DBObjectConfig() that raise exceptions
        """
        # test attributes
        with pytest.raises(
            ConfigAttributeError, match="Attributes is empty: table_name"
        ):
            DBObjectConfig(
                name="table_name",
                manifest_ids=["syn1"],
                attributes=[],
                primary_keys=["pk_col"],
                foreign_keys=[],
            )

        # test primary_keys
        with pytest.raises(ConfigKeyError, match="Primary keys is empty: table_name"):
            DBObjectConfig(
                name="table_name",
                manifest_ids=["syn1"],
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=[],
                foreign_keys=[],
            )

        with pytest.raises(
            ConfigKeyError,
            match="Primary key is missing from attributes: table_name; pk_col1",
        ):
            DBObjectConfig(
                name="table_name",
                manifest_ids=["syn1"],
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=["pk_col1"],
                foreign_keys=[],
            )
        # test foreign_keys
        with pytest.raises(
            ConfigKeyError,
            match="Foreign key is missing from attributes: table_name",
        ):
            DBObjectConfig(
                name="table_name",
                manifest_ids=["syn1"],
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=["pk_col"],
                foreign_keys=[
                    DBForeignKey(
                        name="pk_col1",
                        foreign_object_name="table_two",
                        foreign_attribute_name="pk_one_col",
                    )
                ],
            )

        with pytest.raises(
            ConfigKeyError,
            match="Foreign key references its own object: table_name",
        ):
            DBObjectConfig(
                name="table_name",
                manifest_ids=["syn1"],
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=["pk_col"],
                foreign_keys=[
                    DBForeignKey(
                        name="pk_col",
                        foreign_object_name="table_name",
                        foreign_attribute_name="pk_one_col",
                    )
                ],
            )


class TestDBObjectConfigList:
    """
    Testing for DBObjectConfigList
    """

    def test_db_object_config_list_success(self):
        """
        Successful tests for DBObjectConfigList()
        """
        obj1 = DBObjectConfigList(
            [
                DBObjectConfig(
                    name="table",
                    manifest_ids=["syn1"],
                    attributes=[
                        DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT),
                    ],
                    primary_keys=["pk_col"],
                    foreign_keys=[],
                )
            ]
        )
        assert isinstance(obj1, DBObjectConfigList)

        obj2 = DBObjectConfigList(
            [
                DBObjectConfig(
                    name="table",
                    manifest_ids=["syn1"],
                    attributes=[
                        DBAttributeConfig(name="pk_col1", datatype=DBDatatype.TEXT),
                    ],
                    primary_keys=["pk_col1"],
                    foreign_keys=[],
                ),
                DBObjectConfig(
                    name="table2",
                    manifest_ids=["syn1"],
                    attributes=[
                        DBAttributeConfig(name="pk_col2", datatype=DBDatatype.TEXT),
                    ],
                    primary_keys=["pk_col2"],
                    foreign_keys=[
                        DBForeignKey(
                            name="pk_col2",
                            foreign_object_name="table",
                            foreign_attribute_name="pk_col1",
                        )
                    ],
                ),
            ]
        )
        assert isinstance(obj2, DBObjectConfigList)

    def test_db_object_config_list_exceptions(self):
        """
        Tests for DBObjectConfigList() that raise exceptions
        """

        with pytest.raises(
            ConfigForeignKeyObjectError, match="Foreign key 'DBForeignKey"
        ):
            DBObjectConfigList(
                [
                    DBObjectConfig(
                        name="table2",
                        manifest_ids=["syn1"],
                        attributes=[
                            DBAttributeConfig(name="pk_col2", datatype=DBDatatype.TEXT),
                        ],
                        primary_keys=["pk_col2"],
                        foreign_keys=[
                            DBForeignKey(
                                name="pk_col2",
                                foreign_object_name="table",
                                foreign_attribute_name="pk_col1",
                            )
                        ],
                    )
                ]
            )

        with pytest.raises(
            ConfigForeignKeyObjectError2,
            match="Foreign key 'DBForeignKey",
        ):
            DBObjectConfigList(
                [
                    DBObjectConfig(
                        name="table",
                        manifest_ids=["syn1"],
                        attributes=[
                            DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT),
                        ],
                        primary_keys=["pk_col"],
                        foreign_keys=[],
                    ),
                    DBObjectConfig(
                        name="table2",
                        manifest_ids=["syn1"],
                        attributes=[
                            DBAttributeConfig(name="pk_col2", datatype=DBDatatype.TEXT),
                        ],
                        primary_keys=["pk_col2"],
                        foreign_keys=[
                            DBForeignKey(
                                name="pk_col2",
                                foreign_object_name="table",
                                foreign_attribute_name="pk_col1",
                            )
                        ],
                    ),
                ]
            )
