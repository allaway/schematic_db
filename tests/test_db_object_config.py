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

    def test_db_attribute_config_exceptions(self):
        """
        Tests for DBAttributeConfig() that raise exceptions
        """
        with pytest.raises(
            TypeError, match="Param datatype is not of type DBDatatype:"
        ):
            DBAttributeConfig(name="col", datatype="Text")


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
            attributes=[
                DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT),
            ],
            primary_keys=["pk_col"],
            foreign_keys=[],
        )
        assert isinstance(obj1, DBObjectConfig)

        obj2 = DBObjectConfig(
            name="table",
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
        with pytest.raises(TypeError, match="Param attributes is not of type List:"):
            DBObjectConfig(
                name="table",
                attributes=DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT),
                primary_keys=["pk_col"],
                foreign_keys=[],
            )

        with pytest.raises(ValueError, match="Param attributes is empty."):
            DBObjectConfig(
                name="table", attributes=[], primary_keys=["pk_col"], foreign_keys=[]
            )

        with pytest.raises(
            TypeError,
            match="Item in param attributes is not of type DBAttributeConfig:",
        ):
            DBObjectConfig(
                name="table",
                attributes=[{"name": "pk_col", "datatype": DBDatatype.TEXT}],
                primary_keys=["pk_col"],
                foreign_keys=[],
            )

        # test primary_keys
        with pytest.raises(TypeError, match="Param primary_keys is not of type List:"):
            DBObjectConfig(
                name="table",
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys="pk_col",
                foreign_keys=[],
            )

        with pytest.raises(ValueError, match="Param primary_keys is empty."):
            DBObjectConfig(
                name="table",
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=[],
                foreign_keys=[],
            )

        with pytest.raises(
            ValueError,
            match="Item in param primary_keys is missing from param attributes:",
        ):
            DBObjectConfig(
                name="table",
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=["pk_col1"],
                foreign_keys=[],
            )

        # test foreign_keys
        with pytest.raises(TypeError, match="Param foreign_keys is not of type List:"):
            DBObjectConfig(
                name="table",
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=["pk_col"],
                foreign_keys="col",
            )

        with pytest.raises(
            TypeError, match="Key in param foreign_keys is not of type DBForeignKey:"
        ):
            DBObjectConfig(
                name="table",
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=["pk_col"],
                foreign_keys=["col"],
            )

        with pytest.raises(
            ValueError,
            match="Key in param foreign_keys is missing from param attributes:",
        ):
            DBObjectConfig(
                name="table",
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
            ValueError, match="Key in param foreign_keys references its own table:"
        ):
            DBObjectConfig(
                name="table",
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=["pk_col"],
                foreign_keys=[
                    DBForeignKey(
                        name="pk_col",
                        foreign_object_name="table",
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
                    attributes=[
                        DBAttributeConfig(name="pk_col1", datatype=DBDatatype.TEXT),
                    ],
                    primary_keys=["pk_col1"],
                    foreign_keys=[],
                ),
                DBObjectConfig(
                    name="table2",
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
        with pytest.raises(TypeError, match="Param configs is not of type List:"):
            DBObjectConfigList("x")

        with pytest.raises(
            TypeError, match="Item in param configs is not of type DBObjectConfig:"
        ):
            DBObjectConfigList(["x"])

        with pytest.raises(
            ValueError, match="Foreign key in config does not exist in foreign object:"
        ):
            DBObjectConfigList(
                [
                    DBObjectConfig(
                        name="table2",
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
            ValueError,
            match="Foreign key attribute in config does not exist in foreign object:",
        ):
            DBObjectConfigList(
                [
                    DBObjectConfig(
                        name="table",
                        attributes=[
                            DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT),
                        ],
                        primary_keys=["pk_col"],
                        foreign_keys=[],
                    ),
                    DBObjectConfig(
                        name="table2",
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
