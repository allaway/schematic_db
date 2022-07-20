from abc import ABC, abstractmethod
from typing import List
from db_object_config import DBObjectConfig

class RDBType(ABC):

    @abstractmethod
    def execute_sql_statement(self, statement: str):
        pass

    @abstractmethod
    def execute_sql_query(self, query: str) -> List:
        pass

    @abstractmethod
    def add_table(self, table_id: str, table_config: DBObjectConfig):
        pass

    @abstractmethod
    def drop_table(self, table_id: str):
        pass

    @abstractmethod
    def add_table_column(self, table_id: str, column_name: str, datatype: str):
        pass

    @abstractmethod
    def drop_table_column(self, table_id: str, column_name: str):
        pass

    @abstractmethod
    def insert_table_rows(self, table_id: str):
        pass

    @abstractmethod
    def delete_table_rows(self, table_id: str):
        pass

    @abstractmethod
    def update_table_rows(self, table_id: str):
        pass

    @abstractmethod
    def get_tables(self) -> List[str]:
        pass

    @abstractmethod
    def get_columns_from_table(self, table_id: str) -> List[str]:
        pass

    @abstractmethod
    def get_column_names_from_table(self, table_id: str) -> List[str]:
        pass
