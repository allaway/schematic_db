"""Table config objects
These are a set of object for defining a database table in a dialect agnostic way.
"""
# pylint: disable=C0103
from dataclasses import dataclass
from typing import List
from enum import Enum

class DBDatatype(Enum):
    """A generic datatype that should be supported by all database types.
    """
    Text = '1'
    Date = '2'
    Int = '3'
    Float = '4'
    Boolean = '5'

@dataclass
class DBAttributeConfig:
    """A config for a table attribute(column).
    """
    name: str
    datatype: DBDatatype

@dataclass
class DBObjectConfig:
    """A config for a generic database table.
    """
    name: str
    attributes: List[DBAttributeConfig]
    primary_keys: List[str]
    foreign_keys: List[str]
