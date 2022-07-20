from dataclasses import dataclass
from typing import List
from enum import Enum

class DBDatatype(Enum):
    Text = '1'
    Date = '2'
    Int = '3',
    Float = '4',
    Boolean = '5'

@dataclass
class DBAttributeConfig:
    name: str
    datatype: DBDatatype

@dataclass
class DBObjectConfig:
    name: str
    attributes: List[DBAttributeConfig]
    primary_keys: List[str]
    foreign_keys: List[str]