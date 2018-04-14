from abc import ABC, abstractmethod
from enum import Enum, auto


class Spec(ABC):

    def __init__(self, spec_dict):
        self.spec_dict = spec_dict

    @property
    @abstractmethod
    def api_url(self):
        pass

    @property
    @abstractmethod
    def endpoints(self):
        pass


class Model(ABC):

    def __init__(self, spec_dict):
        self.spec_dict = spec_dict

    @property
    @abstractmethod
    def fields(self):
        pass

    @property
    @abstractmethod
    def operations(self):
        pass


class Field:

    def __init__(self, name, field_type):
        self.name = name
        self.type = field_type


class Endpoint(ABC):

    def __init__(self, spec_dict):
        self.spec_dict = spec_dict

    @property
    @abstractmethod
    def operations(self):
        pass


class Operation(ABC):

    def __init__(self, method, spec_dict):
        self.method = method
        self.spec_dict = spec_dict


class FieldType(Enum):
    STRING = auto()
    INTEGER = auto()
    DECIMAL = auto()
    BOOLEAN = auto()
    ARRAY = auto()
    OBJECT = auto()
    DATETIME = auto()
    DATE = auto()
    TIME = auto()
