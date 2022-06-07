from collections import ChainMap
from typing import Any, Dict


class Override:
    def __init__(self, source: Any, additional: Dict[str, Any]):
        self.__source = source
        self.__additional = additional

    @property
    def source(self):
        return self.__source

    @property
    def __data(self):
        return ChainMap(self.__additional, vars(self.__source))

    def __getattr__(self, item):
        if item not in self.__data:
            raise AttributeError(f"'{self.__source.__class__.__name__}' object has no attribute '{item}'")
        return self.__data[item]


def override(source: Any, additional: Dict[str, Any]) -> Override:
    return Override(source, additional)
