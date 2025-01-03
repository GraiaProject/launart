from typing import Any, TypeVar, cast


class Override:
    def __init__(self, source: Any, additional: dict[str, Any]):
        self.__source = source
        self.__additional = additional

    @property
    def _source(self):
        return self.__source

    def __getattr__(self, item):
        if item in self.__additional:
            return self.__additional[item]
        return getattr(self.__source, item)


T = TypeVar("T")


def override(source: T, additional: dict[str, Any]) -> T:
    return cast(T, Override(source, additional))
