
from __future__ import annotations

from collections import ChainMap
from typing import TYPE_CHECKING, Mapping, TypeVar
from typing_extensions import TypeAlias

if TYPE_CHECKING:
    import asyncio

    from .context import ServiceContext
    from .service import Service

T = TypeVar("T")

_Set: TypeAlias = "dict[T, None]"


class ServiceGraph:
    services: dict[str, Service]
    contexts: dict[str, ServiceContext]
    tasks: dict[str, asyncio.Task]

    _previous: dict[str, _Set[str]]
    _next: dict[str, _Set[str]]

    def __init__(self):
        self.services = {}
        self.contexts = {}
        self.tasks = {}

        self._previous = {}
        self._next = {}

    def subgraph(self, *services: Service):
        services_: dict[str, Service] = {i.id: i for i in services}

        if services_.keys() & self.services.keys():
            raise ValueError("Service id conflict.")

        _services: Mapping[str, Service] = ChainMap(services_, self.services)
        _previous: ChainMap[str, _Set[str]] = ChainMap({}, self._previous)
        _next: ChainMap[str, _Set[str]] = ChainMap({}, self._next)

        # _previous: dict[str, _Set[str]] = {}
        # _next: dict[str, _Set[str]] = {}

        for i in services:
            _previous[i.id] = dict.fromkeys(i.after)
            _next[i.id] = dict.fromkeys(i.before)

            for p in i.after:
                if p not in _services:
                    raise ValueError(f"Service {i.id} after {p} not found.")

                if p in _next:
                    t = _next[p]
                else:
                    t = _next[p] = {}

                t[i.id] = None

            for n in i.before:
                if n not in _services:
                    raise ValueError(f"Service {i.id} before {n} not found.")

                if n in _previous:
                    t = _previous[n]
                else:
                    t = _previous[n] = {}

                t[i.id] = None

        return _services.maps[0], _previous, _next

    def apply(self, service_bind: dict[str, Service], previous: ChainMap[str, _Set[str]], nexts: ChainMap[str, _Set[str]]):
        self.services.update(service_bind)
        self._previous.update(previous)
        self._next.update(nexts)

    def drop(self, service: Service):
        self.services.pop(service.id, None)
        self.contexts.pop(service.id, None)
        self.tasks.pop(service.id, None)

        self._previous.pop(service.id, None)
        self._next.pop(service.id, None)

        for i in self._previous:
            self._previous[i].pop(service.id, None)

        for i in self._next:
            self._next[i].pop(service.id, None)
