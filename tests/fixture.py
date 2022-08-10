from __future__ import annotations

from launart import Launart, Launchable
from launart.service import ExportInterface, Service
from launart.utilles import PriorityType


class EmptyLaunchable(Launchable):
    id = "empty"
    triggered = False

    @property
    def stages(self):
        return set()

    @property
    def required(self):
        return set()

    async def launch(self, manager: Launart):
        assert manager is self.manager
        self.triggered = True


def component_standalone(component_id: str, required_ids: list[str]) -> Launchable:
    class _L(Launchable):
        id = component_id

        @property
        def required(self):
            return set(required_ids)

        @property
        def _required_id(self):
            return set(required_ids)

        @property
        def stages(self):
            return set()

        async def launch(self, _):
            ...

    return _L()


def component(component_id: str, required_ids: list[str | type[ExportInterface]]) -> Launchable:
    class _L(Launchable):
        id = component_id

        @property
        def required(self):
            return set(required_ids)

        @property
        def stages(self):
            return set()

        async def launch(self, _):
            ...

    return _L()


def interface() -> type[ExportInterface]:
    return type("$NewInterface", (ExportInterface,), {})


def service(component_id: str, interfaces: PriorityType, required_ids: list[str | type[ExportInterface]]) -> Service:
    class Srv(Service):
        supported_interface_types = interfaces
        id = component_id

        @property
        def required(self):
            return set(required_ids)

        @property
        def stages(self):
            return set()

        async def launch(self, _):
            ...

        def get_interface(self, interface_type):
            return interface_type()

    return Srv()
