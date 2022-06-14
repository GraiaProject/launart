from __future__ import annotations

from dataclasses import dataclass

from graia.saya.behaviour import Behaviour
from graia.saya.cube import Cube
from graia.saya.schema import BaseSchema

from launart import Launart, Launchable, Service


@dataclass
class LaunchableSchema(BaseSchema):
    pass


@dataclass
class ServiceSchema(BaseSchema):
    pass


class LaunartBehaviour(Behaviour):
    manager: Launart
    allow_unsafe: bool = False

    def __init__(self, manager: Launart, *, allow_unsafe: bool = False) -> None:
        self.manager = manager
        self.allow_unsafe = allow_unsafe

    def allocate(self, cube: Cube[LaunchableSchema | ServiceSchema]):
        if isinstance(cube.metaclass, ServiceSchema):
            if not isinstance(cube.content, Service):
                raise TypeError(f"{cube.content} is not a Service")
            self.manager.add_service(cube.content)
        elif isinstance(cube.metaclass, LaunchableSchema):
            if not isinstance(cube.content, Launchable):
                raise TypeError(f"{cube.content} is not a Launchable")
            self.manager.add_launchable(cube.content)
        else:
            return
        return True

    def release(self, cube: Cube[ServiceSchema]):
        if isinstance(cube.metaclass, ServiceSchema):
            self.manager.remove_service(cube.content, unsafe=self.allow_unsafe)
        elif isinstance(cube.metaclass, Launchable):
            self.manager.remove_launchable(cube.content, unsafe=self.allow_unsafe)
        else:
            return
        return True

    uninstall = release