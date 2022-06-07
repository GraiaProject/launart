from dataclasses import dataclass

from graia.saya.schema import BaseSchema
from graia.saya.behaviour import Behaviour
from graia.saya.cube import Cube

from launart import Launart, Launchable, Service


@dataclass
class ServiceSchema(BaseSchema):
    pass

class LaunartBehaviour(Behaviour):
    manager: Launart

    def __init__(self, manager: Launart) -> None:
        self.manager = manager
    
    def allocate(self, cube: Cube[ServiceSchema]):
        if isinstance(cube.metaclass, ServiceSchema):
            if not isinstance(cube.content, Service):
                raise TypeError(f"{cube.content} is not a Service")
            self.manager.add_service(cube.content)
        else:
            return
        return True
    
    def uninstall(self, cube: Cube[ServiceSchema]):
        if isinstance(cube.metaclass, ServiceSchema):
            self.manager.remove_service(cube.content)
        else:
            return
        return True