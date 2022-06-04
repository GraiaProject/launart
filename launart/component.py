from __future__ import annotations

from abc import ABCMeta, abstractmethod
import asyncio
from contextlib import asynccontextmanager
from tkinter import N
from typing import TYPE_CHECKING, List, Optional, Set, cast

from statv import Stats, Statv

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

if TYPE_CHECKING:
    from launart.manager import Launart

U_Stage = Literal["prepare", "blocking", "cleanup", "finished"]
# 现在所处的阶段.
# 状态机-like: 只有 prepare -> blocking -> cleanup 这个流程.
# finished 仅标记完成, 不表示阶段.


class LaunchableStatus(Statv):
    stage = Stats[Optional[U_Stage]]("stage", default=None)

    def __init__(self) -> None:
        super().__init__()

    @property
    def prepared(self) -> bool:
        return self.stage == "blocking"

    @property
    def blocking(self) -> bool:
        return self.stage == "blocking"

    @property
    def finished(self) -> bool:
        return self.stage == "finished"

    def unset(self) -> None:
        self.stage = None

    async def wait_for_prepared(self):
        while self.stage == "prepare" or self.stage is None:
            await self.wait_for_update()

    async def wait_for_cleaned(self):
        while self.stage != "finished":
            await self.wait_for_update()

    async def wait_for_finished(self):
        while self.stage != "finished":
            await self.wait_for_update()


STAGE_MAPPING = {
    "prepare": "preparing",
    "blocking": "blocking",
    "cleanup": "cleaning",
    "finished": "finished"
}
STAGE_MAPPING_REVERSED = {
    "preparing": "prepare",
    "blocking": "blocking",
    "cleaning": "cleanup",
    "finished": "finished"
}
STAGES: list[U_Stage] = ["prepare", "blocking", "cleanup", "finished"]

class Launchable(metaclass=ABCMeta):
    id: str
    status: LaunchableStatus
    manager: Optional[Launart] = None

    def __init__(self) -> None:
        self.status = LaunchableStatus()

    @property
    @abstractmethod
    def required(self) -> Set[str]:
        ...

    @property
    @abstractmethod
    def stages(self) -> Set[U_Stage]:
        ...

    def ensure_manager(self, manager: Launart):
        if self.manager is not None:
            raise RuntimeError("this launchable attempted to be mistaken a wrong ownership of launart/manager.")
        self.manager = manager

    @asynccontextmanager
    async def stage(self, stage: U_Stage):
        if self.manager is None:
            raise RuntimeError("attempted to set stage of a launchable without a manager.")
        if self.manager.status.stage is None:
            raise RuntimeError("attempted to set stage of a launchable without a current manager")

        m = cast(U_Stage, STAGE_MAPPING_REVERSED[self.manager.status.stage])
        n = STAGES.index(m) + 1
        l = STAGES[n:]
        # example: cleaning -> cleaned -> [cleaned, finished], if stage in [prepare, blocking]: error
        if stage not in l:
            raise ValueError(f"stage {stage} is not allowed in this context/stage of {self.manager.status.stage}")

        while self.manager.status.stage != STAGE_MAPPING[stage]:
            await self.manager.status.wait_for_update()
        yield
        # stage=cleanup -> cleaning -> cleaned and set stat.
        self.status.stage = stage

    async def wait_for_required(self, stage: U_Stage = "blocking"):
        if self.manager is None:
            raise RuntimeError("attempted to set stage of a launchable without a manager.")
        requires = [self.manager.get_launchable(id) for id in self.required]
        if any(launchable.status.stage != stage for launchable in requires):
            await asyncio.wait([launchable.status.wait_for_prepared() for launchable in requires])

    async def wait_for(self, stage: U_Stage, *launchable_id: str):
        if self.manager is None:
            raise RuntimeError("attempted to set stage of a launchable without a manager.")
        launchables = [self.manager.get_launchable(id) for id in launchable_id]
        while any(launchable.status.stage != stage for launchable in launchables):
            await asyncio.wait([launchable.status.wait_for_update() for launchable in launchables if launchable.status.stage != stage])

    @abstractmethod
    async def launch(self, manager: Launart):
        pass

    def on_require_prepared(self, components: Set[str]):
        pass

    def on_require_exited(self, components: Set[str]):
        pass


class RequirementResolveFailed(Exception):
    pass


def resolve_requirements(
    components: Set[Launchable],
) -> List[Set[Launchable]]:
    resolved = set()
    result = []
    while components:
        layer = {component for component in components if component.required.issubset(resolved)}

        if layer:
            components -= layer
            resolved.update(component.id for component in layer)
            result.append(layer)
        else:
            raise RequirementResolveFailed
    return result
