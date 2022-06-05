from __future__ import annotations

import asyncio
from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, List, Optional, Set, cast

from loguru import logger
from statv import Stats, Statv

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

if TYPE_CHECKING:
    from launart.manager import Launart

U_Stage = Literal["waiting-for-prepare", "prepare", "prepared", "blocking", "waiting-for-cleanup", "cleanup", "finished"]
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

    async def wait_for(self, *stages: U_Stage):
        while self.stage not in stages:
            await self.wait_for_update()

    async def wait_blocking_finish(self):
        while self.stage == "blocking":
            await self.wait_for_update()

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
    def stages(self) -> Set[Literal["prepare", "blocking", "cleanup"]]:
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
            raise LookupError("attempted to set stage of a launchable without a current manager")
        if stage not in self.stages:
            raise ValueError(f"undefined and unexpected stage entering: {stage}")
    
        """
        m = cast(U_Stage, STAGE_MAPPING_REVERSED[self.manager.status.stage])
        n = STAGES.index(m) + 1
        l = STAGES[n:]
        # example: cleaning -> cleaned -> [cleaned, finished], if stage in [prepare, blocking]: error
        print(l)
        if stage not in l:
            raise ValueError(f"stage {stage} is not allowed in this context/stage of {self.manager.status.stage}")"""

        if stage == "prepare":
            while self.status.stage in {"waiting-for-prepare", None}:
                await self.status.wait_for_update()
        elif stage == "cleanup":
            while self.status.stage in {"blocking", "waiting-for-cleanup"}:
                await self.status.wait_for_update()
        elif stage == "blocking":
            self.status.stage = "blocking"

        while self.manager.status.stage != STAGE_MAPPING[stage]:
            await self.manager.status.wait_for_update()
        yield
        if stage == "prepare":
            self.status.stage = "prepared"
        elif stage == "blocking":
            logger.info(f"{self.id} completed blocking stage.")
            if "cleanup" in self.stages:
                self.status.stage = "waiting-for-cleanup"
            else:
                self.status.stage = "finished"
        elif stage == "cleanup":
            logger.info(f"{self.id} completed cleanup stage.")
            self.status.stage = "finished"

    async def wait_for_required(self, stage: U_Stage = "blocking"):
        await self.wait_for(stage, *self.required)

    async def wait_for(self, stage: U_Stage, *launchable_id: str):
        if self.manager is None:
            raise RuntimeError("attempted to set stage of a launchable without a manager.")
        launchables = [self.manager.get_launchable(id) for id in launchable_id]
        while any(launchable.status.stage != stage for launchable in launchables):
            await asyncio.wait([launchable.status.wait_for_update() for launchable in launchables if launchable.status.stage != stage])

    @abstractmethod
    async def launch(self, manager: Launart):
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
