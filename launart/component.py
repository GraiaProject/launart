from __future__ import annotations

import asyncio
from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, List, Optional, Set, Union

from statv import Stats, Statv

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

if TYPE_CHECKING:
    from launart.manager import Launart

U_Stage = Union[
    Literal[
        "waiting-for-prepare",
        "preparing",
        "prepared",
        "blocking",
        "blocking-completed",
        "waiting-for-cleanup",
        "cleanup",
        "finished",
    ],
    None,
]
STAGE_STAT = {
    None: {"waiting-for-prepare", "waiting-for-cleanup", "blocking"},
    "waiting-for-prepare": {"preparing"},
    "preparing": {"prepared"},
    "prepared": {"blocking", "waiting-for-cleanup", "finished"},
    "blocking": {"blocking-completed"},
    "blocking-completed": {"waiting-for-cleanup", "finished"},
    "waiting-for-cleanup": {"cleanup"},
    "cleanup": {"finished"},
    "finished": {None},
}
# for runtime check:
STATS = [
    None,
    "waiting-for-prepare",
    "preparing",
    "prepared",
    "blocking",
    "blocking-completed",
    "waiting-for-cleanup",
    "cleanup",
    "finished",
]


class LaunchableStatus(Statv):
    stage = Stats[Optional[U_Stage]]("stage", default=None)

    def __init__(self) -> None:
        super().__init__()

    @property
    def prepared(self) -> bool:
        return self.stage in ("prepared", "blocking")

    @property
    def blocking(self) -> bool:
        return self.stage == "blocking"

    @property
    def finished(self) -> bool:
        return self.stage == "finished"

    @staticmethod
    @stage.validator
    def _(stats: Stats[U_Stage | None], past: U_Stage | None, current: U_Stage | None):
        if current not in STAGE_STAT[past]:
            raise ValueError(f"Invalid stage transition: {past} -> {current}")
        return current

    def unset(self) -> None:
        self.stage = None

    async def wait_for(self, *stages: U_Stage):
        if not stages:
            return
        while self.stage not in stages:
            await self.wait_for_update()


STAGE_MAPPING = {"prepare": "preparing", "blocking": "blocking", "cleanup": "cleaning", "finished": "finished"}
STAGE_MAPPING_REVERSED = {"preparing": "prepare", "blocking": "blocking", "cleaning": "cleanup", "finished": "finished"}


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

        if stage == "preparing":
            if "waiting-for-prepare" not in STAGE_STAT[self.status.stage]:
                raise ValueError(f"unexpected stage entering: {self.status.stage} -> waiting-for-prepare")
            await self.manager.status.wait_for_preparing()
            self.status.stage = "waiting-for-prepare"
            await self.status.wait_for("preparing")
            yield
            self.status.stage = "prepared"
        elif stage == "cleanup":
            if "waiting-for-cleanup" not in STAGE_STAT[self.status.stage]:
                raise ValueError(f"unexpected stage entering: {self.status.stage} -> waiting-for-cleanup")
            await self.manager.status.wait_for_cleaning(current=self.id)
            self.status.stage = "waiting-for-cleanup"
            await self.status.wait_for("cleanup")
            yield
            self.status.stage = "finished"
        elif stage == "blocking":
            if "blocking" not in STAGE_STAT[self.status.stage]:
                raise ValueError(f"unexpected stage entering: {self.status.stage} -> blocking")
            await self.manager.status.wait_for_blocking()
            await self.wait_for_required()
            self.status.stage = "blocking"
            yield
            self.status.stage = "blocking-completed"
        else:
            raise ValueError(f"unexpected stage entering: {stage}(unknown define)")

    async def wait_for_required(self, stage: U_Stage = "prepared"):
        await self.wait_for(stage, *self.required)

    async def wait_for(self, stage: U_Stage, *launchable_id: str):
        if self.manager is None:
            raise RuntimeError("attempted to set stage of a launchable without a manager.")
        launchables = [self.manager.get_launchable(id) for id in launchable_id]
        while any(launchable.status.stage not in STATS[STATS.index(stage) :] for launchable in launchables):
            # print([(i.id, i.status.stage) for i in launchables])
            await asyncio.wait(
                [launchable.status.wait_for_update() for launchable in launchables if launchable.status.stage != stage],
                return_when=asyncio.FIRST_COMPLETED,
            )

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
