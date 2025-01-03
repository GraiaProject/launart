from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .status import Phase, ServiceStatusValue, Stage

if TYPE_CHECKING:
    from .core import Bootstrap


@dataclass
class ServiceContext:
    bootstrap: Bootstrap

    def __post_init__(self):
        self._status: ServiceStatusValue = (Stage.EXIT, Phase.WAITING)
        self._sigexit = asyncio.Event()
        self._notify = asyncio.Event()

    def _forward(self, stage: Stage, phase: Phase):
        prev_stage, prev_phase = self._status

        if stage < prev_stage and prev_stage != Stage.EXIT:
            raise ValueError(f"Cannot update stage from {prev_stage} to {stage}")

        if stage == prev_stage:
            if phase <= prev_phase:
                raise ValueError(f"Cannot update phase from {prev_phase} to {phase}")
        else:
            phase = Phase.WAITING

        self._status = (stage, phase)

        self._notify.set()
        self._notify.clear()

    @property
    def should_exit(self):
        return self._sigexit.is_set()

    async def wait_for(self, stage: Stage, phase: Phase):
        val = (stage, phase)

        while val > self._status:
            await self._notify.wait()

    async def wait_for_sigexit(self):
        await self._sigexit.wait()

    @asynccontextmanager
    async def prepare(self):
        self._forward(Stage.PREPARE, Phase.WAITING)
        await self.wait_for(Stage.PREPARE, Phase.PENDING)
        yield
        self._forward(Stage.PREPARE, Phase.COMPLETED)

    @asynccontextmanager
    async def online(self):
        self._forward(Stage.ONLINE, Phase.WAITING)
        await self.wait_for(Stage.ONLINE, Phase.PENDING)
        yield
        self._forward(Stage.ONLINE, Phase.COMPLETED)

    @asynccontextmanager
    async def cleanup(self):
        self._forward(Stage.CLEANUP, Phase.WAITING)
        await self.wait_for(Stage.CLEANUP, Phase.PENDING)
        yield
        self._forward(Stage.CLEANUP, Phase.COMPLETED)

    def dispatch_prepare(self):
        self._forward(Stage.PREPARE, Phase.PENDING)

    def dispatch_online(self):
        self._forward(Stage.ONLINE, Phase.PENDING)

    def dispatch_cleanup(self):
        self._forward(Stage.CLEANUP, Phase.PENDING)

    def exit(self):
        """Call by the manager"""
        self._sigexit.set()

    def exit_complete(self):
        """Call by the manager"""
        self._status = (Stage.EXIT, Phase.COMPLETED)
