from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from .core import Bootstrap


class _State(Enum):
    PREPARE_PRE = auto()
    PREPARE_POST = auto()
    CLEANUP_PRE = auto()
    CLEANUP_POST = auto()
    READY = auto()


@dataclass
class ServiceContext:
    State: ClassVar = _State

    bootstrap: Bootstrap

    def __post_init__(self):
        self._state: _State | None = None
        self._ready: bool | None = None
        self._notify = asyncio.Event()
        self._switch = asyncio.Event()
        self._sigexit = asyncio.Event()

    def _update(self):
        self._notify.set()

    def switch(self):
        self._switch.set()

    def enter(self):
        if self._state is not _State.PREPARE_POST:
            return

        self._ready = True
        self._update()

    def skip(self):
        if self._state is not _State.PREPARE_POST:
            return

        self._ready = False
        self._update()

    def exit(self):
        "Call by the manager"
        self._sigexit.set()

    @property
    def state(self):
        if self._ready:
            return self.State.READY
        return self._state

    async def wait_prepare_pre(self):
        await self._switch.wait()
        if self._state is not _State.PREPARE_PRE:
            raise RuntimeError(f"expected {self.State.PREPARE_PRE}, got {self._state}")

        self._switch.clear()
        self._update()

    async def wait_cleanup_pre(self):
        await self._switch.wait()
        if self._state is not _State.CLEANUP_PRE:
            raise RuntimeError(f"expected {self.State.CLEANUP_PRE}, got {self._state}")

        self._switch.clear()
        self._update()

    async def wait_prepare_post(self):
        await self._switch.wait()
        if self._state is not _State.PREPARE_POST:
            raise RuntimeError(f"expected {self.State.PREPARE_POST}, got {self._state}")

        self._switch.clear()

    async def wait_cleanup_post(self):
        await self._switch.wait()
        if self._state is not _State.CLEANUP_POST:
            raise RuntimeError(f"expected {self.State.CLEANUP_POST}, got {self._state}")

        self._switch.clear()

    async def wait_for_sigexit(self):
        await self._sigexit.wait()

    @property
    def ready(self):
        if self._ready is None:
            raise RuntimeError("ServiceContext.ready is not available outside of prepare context")

        return self._ready

    @property
    def should_exit(self):
        return self._sigexit.is_set()

    @asynccontextmanager
    async def prepare(self):
        self._state = _State.PREPARE_PRE
        self.switch()
        await self._notify.wait()
        self._notify.clear()
        yield
        self._state = _State.PREPARE_POST
        self.switch()
        await self._notify.wait()
        self._notify.clear()
        self._state = None

    @asynccontextmanager
    async def cleanup(self):
        self._state = _State.CLEANUP_PRE
        self.switch()
        await self._notify.wait()
        self._notify.clear()
        yield
        self._state = _State.CLEANUP_POST
        self.switch()
        await self._notify.wait()
        self._notify.clear()
        self._state = None