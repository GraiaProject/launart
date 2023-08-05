from __future__ import annotations

import asyncio

import pytest

from launart import Launart
from launart.component import Launchable
from tests.fixture import component


class _Base(Launchable):
    @property
    def required(self):
        return set()

    @property
    def stages(self):
        return {"preparing", "blocking", "cleanup"}


@pytest.mark.asyncio
async def test_sideload_base():
    s1 = asyncio.Event()
    s2 = asyncio.Event()
    s_cleanup = asyncio.Event()

    class Sideload1(_Base):
        id = "worker1"

        async def launch(self, manager: Launart):
            async with self.stage("preparing"):
                ...
            async with self.stage("blocking"):
                s1.set()
                await s_cleanup.wait()
            async with self.stage("cleanup"):
                pass

    class Sideload2(_Base):
        id = "worker2"

        async def launch(self, manager: Launart):
            async with self.stage("preparing"):
                ...
            async with self.stage("blocking"):
                s2.set()
                await s_cleanup.wait()
            async with self.stage("cleanup"):
                ...

    class AddSideload(_Base):
        id = "test_sideload_add"

        async def launch(self, manager: Launart):
            async with self.stage("preparing"):
                ...
            async with self.stage("blocking"):
                manager.add_launchable(Sideload1())
                manager.add_launchable(component("worker_none", []))
                with pytest.raises(RuntimeError):
                    manager.add_launchable(component("worker_weird_req", ["$"]))
                with pytest.raises(RuntimeError):
                    manager.remove_launchable("worker1")  # wrong status
                with pytest.raises(RuntimeError):
                    manager.remove_launchable("test_sideload_add")  # removing prohibited
                await s1.wait()
                manager.add_launchable(Sideload2())
                await s2.wait()
                manager.remove_launchable("worker1")
                s_cleanup.set()
            async with self.stage("cleanup"):
                with pytest.raises(ValueError):
                    manager.remove_launchable("worker-any")
                manager.remove_launchable("worker2")

    mgr = Launart()
    mgr.add_launchable(AddSideload())
    await mgr.launch()
