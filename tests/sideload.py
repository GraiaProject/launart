import asyncio

import pytest

from launart import Launart
from launart.component import Launchable


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
            async with self.stage("cleanup"):
                s_cleanup.set()

    class Sideload2(_Base):
        id = "worker2"

        async def launch(self, manager: Launart):
            async with self.stage("preparing"):
                ...
            async with self.stage("blocking"):
                s2.set()
            async with self.stage("cleanup"):
                s_cleanup.set()

    class AddSideload(_Base):
        id = "test_sideload_add"

        async def launch(self, manager: Launart):
            async with self.stage("preparing"):
                ...
            async with self.stage("blocking"):
                sideload = Sideload1()
                manager.add_launchable(Sideload1())
                await s1.wait()
                manager.add_launchable(Sideload2())
                await s2.wait()
            async with self.stage("cleanup"):
                manager.remove_launchable(sideload)

    mgr = Launart()
    mgr.add_launchable(AddSideload())
    await mgr.launch()
    assert s_cleanup.is_set()
