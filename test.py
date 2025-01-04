from __future__ import annotations

import asyncio

from launart import Launart, Service
from _bootstrap import Stage, Phase

art = Launart()


class TestSrv(Service):
    id = "test_srv"

    @property
    def required(self) -> set[str]:
        return set()

    @property
    def stages(self) -> set[str]:
        return {"preparing", "cleanup"}

    async def launch(self, manager: Launart):
        async with self.stage("preparing"):
            print("TestSrv: prepared TestInterface")
        async with self.stage("cleanup"):
            print("TestSrv: cleanup TestInterface")


class TestService(Service):
    id = "test"

    @property
    def required(self):
        return {"test_srv"}

    @property
    def stages(self) -> set[str]:
        return {"preparing", "blocking", "cleanup"}

    async def launch(self, manager: Launart):
        async with self.stage("preparing"):
            print("prepare 1")
            await asyncio.sleep(3)
        async with self.stage("blocking"):
            print("blocking 1")
            await asyncio.sleep(3)
            print("unblocking 1")
        async with self.stage("cleanup"):
            print("cleanup 1")
            await asyncio.sleep(3)


class Test2(Service):
    id = "test2"

    @property
    def required(self) -> set[str]:
        return {"test"}

    @property
    def stages(self) -> set[str]:
        return {"preparing", "blocking", "cleanup"}

    async def launch(self, manager: Launart):
        async with self.stage("preparing"):
            print("prepare 2")

        async with self.stage("blocking"):
            print("blocking 2")
            print("test for sideload")
            await manager.add_sideload(TestSideload())
            await asyncio.sleep(3)
            print("unblocking 2")
            await asyncio.sleep(1)
            await manager._core.contexts["test_sideload"].wait_for(Stage.ONLINE, Phase.PENDING)
            # await manager.components["test_sideload"].status.wait_for("blocking")
            print("sideload in blocking, test for active cleanup")
            await manager.remove_sideload("test_sideload")
            await asyncio.sleep(5)

        async with self.stage("cleanup"):
            print("cleanup2")


class TestSideload(Service):
    id = "test_sideload"

    @property
    def required(self) -> set[str]:
        return set()

    @property
    def stages(self) -> set[str]:
        return {"preparing", "blocking", "cleanup"}

    async def launch(self, manager: Launart):
        async with self.stage("preparing"):
            print("prepare in sideload")
            await asyncio.sleep(3)
        async with self.stage("blocking"):
            print("blocking in sideload")
            await asyncio.sleep(3)
            print("unblocking in sideload")
            # print(manager.taskgroup.blocking_task)
        async with self.stage("cleanup"):
            print("cleanup in sideload")
            await asyncio.sleep(3)


art.add_component(TestSrv())
art.add_component(TestService())
art.add_component(Test2())

art.launch_blocking()
