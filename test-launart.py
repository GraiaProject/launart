from __future__ import annotations

import asyncio

from launart import Launart, Service


def test_launart():
    art = Launart()
    msg = []

    def push_msg(m):
        msg.append(m)

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
                push_msg("TestSrv: prepared TestInterface")
            async with self.stage("cleanup"):
                push_msg("TestSrv: cleanup TestInterface")

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
                push_msg("prepare 1")
                await asyncio.sleep(3)
            async with self.stage("blocking"):
                push_msg("blocking 1")
                await asyncio.sleep(3)
                push_msg("unblocking 1")
            async with self.stage("cleanup"):
                push_msg("cleanup 1")
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
                push_msg("prepare 2")

            async with self.stage("blocking"):
                push_msg("blocking 2")
                push_msg("test for sideload")
                await manager.add_sideload(TestSideload())
                await asyncio.sleep(1)
                await manager.get_component("test_sideload").status.wait_for_blocking()
                push_msg("sideload in blocking, test for active cleanup")
                await manager.remove_sideload("test_sideload")
                await asyncio.sleep(1)
                push_msg("unblocking 2")
                await asyncio.sleep(3)

            async with self.stage("cleanup"):
                push_msg("cleanup2")

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
                push_msg("prepare in sideload")
                await asyncio.sleep(3)
            async with self.stage("blocking"):
                push_msg("blocking in sideload")
                await asyncio.sleep(3)
                push_msg("unblocking in sideload")
            async with self.stage("cleanup"):
                push_msg("cleanup in sideload")
                await asyncio.sleep(3)

    art.add_component(TestSrv())
    art.add_component(TestService())
    art.add_component(Test2())
    assert list(art._initial_services.keys()) == ["test_srv", "test", "test2"]
    art.launch_blocking()

    assert msg == [
        "TestSrv: prepared TestInterface",
        "prepare 1",
        "prepare 2",
        "blocking 1",
        "blocking 2",
        "test for sideload",
        "prepare in sideload",
        "unblocking 1",
        "blocking in sideload",
        "sideload in blocking, test for active cleanup",
        "unblocking in sideload",
        "cleanup in sideload",
        "unblocking 2",
        "cleanup2",
        "cleanup 1",
        "TestSrv: cleanup TestInterface",
    ]
