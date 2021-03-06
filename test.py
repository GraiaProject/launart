# import richuru
# richuru.install()

import asyncio

from launart import Launart, Launchable

art = Launart()


class TestLaunchable(Launchable):
    id = "test"

    @property
    def required(self) -> set[str]:
        return set()

    @property
    def stages(self) -> set[str]:
        return {"preparing", "blocking", "cleanup"}

    async def launch(self, manager: Launart):
        async with self.stage("preparing"):
            print("prepare")
            await asyncio.sleep(3)
        async with self.stage("blocking"):
            print("blocking")
            await asyncio.sleep(3)
            print("unblocking 1")
        async with self.stage("cleanup"):
            print("cleanup")
            await asyncio.sleep(3)


class Test2(Launchable):
    id = "test2"

    @property
    def required(self) -> set[str]:
        return {"test"}

    @property
    def stages(self) -> set[str]:
        return {"preparing", "blocking", "cleanup"}

    async def launch(self, manager: Launart):
        async with self.stage("preparing"):
            print("prepare2")

        async with self.stage("blocking"):
            print("blocking")
            print("test for sideload")
            manager.add_launchable(TestSideload())
            await asyncio.sleep(3)
            print("unblocking 2")
            # await asyncio.sleep(1)
            await manager.launchables["test_sideload"].status.wait_for("blocking")
            print("sideload in blocking, test for active cleanup")
            manager.remove_launchable("test_sideload")
            await asyncio.sleep(10)

        async with self.stage("cleanup"):
            print("cleanup2")


class TestSideload(Launchable):
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


art.add_launchable(TestLaunchable())
art.add_launchable(Test2())

art.launch_blocking()
