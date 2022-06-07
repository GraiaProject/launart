#import richuru
#richuru.install()

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
            await asyncio.sleep(3)
            print("unblocking 2")

        async with self.stage("cleanup"):
            print("cleanup2")

art.add_launchable(TestLaunchable())
art.add_launchable(Test2())

art.launch_blocking()