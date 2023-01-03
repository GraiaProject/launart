import asyncio
from signal import SIGINT, default_int_handler, signal

import pytest

from launart import Launart
from launart.component import Launchable
from launart.service import ExportInterface, Service
from tests.fixture import EmptyLaunchable, component, interface, service


@pytest.mark.asyncio
async def test_nothing():
    mgr = Launart()
    lc = EmptyLaunchable()
    mgr.add_launchable(lc)
    await mgr.launch()
    assert lc.triggered


def test_nothing_blocking():
    mgr = Launart()
    lc = EmptyLaunchable()
    mgr.add_launchable(lc)
    mgr.launch_blocking()
    assert lc.triggered

    # set SIGINT and do again
    lc.triggered = False
    loop = asyncio.new_event_loop()
    tsk = loop.create_task(asyncio.sleep(2.0))
    tsk2 = loop.create_task(asyncio.sleep(0))
    signal(SIGINT, lambda *_: None)
    mgr.launch_blocking(loop=loop)
    assert tsk.cancelled()
    assert tsk.done()
    assert tsk2.done()
    assert lc.triggered
    signal(SIGINT, default_int_handler)


def test_nothing_complex():
    mgr = Launart()

    class _L(Launchable):
        id = "empty"
        triggered = False

        @property
        def stages(self):
            return {"blocking"}

        @property
        def required(self):
            return set()

        async def launch(self, _):
            async with self.stage("blocking"):
                await asyncio.sleep(0.2)

    lc = _L()
    mgr.add_launchable(lc)

    loop = asyncio.new_event_loop()
    launch_tsk = loop.create_task(mgr.launch())
    loop.run_until_complete(asyncio.sleep(0.02))  # head time
    assert mgr._get_task("empty")
    wrong_launch = loop.create_task(mgr.launch())
    loop.run_until_complete(asyncio.sleep(0.1))
    mgr._on_sys_signal(None, None, launch_tsk)
    assert not launch_tsk.done()
    assert wrong_launch.done()
    loop.run_until_complete(launch_tsk)
    mgr._on_sys_signal(None, None, launch_tsk)


def test_manager_stat():
    mgr = Launart()

    class _L(Launchable):
        id = "test_stat"

        @property
        def stages(self):
            return {"preparing", "blocking", "cleanup"}

        @property
        def required(self):
            return set()

        async def launch(self, _):
            await self.status.wait_for()  # test empty wait
            await asyncio.sleep(0.02)
            async with self.stage("preparing"):
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.02)
            assert self.status.prepared
            async with self.stage("blocking"):
                assert self.status.prepared
                assert self.status.blocking
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.02)
            async with self.stage("cleanup"):
                assert not self.status.prepared
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.02)

    mgr.add_launchable(_L())
    loop = asyncio.new_event_loop()
    mk_task = loop.create_task
    tasks = [mk_task(mgr.status.wait_for_preparing())]
    loop.run_until_complete(asyncio.sleep(0.01))
    mk_task(mgr.launch())
    tasks.append(mk_task(mgr.status.wait_for_blocking()))
    tasks.append(mk_task(mgr.status.wait_for_cleaning()))
    loop.run_until_complete(asyncio.sleep(0.02))
    tasks.append(mk_task(mgr.status.wait_for_sigexit()))
    tasks.append(mk_task(mgr.status.wait_for_finished()))
    loop.run_until_complete(asyncio.sleep(0.2))
    for task in tasks:
        assert task.done() and not task.cancelled()


@pytest.mark.asyncio
async def test_wait_for(event_loop: asyncio.AbstractEventLoop):
    loop = event_loop
    mgr = Launart()

    class _L(Launchable):
        id = "test_stat"

        @property
        def stages(self):
            return {"preparing", "blocking", "cleanup"}

        @property
        def required(self):
            return set()

        async def launch(self, _):
            await self.status.wait_for()  # test empty wait
            await asyncio.sleep(0.02)
            async with self.stage("preparing"):
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.02)
            assert self.status.prepared
            async with self.stage("blocking"):
                assert self.status.prepared
                assert self.status.blocking
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.02)
            async with self.stage("cleanup"):
                assert not self.status.prepared
                await asyncio.sleep(0.02)

    l = _L()
    with pytest.raises(RuntimeError):
        await l.wait_for("preparing", "test_stat")
    mgr.add_launchable(l)
    mk_task = loop.create_task
    mk_task(mgr.launch())
    await l.wait_for("finished", "test_stat")


def test_signal_change_during_running():
    mgr = Launart()

    class _L(Launchable):
        id = "empty"
        triggered = False

        @property
        def stages(self):
            return {"blocking"}

        @property
        def required(self):
            return set()

        async def launch(self, _):
            signal(SIGINT, lambda *_: None)

    lc = _L()
    mgr.add_launchable(lc)

    mgr.launch_blocking()


def test_bare_bone():
    mgr = Launart()
    lc = component("component.test", [])
    mgr.add_launchable(lc)
    assert mgr.launchables["component.test"] == lc
    i = interface()
    srv = service("service.test", {i}, [])
    mgr.add_launchable(srv)
    assert mgr.launchables["service.test"] == srv
    assert mgr.get_launchable("service.test") is srv
    assert mgr.get_service("service.test") is srv
    assert isinstance(mgr.get_interface(i), i)
    with pytest.raises(ValueError):
        mgr.get_interface(interface())
    with pytest.raises(TypeError):
        mgr.get_service("component.test")
    with pytest.raises(ValueError):
        mgr.add_launchable(lc)
    with pytest.raises(ValueError):
        mgr.get_launchable("$?")


@pytest.mark.asyncio
async def test_basic_components():
    TestInterface = type("SayaTestInterface", (ExportInterface,), {})

    stage = []

    class TestLaunchable(Launchable):
        id = "launchable.test"

        @property
        def required(self):
            return {TestInterface}

        @property
        def stages(self):
            return {"preparing", "blocking", "cleanup"}

        async def launch(self, _):
            async with self.stage("preparing"):
                stage.append("lc prepare")
            async with self.stage("blocking"):
                stage.append("blocking")
                assert isinstance(mgr._get_task("service.test"), asyncio.Task)
                assert mgr._get_task("nothing") is None
            async with self.stage("cleanup"):
                stage.append("lc cleanup")

    class TestSrv(Service):
        supported_interface_types = {TestInterface}
        id = "service.test"

        @property
        def required(self):
            return set()

        @property
        def stages(self):
            return {"preparing", "blocking", "cleanup"}

        async def launch(self, _):
            async with self.stage("preparing"):
                stage.append("srv prepare")
            async with self.stage("blocking"):
                stage.append("blocking")
            async with self.stage("cleanup"):
                stage.append("srv cleanup")

        def get_interface(self, interface_type):
            return interface_type()

    mgr = Launart()
    mgr.add_launchable(TestLaunchable())
    mgr.add_launchable(TestSrv())
    with pytest.raises(RuntimeError):
        mgr._get_task("service.test")
    await mgr.launch()
    assert stage == ["srv prepare", "lc prepare", "blocking", "blocking", "lc cleanup", "srv cleanup"]


def test_override_bind():
    TestInterface = type("SayaTestInterface", (ExportInterface,), {})

    class Srv1(Service):
        supported_interface_types = {TestInterface: 3}
        id = "service.test1"

        @property
        def required(self):
            return set()

        @property
        def stages(self):
            return {"preparing", "blocking", "cleanup"}

        async def launch(self, _):
            ...

        def get_interface(self, interface_type):
            return interface_type()

    class Srv2(Service):
        supported_interface_types = {TestInterface: 1}
        id = "service.test2"

        @property
        def required(self):
            return set()

        @property
        def stages(self):
            return {"preparing", "blocking", "cleanup"}

        async def launch(self, _):
            ...

        def get_interface(self, interface_type):
            return interface_type()

    mgr = Launart()
    srv1, srv2 = Srv1(), Srv2()
    mgr.add_launchable(srv1)
    assert mgr._service_bind[TestInterface] is srv1
    mgr.add_launchable(srv2)
    assert mgr._service_bind[TestInterface] is srv1
    mgr.override_bind(TestInterface, srv2)
    assert mgr._service_bind[TestInterface] is srv2
    with pytest.raises(ValueError):
        mgr.override_bind(TestInterface, srv1)


def test_graceful_abort():

    failure: bool = False

    class Malfunction(Launchable):
        id = "malfunction"

        @property
        def required(self):
            return set()

        @property
        def stages(self):
            return {"preparing"}

        async def launch(self, _):
            async with self.stage("preparing"):
                raise ValueError

    class Dependent(Launchable):
        id = "dependent"

        @property
        def required(self):
            return {"malfunction"}

        @property
        def stages(self):
            return {"preparing", "blocking"}

        async def launch(self, _):
            async with self.stage("preparing"):
                ...
            async with self.stage("blocking"):
                nonlocal failure
                failure = True

    mgr = Launart()
    mgr.add_launchable(Malfunction())
    mgr.add_launchable(Dependent())
    mgr.launch_blocking()

    if failure:
        pytest.fail("Error: dependent reached blocking stage while dependency failed to prepare.")
