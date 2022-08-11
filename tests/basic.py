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
    wrong_launch = loop.create_task(mgr.launch())
    loop.run_until_complete(asyncio.sleep(0.1))
    mgr._on_sys_signal(None, None, launch_tsk)
    assert not launch_tsk.done()
    assert wrong_launch.done()
    loop.run_until_complete(launch_tsk)
    mgr._on_sys_signal(None, None, launch_tsk)


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
        id = "launchable.test.saya"

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
            async with self.stage("cleanup"):
                stage.append("lc cleanup")

    class TestSrv(Service):
        supported_interface_types = {TestInterface}
        id = "service.test.saya"

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
    await mgr.launch()
    assert stage == ["srv prepare", "lc prepare", "blocking", "blocking", "lc cleanup", "srv cleanup"]