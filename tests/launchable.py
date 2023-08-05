from __future__ import annotations

import asyncio

import pytest

from launart import Launart
from launart.component import Launchable, LaunchableStatus
from tests.fixture import EmptyLaunchable, component, interface, service


def test_ensure():
    lc = EmptyLaunchable()
    mgr = Launart()
    mgr.add_launchable(lc)
    assert lc.manager is mgr
    with pytest.raises(RuntimeError):
        lc.ensure_manager(Launart())


def test_resolve_req():
    i = interface()
    s = service("srv", {i}, [])
    a = component("a", ["b", i])
    b = component("b", [])
    mgr = Launart()
    with pytest.raises(RuntimeError):
        a._required_id
    mgr.add_launchable(a)
    with pytest.raises(RuntimeError):
        a._required_id
    mgr.add_launchable(s)
    with pytest.raises(RuntimeError):
        a._required_id
    mgr.add_launchable(b)
    assert a._required_id == {"srv", "b"}


def test_launchable_stat_transition_raw():
    stat = LaunchableStatus()
    stat.stage = "blocking"
    assert stat.stage == "blocking"
    with pytest.raises(ValueError):
        stat.stage = "preparing"  # rollback is not allowed
    stat.stage = "blocking-completed"
    stat.stage = "finished"
    stat.unset()


def test_launchable_stat_transition_base_err_report():
    class _Base(Launchable):
        @property
        def required(self):
            return set()

        @property
        def stages(self):
            return {"preparing"}

    class ErrNoMgr(_Base):
        id = "e1"

        async def launch(self, _):
            async with self.stage("preparing"):
                ...

    with pytest.raises(RuntimeError):
        asyncio.run(ErrNoMgr().launch(None))
    with pytest.raises(LookupError):
        mgr = Launart()
        e = ErrNoMgr()
        assert not e.manager
        e.ensure_manager(mgr)
        asyncio.run(e.launch(None))

    class ErrUnexpectedStage(_Base):
        id = "e2"

        async def launch(self, _):
            async with self.stage("blocking"):  # oops
                ...

    with pytest.raises(ValueError):
        mgr = Launart()
        e = ErrUnexpectedStage()
        e.ensure_manager(mgr)
        mgr.status.stage = "preparing"
        asyncio.run(e.launch(None))

    class ErrUnknownStageDef(_Base):
        id = "e2"

        @property
        def stages(self):
            return {"finished"}

        async def launch(self, _):
            async with self.stage("finished"):  # oops
                ...

    with pytest.raises(ValueError):
        mgr = Launart()
        e = ErrUnknownStageDef()
        e.ensure_manager(mgr)
        mgr.status.stage = "preparing"
        asyncio.run(e.launch(None))


def test_launchable_stat_transition_err():
    class _Base(Launchable):
        @property
        def required(self):
            return set()

        @property
        def stages(self):
            return {"preparing", "blocking", "cleanup"}

    class ErrToPrepare(_Base):
        id = "e1"

        async def launch(self, _):
            async with self.stage("preparing"):
                ...
            async with self.stage("blocking"):
                ...
            async with self.stage("cleanup"):
                ...
            with pytest.raises(ValueError):
                async with self.stage("preparing"):
                    ...

    class ErrToBlocking(_Base):
        id = "e2"

        async def launch(self, _):
            async with self.stage("preparing"):
                ...
            async with self.stage("blocking"):
                ...
            async with self.stage("cleanup"):
                ...
            with pytest.raises(ValueError):
                async with self.stage("blocking"):
                    ...

    class ErrToCleanup(_Base):
        id = "e3"

        async def launch(self, _):
            async with self.stage("preparing"):
                ...
            async with self.stage("blocking"):
                ...
            async with self.stage("cleanup"):
                ...
            with pytest.raises(ValueError):
                async with self.stage("cleanup"):
                    ...

    mgr = Launart()

    for e in [ErrToPrepare(), ErrToBlocking(), ErrToCleanup()]:
        mgr.add_launchable(e)
    mgr.launch_blocking()
