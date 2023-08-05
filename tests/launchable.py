import asyncio

import pytest

from launart import Launart
from launart.component import Service, ServiceStatus
from tests.fixture import EmptyService

def test_ensure():
    lc = EmptyService()
    mgr = Launart()
    mgr.add_component(lc)
    assert lc.manager is mgr
    with pytest.raises(RuntimeError):
        lc.ensure_manager(Launart())


def test_service_stat_transition_raw():
    stat = ServiceStatus()
    stat.stage = "blocking"
    assert stat.stage == "blocking"
    with pytest.raises(ValueError):
        stat.stage = "preparing"  # rollback is not allowed
    stat.stage = "blocking-completed"
    stat.stage = "finished"
    stat.unset()


def test_service_stat_transition_base_err_report():
    class _Base(Service):
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


@pytest.mark.asyncio
async def test_service_stat_transition_err():
    class _Base(Service):
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
        mgr.add_component(e)
    await mgr.launch()
