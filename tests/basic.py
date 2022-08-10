import pytest

from launart import Launart
from tests.fixture import EmptyLaunchable, component, interface, service


@pytest.mark.asyncio
async def test_launch_nothing():
    mgr = Launart()
    lc = EmptyLaunchable()
    mgr.add_launchable(lc)
    await mgr.launch()
    assert lc.triggered


def test_add():
    mgr = Launart()
    lc = component("component.test", [])
    mgr.add_launchable(lc)
    assert mgr.launchables["component.test"] == lc
    i = interface()
    srv = service("service.test", {i}, [])
    mgr.add_launchable(srv)
    assert mgr.launchables["service.test"] == srv
