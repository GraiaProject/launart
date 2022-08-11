import pytest

from launart import Launart
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
