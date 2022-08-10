import pytest

from launart import Launart
from tests.fixture import EmptyLaunchable


def test_ensure():
    lc = EmptyLaunchable()
    mgr = Launart()
    mgr.add_launchable(lc)
    assert lc.manager is mgr
    with pytest.raises(RuntimeError):
        lc.ensure_manager(Launart())
