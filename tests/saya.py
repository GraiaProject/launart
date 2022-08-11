import pytest
from graia.saya import Saya
from graia.saya.behaviour.entity import Behaviour
from graia.saya.schema import BaseSchema

from launart.manager import Launart
from launart.saya import LaunartBehaviour


class EmptyBehaviour(Behaviour):
    def allocate(self, cube):
        if cube.metaclass.__class__ is BaseSchema:
            return True

    def release(self, cube):
        if cube.metaclass.__class__ is BaseSchema:
            return True


def test_saya():
    saya = Saya()
    mgr = Launart()
    saya.install_behaviours(LaunartBehaviour(mgr), EmptyBehaviour())

    empty_mod = saya.require("tests._saya_mod.empty_sub")

    with pytest.raises(TypeError):
        saya.require("tests._saya_mod.fail_sub")

    channel = saya.require("tests._saya_mod.ok_sub")
    assert "launchable.test.saya" in mgr.launchables
    assert "service.test.saya" in mgr.launchables
    saya.uninstall_channel(channel)
    assert "launchable.test.saya" not in mgr.launchables
    assert "service.test.saya" not in mgr.launchables

    saya.uninstall_channel(empty_mod)
