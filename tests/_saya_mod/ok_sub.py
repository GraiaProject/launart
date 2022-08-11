from graia.saya.channel import Channel

from launart.component import Launchable
from launart.saya import LaunchableSchema
from launart.service import ExportInterface, Service

c = Channel.current()


SayaTestInterface = type("SayaTestInterface", (ExportInterface,), {})


class SayaTestLaunchable(Launchable):
    id = "launchable.test.saya"

    @property
    def required(self):
        return set()

    @property
    def stages(self):
        return set()

    async def launch(self, _):
        ...


class SayaTestSrv(Service):
    supported_interface_types = {SayaTestInterface}
    id = "service.test.saya"

    @property
    def required(self):
        return set()

    @property
    def stages(self):
        return set()

    async def launch(self, _):
        ...

    def get_interface(self, interface_type):
        return interface_type()


c.use(LaunchableSchema())(SayaTestLaunchable())
c.use(LaunchableSchema())(SayaTestSrv())
