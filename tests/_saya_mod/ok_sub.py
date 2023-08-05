from graia.saya.channel import Channel

from launart.service import Service
from launart.saya import ServiceSchema

c = Channel.current()



class SayaTestService(Service):
    id = "lc.test.saya"

    @property
    def required(self):
        return set()

    @property
    def stages(self):
        return set()

    async def launch(self, _):
        ...


class SayaTestSrv(Service):
    id = "service.test.saya"

    @property
    def required(self):
        return set()

    @property
    def stages(self):
        return set()

    async def launch(self, _):
        ...


c.use(ServiceSchema())(SayaTestService())
c.use(ServiceSchema())(SayaTestSrv())
