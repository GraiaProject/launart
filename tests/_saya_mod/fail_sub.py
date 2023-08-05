from graia.saya.channel import Channel

from launart.saya import ServiceSchema

c = Channel.current()

c.use(ServiceSchema())(3)
