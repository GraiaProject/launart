from graia.saya.channel import Channel

from launart.saya import LaunchableSchema

c = Channel.current()

c.use(LaunchableSchema())(3)
