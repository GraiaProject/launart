from graia.saya import Channel
from graia.saya.schema import BaseSchema

c = Channel.current()

c.use(BaseSchema())(1234)
