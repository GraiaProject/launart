from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Literal, Optional, Set, ClassVar


from _bootstrap.service import Service as BaseService
from _bootstrap.context import ServiceContext
from _bootstrap.status import Stage

from .util import override
from .status import ManagerStatus

if TYPE_CHECKING:
    from launart.manager import Launart


class Service(metaclass=ABCMeta):
    id: str
    manager: Optional[Launart] = None
    _context: Optional[ServiceContext] = None

    @property
    @abstractmethod
    def required(self) -> Set[str]:
        ...

    def ensure_manager(self, manager: Launart):
        if self.manager is not None and self.manager is not manager:
            raise RuntimeError("this component attempted to be mistaken a wrong ownership of launart/manager.")
        self.manager = manager

    def _ensure_context(self, context: ServiceContext):
        if self._context is not None and self._context is not context:
            raise RuntimeError("this component attempted to be mistaken a wrong context.")
        self._context = context

    def stage(self, stage: Literal["preparing", "blocking", "cleanup"]):
        if self._context is None:
            raise RuntimeError("attempted to set stage of a component without a context.")
        # if self._context._status[0] is Stage.EXIT:
        #    raise LookupError("attempted to set stage of a component without a current context")
        if stage not in {"preparing", "blocking", "cleanup"}:
            raise ValueError(f"undefined and unexpected stage entering: {stage}")
        ctx = self._context
        if stage == "preparing":
            return ctx.prepare()
        elif stage == "blocking":
            return ctx.online()
        elif stage == "cleanup":
            return ctx.cleanup()
        else:
            raise ValueError(f"entering unexpected stage: {stage}(unknown definition)")

    async def launch(self, manager: Launart):
        pass


def make_service(serv: Service) -> BaseService:
    from launart.manager import Launart

    class _Service(BaseService):
        id = serv.id
        __launart_service__: ClassVar[Service] = serv

        @property
        def dependencies(self):
            return tuple(serv.required)

        async def launch(self, context: ServiceContext):
            serv._ensure_context(context)
            manager = Launart.current()
            await serv.launch(override(manager, {"status": ManagerStatus(context)}))

    b_s = type(serv.__class__.__name__, (_Service,), {})()
    return b_s
