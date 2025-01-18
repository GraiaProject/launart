from __future__ import annotations

import asyncio
import signal
from contextvars import ContextVar
from collections.abc import Coroutine
from typing import Any, ClassVar, Callable, Iterable, TypeVar, cast, overload

from loguru import logger

from _bootstrap import Bootstrap
from _bootstrap import Service as _Service
from _bootstrap.utiles import cancel_alive_tasks, cvar
from launart.service import Service, make_service

from .status import Status as ManagerStatus

T = TypeVar("T")
TL = TypeVar("TL", bound=Service)


class Launart:
    status: ManagerStatus
    _context: ClassVar[ContextVar[Launart]] = ContextVar("launart._context")

    def __init__(self):
        self._core = Bootstrap()
        self._rollbacks: dict[str, Callable[[], Coroutine[Any, Any, None]]] = {}
        self._running = False
        self._default_isolate = {"interface_provide": {}}
        self._initial_services: dict[str, Service] = {}

    @classmethod
    def current(cls) -> Launart:
        return cls._context.get()

    def export_interface(self, interface: type, service: Service):
        self._default_isolate["interface_provide"][interface] = service

    def add_component(self, component: Service):
        if not self._running:
            self._initial_services[component.id] = component
            return

        _t = asyncio.create_task(self._core.spawn(make_service(component)))
        _t.add_done_callback(lambda _: self._rollbacks.update({component.id: _t.result()}))

    async def add_sideload(self, component: Service):
        if not self._running:
            raise ValueError("Cannot add a service while the launart is not running.")
        self._rollbacks[component.id] = await self._core.spawn(make_service(component))

    @overload
    def get_component(self, target: type[TL]) -> TL:
        ...

    @overload
    def get_component(self, target: str) -> Service:
        ...

    def get_component(self, target: str | type[TL]) -> TL | Service:
        try:
            _id = target if isinstance(target, str) else target.id
            _serv = self._core.graph.services[_id]
        except KeyError:
            raise ValueError(f"Service {target} does not exists.") from None
        if not hasattr(_serv, "__launart_service__"):
            raise ValueError(f"Service {target} does not exists.")
        return cast("TL | Service", _serv.__launart_service__)  # type: ignore

    def remove_component(
        self,
        component: str | Service,
    ):
        serv_id = component if isinstance(component, str) else component.id
        if not self._running:
            if serv_id not in self._initial_services:
                raise ValueError(f"Service {serv_id} does not exists.")
            self._initial_services.pop(serv_id)
            return
        if serv_id not in self._rollbacks:
            raise ValueError(f"Service {serv_id} cannot be removed.")
        rollback = self._rollbacks.pop(serv_id)
        asyncio.create_task(rollback())

    async def remove_sideload(self, component: str | Service):
        serv_id = component if isinstance(component, str) else component.id
        if serv_id not in self._rollbacks:
            raise ValueError(f"Service {serv_id} cannot be removed.")
        rollback = self._rollbacks.pop(serv_id)
        await rollback()

    def get_interface(self, interface_type: type[T]) -> T:
        provider_map = self._default_isolate["interface_provide"]
        service = provider_map.get(interface_type)
        if service is None:
            raise ValueError(f"{interface_type} is not supported.")
        return service.get_interface(interface_type)

    async def launch(self):
        self._running = True
        srvs = [make_service(s) for s in self._initial_services.values()]
        with cvar(self._context, self):
            await self._core.launch(*srvs)
        self._running = False
        return

    def launch_blocking(
        self,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        stop_signal: Iterable[signal.Signals] = (signal.SIGINT,),
    ):
        import contextlib
        import threading

        from creart import it

        if loop is not None:  # pragma: no cover
            from warnings import warn

            warn(
                "The loop argument is deprecated since launart 0.6.4, " "and scheduled for removal in launart 0.7.0.",
                DeprecationWarning,
                stacklevel=2,
            )

        loop = it(asyncio.AbstractEventLoop)

        logger.info("Starting launart main task...", style="green bold")

        launch_task = loop.create_task(self.launch(), name="amnesia-launch")
        handled_signals: dict[signal.Signals, Any] = {}

        def signal_handler(*_):
            for service in self._core.graph.services:
                self._core.graph.contexts[service].exit()

            if not launch_task.done():
                launch_task.cancel()
                # wakeup loop if it is blocked by select() with long timeout
                launch_task.get_loop().call_soon_threadsafe(lambda: None)
                logger.warning("Ctrl-C triggered by user.", style="dark_orange bold")

        if threading.current_thread() is threading.main_thread():  # pragma: worst case
            try:
                for sig in stop_signal:
                    handled_signals[sig] = signal.getsignal(sig)
                    signal.signal(sig, signal_handler)
            except ValueError:  # pragma: no cover
                # `signal.signal` may throw if `threading.main_thread` does
                # not support signals
                handled_signals.clear()

        loop.run_until_complete(launch_task)

        for sig, handler in handled_signals.items():
            if signal.getsignal(sig) is signal_handler:
                signal.signal(sig, handler)

        try:
            cancel_alive_tasks(loop)
            loop.run_until_complete(loop.shutdown_asyncgens())
            with contextlib.suppress(RuntimeError, AttributeError):
                # LINK: https://docs.python.org/3.10/library/asyncio-eventloop.html#asyncio.loop.shutdown_default_executor
                loop.run_until_complete(loop.shutdown_default_executor())  # type: ignore
        finally:
            asyncio.set_event_loop(None)
            logger.success("asyncio shutdown complete.", style="green bold")
