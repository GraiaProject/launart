from __future__ import annotations

import asyncio
import signal
from contextvars import ContextVar
from typing import Any, ClassVar, Iterable, TypeVar, cast, overload

from loguru import logger

from launart.service import Service, make_service

from _bootstrap import Bootstrap
from _bootstrap import Service as _Service
from _bootstrap.utiles import cvar, cancel_alive_tasks

from .status import ManagerStatus

T = TypeVar("T")
TL = TypeVar("TL", bound=Service)


class Launart:
    status: ManagerStatus
    _context: ClassVar[ContextVar[Launart]] = ContextVar("launart._context")

    def __init__(self):
        self._core = Bootstrap()
        self._default_isolate = {
            "interface_provide": {}
        }

    @classmethod
    def current(cls) -> Launart:
        return cls._context.get()

    def export_interface(self, interface: type, service: Service):
        self._default_isolate['interface_provide'][interface] = service

    def add_component(self, component: Service):
        if not self._core.running:
            return self._core.add_initial_services(make_service(component))
        # TODO: add service during running.

    @overload
    def get_component(self, target: type[TL]) -> TL:
        ...

    @overload
    def get_component(self, target: str) -> Service:
        ...

    def get_component(self, target: str | type[TL]) -> TL | Service:
        try:
            if isinstance(target, str):
                _serv = self._core.get_service(target)
            else:
                _serv = self._core.get_service(target.id)
        except KeyError:
            raise ValueError(f"Service {target} does not exists.") from None
        if not hasattr(_serv, "__launart_service__"):
            raise ValueError(f"Service {target} does not exists.")
        return cast("TL | Service", _serv.__launart_service__)  # type: ignore

    def remove_component(
        self,
        component: str | Service,
    ):
        if not self._core.running:
            if isinstance(component, str):
                serv_id = component
            else:
                serv_id = component.id
            if serv_id not in self._core.initial_services:
                raise ValueError(f"Service {serv_id} does not exists.")
            self._core.initial_services.pop(serv_id)
            return
        # TODO: remove service during running.

    def get_interface(self, interface_type: type[T]) -> T:
        provider_map = self._default_isolate['interface_provide']
        service = provider_map.get(interface_type)
        if service is None:
            raise ValueError(f"{interface_type} is not supported.")
        return service.get_interface(interface_type)

    async def launch(self):
        with cvar(self._context, self):
            return await self._core.launch()

    def launch_blocking(
        self,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        stop_signal: Iterable[signal.Signals] = (signal.SIGINT,),
    ):
        import contextlib
        import threading

        loop = asyncio.new_event_loop()

        logger.info("Starting launart main task...", style="green bold")

        launch_task = loop.create_task(self.launch(), name="amnesia-launch")
        handled_signals: dict[signal.Signals, Any] = {}

        def signal_handler(*_):
            return self._on_sys_signal(launch_task)

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

    def _sigexit_trig(self, services: Iterable[_Service]):
        for service in services:
            self._core.contexts[service.id].exit()

    def _on_sys_signal(self, launch_task: asyncio.Task):
        self._sigexit_trig(self._core.services.values())

        if self._core.task_group is not None:
            self._core.task_group.stop()
            if self._core.task_group.main is not None:  # pragma: worst case
                self._core.task_group.main.cancel()

        if not launch_task.done():
            launch_task.cancel()
            # wakeup loop if it is blocked by select() with long timeout
            launch_task.get_loop().call_soon_threadsafe(lambda: None)
            logger.warning("Ctrl-C triggered by user.", style="dark_orange bold")
