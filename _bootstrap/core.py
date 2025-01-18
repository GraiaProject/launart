from __future__ import annotations

import asyncio
import signal
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Iterable

from exceptiongroup import ExceptionGroup  # noqa: A004
from loguru import logger

from .context import ServiceContext
from .graph import ServiceGraph
from .utiles import TaskGroup, cvar, oneof, cancel_alive_tasks


if TYPE_CHECKING:
    from .service import Service


class UnhandledExit(Exception):
    pass


BOOTSTRAP_CONTEXT: ContextVar[Bootstrap] = ContextVar("BOOTSTRAP_CONTEXT")


class Bootstrap:
    graph: ServiceGraph

    def __init__(self):
        self.graph = ServiceGraph()

    async def spawn(self, *services: Service):
        service_bind, previous, nexts = self.graph.subgraph(*services)
        tasks: dict[str, asyncio.Task] = self.graph.tasks

        prepare_errors: list[Exception] = []
        cleanup_errors: list[Exception] = []

        done_prepare: dict[str, None] = {}
        pending_prepare = TaskGroup()
        pending_cleanup = TaskGroup()
        queued_prepare = {k: v.copy() for k, v in previous.maps[0].items()}
        queued_cleanup = {k: v.copy() for k, v in nexts.maps[0].items()}

        spawn_forward_prepare: bool = True

        def spawn_prepare(service: Service):
            async def prepare_guard():
                nonlocal spawn_forward_prepare

                context = ServiceContext(self)
                self.graph.contexts[service.id] = context
                task = tasks[service.id] = asyncio.create_task(service.launch(context))

                await oneof(context.wait_prepare_pre(), task)
                await oneof(context.wait_prepare_post(), task)

                if task.done():
                    spawn_forward_prepare = False

                    prepare_errors.append(task.exception() or UnhandledExit())  # type: ignore
                    self.graph.drop(service)
                    return

                done_prepare[service.id] = None

                if not spawn_forward_prepare:
                    return

                for next_service, barriers in list(queued_prepare.items()):
                    if service.id in barriers:
                        barriers.pop(service.id)

                        if not barriers:
                            spawn_prepare(service_bind[next_service])
                            queued_prepare.pop(next_service)

            pending_prepare.spawn(prepare_guard())

        def spawn_cleanup(service: Service):
            async def cleanup_guard():
                context = self.graph.contexts[service.id]
                task = tasks[service.id]

                context.exit()
                await oneof(context.wait_cleanup_pre(), task)
                await oneof(context.wait_cleanup_post(), task)

                self.graph.drop(service)

                if task.done():
                    cleanup_errors.append(task.exception() or UnhandledExit())  # type: ignore
                    return

                for previous_service, barriers in list(queued_cleanup.items()):
                    if service.id in barriers:
                        barriers.pop(service.id)

                        if not barriers:
                            spawn_cleanup(service_bind[previous_service])
                            queued_cleanup.pop(previous_service)

            pending_cleanup.spawn(cleanup_guard())

        def toggle_enter():
            for i in done_prepare:
                self.graph.contexts[i].enter()

        def toggle_skip():
            for i in done_prepare:
                self.graph.contexts[i].skip()

        def rollback():
            spawned = False

            for i in done_prepare:
                if not (nexts[i] & done_prepare.keys()):
                    spawned = True
                    spawn_cleanup(service_bind[i])

            if not spawned:
                raise RuntimeError("Unsatisfied dependencies, rollback failed")

            return pending_cleanup.wait()

        for i, v in previous.maps[0].items():
            if not v:
                spawn_prepare(service_bind[i])
                queued_prepare.pop(i)

        await pending_prepare

        if queued_prepare:
            toggle_skip()
            await rollback()

            if cleanup_errors:
                raise RuntimeError("Unsatisfied dependencies") from ExceptionGroup("", cleanup_errors)

            raise RuntimeError("Unsatisfied dependencies")

        if prepare_errors:
            toggle_skip()
            await rollback()

            if cleanup_errors:
                raise ExceptionGroup("", cleanup_errors) from ExceptionGroup("", prepare_errors)

            raise ExceptionGroup("", prepare_errors)

        self.graph.apply(dict(service_bind), previous, nexts)
        toggle_enter()

        return rollback

    async def launch(self, *services: Service):
        rollback = await self.spawn(*services)
        try:
            await asyncio.gather(*[self.graph.contexts[i.id]._switch.wait() for i in services])
        except asyncio.CancelledError:
            pass
        finally:
            await rollback()

    def launch_blocking(
        self,
        *services: Service,
        loop: asyncio.AbstractEventLoop | None = None,
        stop_signal: Iterable[signal.Signals] = (signal.SIGINT,),
    ):
        import contextlib
        import threading

        loop = asyncio.new_event_loop()

        logger.info("Starting launart main task...", style="green bold")

        with cvar(BOOTSTRAP_CONTEXT, self):
            launch_task = loop.create_task(self.launch(*services), name="amnesia-launch")

        handled_signals: dict[signal.Signals, Any] = {}

        def signal_handler(*args, **kwargs):  # noqa: ARG001
            for service in self.graph.services:
                self.graph.contexts[service].exit()

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