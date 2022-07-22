from __future__ import annotations

import asyncio
from contextvars import ContextVar
from functools import partial
from typing import TYPE_CHECKING, ClassVar, Dict, Literal, Optional, Type, cast

from loguru import logger
from statv import Stats, Statv

from launart._sideload import FutureMark, override
from launart.component import Launchable, resolve_requirements
from launart.service import ExportInterface, Service, TInterface
from launart.utilles import FlexibleTaskGroup, priority_strategy

U_ManagerStage = Literal["preparing", "blocking", "cleaning", "finished"]


def _launchable_task_done_callback(mgr: "Launart", t: asyncio.Task):
    exc = t.exception()
    if exc:
        logger.opt(exception=exc).error(
            f"[{t.get_name()}] raised a exception.",
            alt=f"[red bold]component [magenta]{t.get_name()}[/] raised an exception.",
        )
        return

    component = mgr.get_launchable(t.get_name())
    if mgr.status.preparing:
        if "preparing" in component.stages:
            if component.status.prepared:
                logger.info(f"Component {t.get_name()} completed preparation.")
            else:
                logger.error(f"Component {t.get_name()} exited before preparation.")
    elif mgr.status.blocking:
        if "cleanup" in component.stages and component.status.stage != "finished":
            logger.warning(f"Component {t.get_name()} exited without cleanup.")
        else:
            logger.success(f"Component {t.get_name()} finished.")
    elif mgr.status.cleaning:
        if "cleanup" in component.stages:
            if component.status.finished:
                logger.success(f"component {t.get_name()} finished.")
            else:
                logger.warning(f"component {t.get_name()} exited before completing cleanup.")

    logger.info(
        f"[{t.get_name()}] completed.",
        alt=rf"\[[magenta]{t.get_name()}[/magenta]] completed.",
    )


class ManagerStatus(Statv):
    stage = Stats[Optional[U_ManagerStage]]("U_ManagerStage", default=None)
    exiting = Stats[bool]("exiting", default=False)

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return f"<ManagerStatus stage={self.stage} waiters={len(self._waiters)}>"

    @property
    def preparing(self) -> bool:
        return self.stage == "preparing"

    @property
    def blocking(self) -> bool:
        return self.stage == "blocking"

    @property
    def cleaning(self) -> bool:
        return self.stage == "cleaning"

    async def wait_for_update(self, *, current: str | None = None, stage: U_ManagerStage | None = None):
        waiter = asyncio.Future()
        if current is not None:
            waiter.add_done_callback(FutureMark(current, stage))
        self._waiters.append(waiter)
        try:
            return await waiter
        finally:
            self._waiters.remove(waiter)

    async def wait_for_preparing(self):
        while self.stage != "preparing":
            await self.wait_for_update()

    async def wait_for_blocking(self):
        while self.stage != "blocking":
            await self.wait_for_update()

    async def wait_for_cleaning(self, *, current: str | None = None):
        while self.stage != "cleaning":
            await self.wait_for_update(current=current, stage="cleaning")

    async def wait_for_finished(self, *, current: str | None = None):
        while self.stage != "finished":
            await self.wait_for_update(current=current, stage="finished")

    async def wait_for_sigexit(self):
        while self.stage in {"preparing", "blocking"} and not self.exiting:
            await self.wait_for_update()


class Launart:
    launchables: Dict[str, Launchable]
    status: ManagerStatus
    tasks: dict[str, asyncio.Task]
    task_group: Optional[FlexibleTaskGroup] = None

    _service_bind: Dict[Type[ExportInterface], Service]

    _context: ClassVar[ContextVar[Launart]] = ContextVar("launart._context")

    def __init__(self):
        self.launchables = {}
        self._service_bind = {}
        self.tasks = {}
        self.status = ManagerStatus()

    @classmethod
    def current(cls) -> Launart:
        return cls._context.get()

    def add_launchable(self, launchable: Launchable):
        if launchable.id in self.launchables:
            raise ValueError(f"Launchable {launchable.id} already exists.")
        self.launchables[launchable.id] = launchable
        if self.task_group is not None:
            assert self.task_group.blocking_task is not None
            loop = self.task_group.blocking_task.get_loop()
            loop.create_task(self._sideload_prepare(launchable))

    def get_launchable(self, id: str):
        if id not in self.launchables:
            raise ValueError(f"Launchable {id} does not exists.")
        return self.launchables[id]

    def remove_launchable(self, id: str, *, unsafe: bool = False):
        if id not in self.launchables:
            raise ValueError(f"Launchable {id} does not exists.")
        if self.task_group is not None:
            assert self.task_group.blocking_task is not None
            loop = self.task_group.blocking_task.get_loop()

            launchable = self.launchables[id]

            if launchable.status.stage not in {"prepared", "blocking", "blocking-completed", "waiting-for-cleanup"}:
                raise RuntimeError(
                    f"{launchable.id} obtains invalid stats to sideload active release, it's {launchable.status.stage}"
                )

            # check requirements status
            if not unsafe:
                layers = resolve_requirements(self.launchables.values())
                if not layers or launchable not in layers[0]:
                    raise RuntimeError

            loop.create_task(self._sideload_cleanup(launchable))
        else:
            del self.launchables[id]

    def _update_service_bind(self):
        self._service_bind = priority_strategy(
            [i for i in self.launchables.values() if isinstance(i, Service)],
            lambda a: a.supported_interface_types,
        )

    def get_service(self, id: str) -> Service:
        launchable = self.get_launchable(id)
        if not isinstance(launchable, Service):
            raise ValueError(f"{id} is not a service.")
        return launchable

    def add_service(self, service: Service):
        self.add_launchable(service)
        self._update_service_bind()

    def remove_service(self, service: Service, *, unsafe: bool = False):
        self.remove_launchable(service.id, unsafe=unsafe)
        self._update_service_bind()

    def get_interface(self, interface_type: Type[TInterface]) -> TInterface:
        if interface_type not in self._service_bind:
            raise ValueError(f"{interface_type} is not supported.")
        return self._service_bind[interface_type].get_interface(interface_type)

    def _get_task(self, launchable_id: str):
        if self.task_group is None:
            raise ValueError("Task Group is not initialized.")
        for key, task in self.tasks.items():
            if key == launchable_id:
                return task

    async def _sideload_prepare(self, launchable: Launchable):
        if TYPE_CHECKING:
            assert self.task_group is not None
        if "preparing" not in launchable.stages:
            return
        if not set(self.launchables.keys()).issuperset(launchable.required):
            raise ValueError(
                f"Sideload {launchable.id} requires {launchable.required} but {set(self.launchables.keys()) - launchable.required} are missing."
            )

        logger.info(f"Sideload {launchable.id}: injecting")

        # shallow status to avoid wrong judgement to application's current status

        local_status = ManagerStatus()
        shallow_self = cast(Launart, override(self, {"status": local_status}))
        launchable.manager = shallow_self

        loop = asyncio.get_running_loop()
        task = loop.create_task(launchable.launch(shallow_self), name=launchable.id)
        task.add_done_callback(partial(_launchable_task_done_callback, self))
        self.tasks[launchable.id] = task

        local_status.stage = "preparing"
        if launchable.status.stage != "waiting-for-prepare":
            logger.info(f"waiting sideload {launchable.id} for prepare")
            await asyncio.wait(
                [
                    task,
                    loop.create_task(launchable.status.wait_for("waiting-for-prepare")),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

        logger.info(f"Sideload {launchable.id}: preparing")

        launchable.status.stage = "preparing"
        await asyncio.wait(
            [task, loop.create_task(launchable.status.wait_for("prepared"))],
            return_when=asyncio.FIRST_COMPLETED,
        )

        logger.info(f"Sideload {launchable.id}: start blocking")
        self.task_group.add_coroutine(
            asyncio.wait(
                [
                    task,
                    loop.create_task(launchable.status.wait_for("blocking-completed", "finished")),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
        )
        local_status.stage = "blocking"
        launchable.manager = self

    async def _sideload_cleanup(self, launchable: Launchable):
        if TYPE_CHECKING:
            assert self.task_group is not None
        if "cleanup" not in launchable.stages:
            return

        loop = asyncio.get_running_loop()
        task = self.tasks[launchable.id]

        local_status = ManagerStatus()
        shallow_self = cast(Launart, override(self, {"status": local_status}))
        launchable.manager = shallow_self

        # take over existed futures
        owned_waiters = [fut for fut in self.status._waiters if FutureMark in {type(cb) for cb, ctx in fut._callbacks}]

        logger.info(f"Sideload {launchable.id}: cleaning up")

        local_status._waiters.extend(owned_waiters)
        local_status.update_multi(
            {
                ManagerStatus.stage: "cleaning",  # type: ignore
                ManagerStatus.exiting: True,  # type: ignore
            }
        )

        if launchable.status.stage != "waiting-for-cleanup":
            await asyncio.wait(
                [
                    task,
                    loop.create_task(launchable.status.wait_for("waiting-for-cleanup")),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

        launchable.status.stage = "cleanup"
        await asyncio.wait(
            [
                task,
                loop.create_task(launchable.status.wait_for("finished")),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        logger.info(f"Sideload {launchable.id}: cleanup completed.")
        if not task.done() or not task.cancelled():
            await task
        logger.info(f"Sideload {launchable.id}: completed.")
        del self.tasks[launchable.id]
        del self.launchables[launchable.id]

    async def launch(self):  # sourcery skip: low-code-quality
        _token = self._context.set(self)
        if self.status.stage is not None:
            logger.error("Incorrect ownership, launart is already running.")
            return

        logger.info(
            f"Launching {len(self.launchables)} components as async task...",
            alt=f"Launching [magenta]{len(self.launchables)}[/] components as async task...",
        )

        loop = asyncio.get_running_loop()
        as_task = loop.create_task
        self.tasks = {}

        for launchable in self.launchables.values():
            launchable.ensure_manager(self)
            task = loop.create_task(launchable.launch(self), name=launchable.id)
            task.add_done_callback(partial(_launchable_task_done_callback, self))
            self.tasks[launchable.id] = task

        self.status.stage = "preparing"
        for k, v in self.launchables.items():
            if "preparing" in v.stages and v.status.stage != "waiting-for-prepare":
                await asyncio.wait(
                    [self.tasks[k], as_task(v.status.wait_for("waiting-for-prepare"))],
                    return_when=asyncio.FIRST_COMPLETED,
                )
        for layer, components in enumerate(
            resolve_requirements(self.launchables.values()),
            start=1,
        ):
            preparing_tasks = []
            for i in components:
                if "preparing" in i.stages:
                    i.status.stage = "preparing"
                    preparing_tasks.append(
                        asyncio.wait(
                            [self.tasks[i.id], as_task(i.status.wait_for("prepared"))],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    )
            if preparing_tasks:
                await asyncio.gather(*preparing_tasks)

                logger.success(
                    f"Layer #{layer}:[{', '.join([i.id for i in components])}] preparation completed.",
                    alt=f"[green]Layer [magenta]#{layer}[/]:[{', '.join([f'[cyan italic]{i.id}[/cyan italic]' for i in components])}] preparation completed.",
                )

        logger.info("All components prepared, start blocking phase.", style="green bold")

        self.status.stage = "blocking"
        self.task_group = FlexibleTaskGroup()
        blocking_tasks = [
            asyncio.wait(
                [
                    self.tasks[i.id],
                    as_task(i.status.wait_for("blocking-completed", "finished")),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for i in self.launchables.values()
            if "blocking" in i.stages
        ]
        self.task_group.add_coroutines(*blocking_tasks)
        try:
            if blocking_tasks:
                await self.task_group.wait()
        except asyncio.CancelledError:
            logger.info("Blocking phase cancelled by user.", style="red bold")
            self.status.exiting = True
        finally:
            logger.info("Entering cleanup phase.", style="yellow bold")

            self.status.stage = "cleaning"
            for k, v in self.launchables.items():
                if "cleanup" in v.stages and v.status.stage != "waiting-for-cleanup":
                    await asyncio.wait(
                        [
                            self.tasks[k],
                            as_task(v.status.wait_for("waiting-for-cleanup")),
                        ],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
            for layer, components in enumerate(
                resolve_requirements(self.launchables.values(), reverse=True),
                start=1,
            ):
                cleaning_tasks = []
                for i in components:
                    if "cleanup" in i.stages:
                        i.status.stage = "cleanup"
                        cleaning_tasks.append(
                            asyncio.wait(
                                [
                                    self.tasks[i.id],
                                    as_task(i.status.wait_for("finished")),
                                ],
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                        )
                if cleaning_tasks:
                    await asyncio.gather(*cleaning_tasks)

                    logger.success(
                        f"Layer #{layer}:[{', '.join([i.id for i in components])}] cleanup completed.",
                        alt=f"[green]Layer [magenta]#{layer}[/]:[{', '.join([f'[cyan]{i.id}[/cyan]' for i in components])}] cleanup completed.",
                    )

            self.status.stage = "finished"
            logger.info(
                "Cleanup completed, waiting for finalization.",
                style="green bold",
            )

            finale_tasks = [i for i in self.tasks.values() if not i.done()]
            if finale_tasks:
                await asyncio.wait(finale_tasks)
            self.task_group = None
            self._context.reset(_token)
            logger.success("All launch task finished.", style="green bold")

    def launch_blocking(self, *, loop: Optional[asyncio.AbstractEventLoop] = None):
        import functools
        import signal
        import threading

        loop = loop or asyncio.new_event_loop()

        logger.info("Starting launart main task...", style="green bold")

        launch_task = loop.create_task(self.launch(), name="amnesia-launch")
        if (
            threading.current_thread() is threading.main_thread()
            and signal.getsignal(signal.SIGINT) is signal.default_int_handler
        ):
            sigint_handler = functools.partial(self._on_sigint, main_task=launch_task)
            try:
                signal.signal(signal.SIGINT, sigint_handler)
            except ValueError:
                # `signal.signal` may throw if `threading.main_thread` does
                # not support signals
                signal_handler = None
        else:
            sigint_handler = None

        loop.run_until_complete(launch_task)

        if sigint_handler is not None and signal.getsignal(signal.SIGINT) is sigint_handler:
            signal.signal(signal.SIGINT, signal.default_int_handler)

        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())

        logger.success("Asyncio shutdown complete.", style="green bold")

    def _on_sigint(self, _, __, main_task: asyncio.Task):
        self.status.exiting = True
        if self.task_group is not None:
            self.task_group.stop = True
            if self.task_group.blocking_task is not None:
                self.task_group.blocking_task.cancel()
        if not main_task.done():
            main_task.cancel()
            # wakeup loop if it is blocked by select() with long timeout
            main_task._loop.call_soon_threadsafe(lambda: None)
            logger.warning("Ctrl-C triggered by user.", style="dark_orange bold")
