from __future__ import annotations

import asyncio
import signal
from contextvars import ContextVar
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    Literal,
    Optional,
    Type,
    cast,
)

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
        while not self.preparing:
            await self.wait_for_update()

    async def wait_for_blocking(self):
        while not self.blocking:
            await self.wait_for_update()

    async def wait_for_cleaning(self, *, current: str | None = None):
        while not self.cleaning:
            await self.wait_for_update(current=current, stage="cleaning")

    async def wait_for_finished(self, *, current: str | None = None):
        while self.stage not in {"finished", None}:
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
        launchable.ensure_manager(self)
        if launchable.id in self.launchables:
            raise ValueError(f"Launchable {launchable.id} already exists.")
        self.launchables[launchable.id] = launchable
        if isinstance(launchable, Service):
            self._update_service_bind()
        if self.task_group is not None:
            assert self.task_group.blocking_task is not None
            loop = asyncio.get_running_loop()
            loop.create_task(self._sideload_prepare(launchable))

    def get_launchable(self, id: str) -> Launchable:
        if id not in self.launchables:
            raise ValueError(f"Launchable {id} does not exists.")
        return self.launchables[id]

    def get_service(self, id: str) -> Service:
        launchable = self.get_launchable(id)
        if not isinstance(launchable, Service):
            raise TypeError(f"{id} is not a service.")
        return launchable

    def remove_launchable(self, launchable: str | Launchable, *, unsafe: bool = False):
        if isinstance(launchable, str):
            if launchable not in self.launchables:
                raise ValueError(f"Launchable {id} does not exist.")
            target = self.launchables[launchable]
        else:
            target = launchable
        if self.task_group is not None:
            assert self.task_group.blocking_task is not None
            loop = asyncio.get_running_loop()

            if target.status.stage not in {"prepared", "blocking", "blocking-completed", "waiting-for-cleanup"}:
                raise RuntimeError(
                    f"{target.id} obtains invalid stats to sideload active release, it's {target.status.stage}"
                )

            # check requirements status
            if not unsafe:
                layers = resolve_requirements(self.launchables.values())
                if not layers or launchable not in layers[0]:
                    raise RuntimeError

            loop.create_task(self._sideload_cleanup(target))
        else:
            del self.launchables[target.id]
        if isinstance(target, Service):
            self._update_service_bind()

    def _update_service_bind(self):
        self._service_bind = priority_strategy(
            [i for i in self.launchables.values() if isinstance(i, Service)],
            lambda a: a.supported_interface_types,
        )

    add_service = add_launchable
    remove_service = remove_launchable

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
            return  # FIXME
        if not set(self.launchables.keys()).issuperset(launchable._required_id):
            raise ValueError(
                f"Sideload {launchable.id} requires {launchable._required_id} but {set(self.launchables.keys()) - launchable._required_id} are missing."
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
        self.task_group.add(
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

    async def _sideload_cleanup(self, launchable: Launchable):  # FIXME
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
                ManagerStatus.stage: "cleaning",
                ManagerStatus.exiting: True,
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
        self.status.exiting = False
        logger.info(
            f"Launching {len(self.launchables)} components as async task...",
            alt=f"Launching [magenta]{len(self.launchables)}[/] components as async task...",
        )

        loop = asyncio.get_running_loop()
        as_task = loop.create_task
        self.tasks = {}

        for launchable in self.launchables.values():
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
        self.task_group.add(*blocking_tasks)
        try:
            if blocking_tasks:
                await self.task_group
        except asyncio.CancelledError:
            logger.info("Blocking phase cancelled by user.", style="red bold")
        finally:
            self.status.exiting = True
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
            self.status.stage = None  # reset status

    def launch_blocking(
        self,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        stop_signal: Iterable[signal.Signals] = (signal.SIGINT,),
    ):
        import contextlib
        import functools
        import threading

        loop = loop or asyncio.new_event_loop()

        logger.info("Starting launart main task...", style="green bold")

        launch_task = loop.create_task(self.launch(), name="amnesia-launch")
        handled_signals: Dict[signal.Signals, Any] = {}
        signal_handler = functools.partial(self._on_sys_signal, main_task=launch_task)
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
            self._cancel_tasks(loop)
            loop.run_until_complete(loop.shutdown_asyncgens())
            with contextlib.suppress(RuntimeError, AttributeError):
                # LINK: https://docs.python.org/3.10/library/asyncio-eventloop.html#asyncio.loop.shutdown_default_executor
                # TODO: MENTION THIS IN CHANGELOG
                loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            logger.success("asyncio shutdown complete.", style="green bold")

    def _on_sys_signal(self, _, __, main_task: asyncio.Task):
        self.status.exiting = True
        if self.task_group is not None:
            self.task_group.stop = True
            if self.task_group.blocking_task is not None:  # TODO: TEST THIS
                self.task_group.blocking_task.cancel()
        if not main_task.done():
            main_task.cancel()
            # wakeup loop if it is blocked by select() with long timeout
            main_task._loop.call_soon_threadsafe(lambda: None)
            logger.warning("Ctrl-C triggered by user.", style="dark_orange bold")

    @staticmethod
    def _cancel_tasks(loop: asyncio.AbstractEventLoop):
        import asyncio
        import asyncio.tasks

        to_cancel = asyncio.tasks.all_tasks(loop)
        if to_cancel:
            for tsk in to_cancel:
                tsk.cancel()
            loop.run_until_complete(asyncio.gather(*to_cancel, return_exceptions=True))

            for task in to_cancel:  # pragma: no cover
                # BELIEVE IN PSF
                if task.cancelled():
                    continue
                if task.exception() is not None:
                    logger.opt(exception=task.exception()).error(f"Unhandled exception when shutting down {task}:")
