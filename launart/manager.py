import asyncio
from functools import partial
from typing import TYPE_CHECKING, Dict, Literal, Optional, Type, cast

from loguru import logger
from statv import Stats, Statv

from launart._sideload import override
from launart.component import Launchable, resolve_requirements
from launart.service import ExportInterface, Service, TInterface
from launart.utilles import FlexibleTaskGroup, priority_strategy

U_ManagerStage = Literal["preparing", "blocking", "cleaning", "finished"]


def _launchable_task_done_callback(mgr: "Launart", t: asyncio.Task):
    exc = t.exception()
    if exc:
        logger.opt(exception=exc).error(
            f"[{t.get_name()}] raised a exception.",
            alt=f"[red bold]component [magenta]{t.get_name()}[/magenta] raised a exception.",
        )
        return

    component = mgr.get_launchable(t.get_name())
    if mgr.status.preparing:
        if "preparing" in component.stages:
            if component.status.prepared:
                logger.info(f"component {t.get_name()} reported prepare completed.")
            else:
                logger.warning(f"component {t.get_name()} defined preparing, but exited before status updating.")
    elif mgr.status.blocking:
        if "cleanup" in component.stages:
            logger.warning(f"component {t.get_name()} blocking exited without cleanup.")
        else:
            logger.success(f"component {t.get_name()} finished.")
    elif mgr.status.cleaning:
        if "cleanup" in component.stages:
            if component.status.finished:
                logger.success(f"component {t.get_name()} finished.")
            else:
                logger.error(f"component {t.get_name()} defined cleanup, but task completed without reporting.")

    logger.info(
        f"[{t.get_name()}] running completed.",
        alt=f"\\[[magenta]{t.get_name()}[/magenta]] running completed.",
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

    async def wait_for_preparing(self):
        while self.stage != "preparing":
            await self.wait_for_update()

    async def wait_for_blocking(self):
        while self.stage != "blocking":
            await self.wait_for_update()

    async def wait_for_cleaning(self):
        while self.stage != "cleaning":
            await self.wait_for_update()

    async def wait_for_finished(self):
        while self.stage != "finished":
            await self.wait_for_update()

    async def wait_for_sigexit(self):
        while self.stage in {"preparing", "blocking"} and not self.exiting:
            await self.wait_for_update()


class Launart:
    launchables: Dict[str, Launchable]
    status: ManagerStatus
    tasks: dict[str, asyncio.Task]
    taskgroup: Optional[FlexibleTaskGroup] = None

    _service_bind: Dict[Type[ExportInterface], Service]

    def __init__(self):
        self.launchables = {}
        self._service_bind = {}
        self.tasks = {}
        self.status = ManagerStatus()

    def add_launchable(self, launchable: Launchable):
        if launchable.id in self.launchables:
            raise ValueError(f"Launchable {launchable.id} already exists.")
        self.launchables[launchable.id] = launchable
        if self.taskgroup is not None:
            assert self.taskgroup.blocking_task is not None
            loop = self.taskgroup.blocking_task.get_loop()
            loop.create_task(self._sideload_prepare(launchable))

    def get_launchable(self, id: str):
        if id not in self.launchables:
            raise ValueError(f"Launchable {id} does not exists.")
        return self.launchables[id]

    def remove_launchable(self, id: str):
        if id not in self.launchables:
            raise ValueError(f"Launchable {id} does not exists.")
        if self.taskgroup is not None:
            raise RuntimeError("cannot remove launchable while taskgroup is running.")
        # TODO: unsafe remove on runtime
        del self.launchables[id]

    def _update_service_bind(self):
        self._service_bind = priority_strategy(
            [i for i in self.launchables.values() if isinstance(i, Service)], lambda a: a.supported_interface_types
        )

    def get_service(self, id: str) -> Service:
        launchable = self.get_launchable(id)
        if not isinstance(launchable, Service):
            raise ValueError(f"{id} is not a service.")
        return launchable

    def add_service(self, service: Service):
        self.add_launchable(service)
        self._update_service_bind()

    def remove_service(self, service: Service):
        self.remove_launchable(service.id)
        self._update_service_bind()

    def get_interface(self, interface_type: Type[TInterface]) -> TInterface:
        if interface_type not in self._service_bind:
            raise ValueError(f"{interface_type} is not supported.")
        return self._service_bind[interface_type].get_interface(interface_type)

    def _get_task(self, launchable_id: str):
        if self.taskgroup is None:
            raise ValueError("Taskgroup is not initialized.")
        for key, task in self.tasks.items():
            if key == launchable_id:
                return task

    async def _sideload_prepare(self, launchable: Launchable):
        if TYPE_CHECKING:
            assert self.taskgroup is not None
        if "preparing" not in launchable.stages:
            return
        if not set(self.launchables.keys()).issuperset(launchable.required):
            raise ValueError(
                f"{launchable.id} requires {launchable.required} but {set(self.launchables.keys()) - launchable.required} are missing."
            )

        logger.info(f"sideload {launchable.id} enter the workflow")

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
                [task, loop.create_task(launchable.status.wait_for("waiting-for-prepare"))],
                return_when=asyncio.FIRST_COMPLETED,
            )

        logger.info(f"start preparing for sideload {launchable.id}")

        launchable.status.stage = "preparing"
        await asyncio.wait(
            [task, loop.create_task(launchable.status.wait_for("prepared"))], return_when=asyncio.FIRST_COMPLETED
        )

        logger.info(f"sideload {launchable.id} prepared, join the taskgroup")
        self.taskgroup.add_coroutine(
            asyncio.wait(
                [task, loop.create_task(launchable.status.wait_for("blocking-completed", "finished"))],
                return_when=asyncio.FIRST_COMPLETED,
            )
        )
        local_status.stage = "blocking"
        launchable.manager = self

    async def launch(self):  # sourcery skip: low-code-quality
        if self.status.stage is not None:
            logger.error("detected incorrect ownership, launart may already running.")
            return

        logger.info(f"launchable components count: {len(self.launchables)}")
        logger.info("launch all components as async task...")

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
        for layer, components in enumerate(resolve_requirements(set(self.launchables.values()))):
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

        logger.info("all components prepared, blocking start.", style="green bold")

        self.status.stage = "blocking"
        self.taskgroup = FlexibleTaskGroup()
        blocking_tasks = [
            asyncio.wait(
                [self.tasks[i.id], as_task(i.status.wait_for("blocking-completed", "finished"))],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for i in self.launchables.values()
            if "blocking" in i.stages
        ]
        self.taskgroup.add_coroutines(*blocking_tasks)
        try:
            if blocking_tasks:
                await self.taskgroup.wait()
        except asyncio.CancelledError:
            logger.info("blocking phase cancelled by user.", style="red bold")
            self.status.exiting = True
        finally:
            logger.info("application will enter the clean-up phase.", style="bold")

            self.status.stage = "cleaning"
            for k, v in self.launchables.items():
                if "cleanup" in v.stages and v.status.stage != "waiting-for-cleanup":
                    await asyncio.wait(
                        [self.tasks[k], as_task(v.status.wait_for("waiting-for-cleanup"))],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
            for layer, components in enumerate(reversed(resolve_requirements(set(self.launchables.values())))):
                cleaning_tasks = []
                for i in components:
                    if "cleanup" in i.stages:
                        i.status.stage = "cleanup"
                        cleaning_tasks.append(
                            asyncio.wait(
                                [self.tasks[i.id], as_task(i.status.wait_for("finished"))],
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
            logger.info("cleanup stage finished, now waits for launchable tasks' finale.", style="green bold")

            finale_tasks = [i for i in self.tasks.values() if not i.done()]
            if finale_tasks:
                await asyncio.wait(finale_tasks)
            logger.warning("all launch task finished.", style="green bold")

    def launch_blocking(self, *, loop: Optional[asyncio.AbstractEventLoop] = None):
        import functools
        import signal
        import threading

        loop = loop or asyncio.new_event_loop()
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

    def _on_sigint(self, _, __, main_task: asyncio.Task):
        self.status.exiting = True
        if self.taskgroup is not None:
            self.taskgroup.stop = True
            if self.taskgroup.blocking_task is not None:
                self.taskgroup.blocking_task.cancel()
        if not main_task.done():
            main_task.cancel()
            # wakeup loop if it is blocked by select() with long timeout
            main_task._loop.call_soon_threadsafe(lambda: None)
            logger.info("Ctrl-C triggered by user.", style="dark_orange bold")
