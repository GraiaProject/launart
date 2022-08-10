from __future__ import annotations

import asyncio
import enum
from typing import (
    Callable,
    Coroutine,
    Dict,
    Hashable,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

T = TypeVar("T")
H = TypeVar("H", bound=Hashable)


class _Unmarked(enum.Enum):
    UNMARKED = object()


UNMARKED = _Unmarked.UNMARKED

# "Unmarked" is a great way of replacing ellipsis.

PriorityType = Union[
    Set[T],
    Dict[T, float],
    Tuple[
        Union[
            Set[T],
            Dict[T, float],
        ],
        ...,
    ],
]


def priority_strategy(
    items: List[T],
    getter: Callable[
        [T],
        PriorityType[H],
    ],
) -> Dict[H, T]:
    result: Dict[H, T] = {}
    _priority_mem: Dict[H, float | Literal[UNMARKED]] = {}

    def _handle(pattern: PriorityType[H]) -> None:
        """Handle an actual pattern."""
        if isinstance(pattern, set):
            # Pattern has unknown priorities.
            for content in pattern:
                if content in _priority_mem:
                    raise ValueError(f"{content} conflicts with {result[content]}")
                _priority_mem[content] = UNMARKED
                result[content] = item

        elif isinstance(pattern, dict):
            for content, priority in pattern.items():
                if content in _priority_mem:
                    current_priority: float | Literal[_Unmarked.UNMARKED] = _priority_mem[content]
                    if current_priority is UNMARKED or priority is UNMARKED:
                        raise ValueError(f"Unable to determine priority order: {content}, {result[content]}.")
                    if current_priority < priority:
                        _priority_mem[content] = priority
                        result[content] = item
                else:
                    _priority_mem[content] = priority
                    result[content] = item

        else:
            raise TypeError(f"{pattern} is not a valid pattern.")

    for item in items:
        pattern: PriorityType[H] = getter(item)
        if isinstance(pattern, (dict, set)):
            _handle(pattern)
        elif isinstance(pattern, tuple):
            for sub_pattern in pattern:
                _handle(sub_pattern)
        else:
            raise TypeError(f"{pattern} is not a valid pattern.")
    return result


async def wait_fut(
    coros: Iterable[Union[Coroutine, asyncio.Task]],
    *,
    timeout: Optional[float] = None,
    return_when: str = asyncio.ALL_COMPLETED,
) -> None:
    tasks = []
    for c in coros:
        if asyncio.iscoroutine(c):
            tasks.append(asyncio.create_task(c))
        else:
            tasks.append(c)
    if tasks:
        await asyncio.wait(tasks, timeout=timeout, return_when=return_when)


class FlexibleTaskGroup:
    tasks: list[asyncio.Task]
    blocking_task: Optional[asyncio.Task] = None
    stop: bool = False

    def __init__(self, *tasks):
        self.tasks = list(tasks)

    def __await__(self):
        loop = asyncio.get_running_loop()
        self.blocking_task = loop.create_task(self.__await_impl__())
        return self.blocking_task.__await__()

    async def __await_impl__(self):
        while True:
            try:
                return await asyncio.shield(asyncio.wait(self.tasks))
            except asyncio.CancelledError:
                if self.stop:
                    raise
            except KeyboardInterrupt:
                for task in self.tasks:
                    task.cancel()
                raise

    async def wait(self):
        loop = asyncio.get_running_loop()
        self.blocking_task = loop.create_task(self.__await_impl__())
        await self.blocking_task

    def add_task(self, task: asyncio.Task):
        if self.blocking_task is not None:
            self.blocking_task.cancel()
        self.tasks.append(task)

    def add_coroutine(self, coroutine: Coroutine):
        if self.blocking_task is not None:
            task = self.blocking_task._loop.create_task(coroutine)
        else:
            task = asyncio.create_task(coroutine)
        self.add_task(task)

    def add_tasks(self, *tasks: asyncio.Task):
        if self.blocking_task is not None:
            self.blocking_task.cancel()
        self.tasks.extend(tasks)

    def add_coroutines(self, *coroutines: Coroutine):
        if self.blocking_task is not None:
            tasks = [self.blocking_task._loop.create_task(coroutine) for coroutine in coroutines]
        else:
            tasks = [asyncio.create_task(coroutine) for coroutine in coroutines]
        self.add_tasks(*tasks)
