from __future__ import annotations

import asyncio
from typing import (
    Callable,
    Coroutine,
    Dict,
    Hashable,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

T = TypeVar("T")
H = TypeVar("H", bound=Hashable)

PriorityType = Union[
    Set[T],
    Dict[T, Union[int, float]],
    Tuple[
        Union[
            Set[T],
            Dict[T, Union[int, float]],
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
    result = {}
    _cache = {}

    def _raise_conflict(content):
        raise ValueError(
            f"{content} which is an unlocated item is already existed, and it conflicts with {result[content]}"
        )

    def _raise_existed(content):
        raise ValueError(f"{content} is already existed, and it conflicts with {result[content]}, an unlocated item.")

    def _handle(pattern):
        if isinstance(pattern, Set):
            for content in pattern:
                if content in _cache:
                    _raise_conflict(content)
                _cache[content] = ...
                result[content] = item
        elif isinstance(pattern, Dict):
            for content, priority in pattern.items():
                if content in _cache:
                    if _cache[content] is ...:
                        _raise_existed(content)
                    if priority is ...:
                        _raise_conflict(content)
                    if _cache[content] < priority:
                        _cache[content] = priority
                        result[content] = item
                else:
                    _cache[content] = priority
                    result[content] = item
        else:
            raise TypeError(f"{pattern} is not a valid pattern.")

    for item in items:
        pattern = getter(item)
        if isinstance(pattern, (dict, set)):
            _handle(pattern)
        elif isinstance(pattern, tuple):
            for subpattern in pattern:
                _handle(subpattern)
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
                print("tg c!", self.stop)
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
            tasks = [
                self.blocking_task._loop.create_task(coroutine)
                for coroutine in coroutines
            ]
        else:
            tasks = [asyncio.create_task(coroutine) for coroutine in coroutines]
        self.add_tasks(*tasks)