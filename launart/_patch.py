from __future__ import annotations

from collections.abc import Awaitable
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from launart.manager import Launart
    from launart.service import Service


def patch_launch(serv: Service) -> Callable[[Launart], Awaitable[None]]:
    if serv.stages == {"preparing", "blocking", "cleanup"} or serv.stages == {"preparing", "cleanup"}:
        return serv.launch
    elif not serv.stages:

        async def _launch(manager: Launart):
            await serv.launch(manager)
            async with serv.stage("preparing"):
                pass
            async with serv.stage("blocking"):
                pass
            async with serv.stage("cleanup"):
                pass

        return _launch
    elif serv.stages == {"preparing", "blocking"}:

        async def _launch(manager: Launart):
            await serv.launch(manager)
            async with serv.stage("cleanup"):
                pass

        return _launch
    elif serv.stages == {"blocking", "cleanup"}:

        async def _launch(manager: Launart):
            async with serv.stage("preparing"):
                pass
            await serv.launch(manager)

        return _launch
    elif serv.stages == {"preparing"}:

        async def _launch(manager: Launart):
            await serv.launch(manager)
            async with serv.stage("cleanup"):
                pass

        return _launch
    elif serv.stages == {"blocking"}:

        async def _launch(manager: Launart):
            async with serv.stage("preparing"):
                pass
            await serv.launch(manager)
            async with serv.stage("cleanup"):
                pass

        return _launch
    else:  # serv.stages == {"cleanup"}

        async def _launch(manager: Launart):
            async with serv.stage("preparing"):
                pass
            await serv.launch(manager)

        return _launch
