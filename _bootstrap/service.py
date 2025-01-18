from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context import ServiceContext


class Service:
    id: str

    @property
    def before(self) -> tuple[str, ...]:
        return ()

    @property
    def after(self) -> tuple[str, ...]:
        return ()

    async def launch(self, context: ServiceContext):
        async with context.prepare():
            pass

        if context.ready:
            ...

        async with context.cleanup():
            pass