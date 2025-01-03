from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

from ._resolve import RequirementResolveFailed as RequirementResolveFailed
from ._resolve import resolve_dependencies as resolve_dependencies
from ._resolve import validate_services_removal

if TYPE_CHECKING:
    from .context import ServiceContext


class Service:
    id: str

    @property
    def dependencies(self) -> tuple[str, ...]:
        return ()

    @property
    def before(self) -> tuple[str, ...]:
        return ()

    @property
    def after(self) -> tuple[str, ...]:
        return ()

    async def launch(self, context: ServiceContext):
        async with context.prepare():
            pass

        async with context.online():
            pass

        async with context.cleanup():
            pass


def resolve_services_dependency(services: Iterable[Service], exclude: Iterable[Service], *, reverse: bool = False):
    return resolve_dependencies(
        services,
        exclude=set(exclude),
        reverse=reverse,
    )


def validate_service_removal(existed: Iterable[Service], remove: Iterable[Service]):
    validate_services_removal(existed, remove)
