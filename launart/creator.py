from __future__ import annotations

from typing import TYPE_CHECKING

from creart import AbstractCreator, CreateTargetInfo

if TYPE_CHECKING:
    from .manager import Launart


class LaunartCreator(AbstractCreator):
    targets = (
        CreateTargetInfo(
            module="launart.amnager",
            identify="Launart",
            humanized_name="Launart",
            description="<common,graia> universal application lifespan manager",
            author=["GraiaProject@github"],
        ),
    )

    @staticmethod
    def create(
        create_type: type[Launart],
    ) -> Launart:
        return create_type()
