from contextlib import suppress as _suppress

from _bootstrap._resolve import (  # noqa: F401
    RequirementResolveFailed as RequirementResolveFailed,
)
from _bootstrap.utiles import any_completed as any_completed  # noqa: F401

from .manager import Launart as Launart
from .service import Service as Service

with _suppress(ImportError, ModuleNotFoundError):
    from .saya import LaunartBehaviour as LaunartBehaviour
    from .saya import ServiceSchema as ServiceSchema
del _suppress
