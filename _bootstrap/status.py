from enum import Enum


class Stage(int, Enum):
    PREPARE = 0
    ONLINE = 1
    CLEANUP = 2
    EXIT = 3


class Phase(int, Enum):
    WAITING = 0
    PENDING = 1
    COMPLETED = 2


ServiceStatusValue = tuple[Stage, Phase]