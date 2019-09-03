from enum import IntEnum


class ClientStatus(IntEnum):
    INIT = 0
    CONNECTED = 1
    ACTIVE = 2
