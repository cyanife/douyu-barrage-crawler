BARRAGE_MESSAGE_TYPE: str = "chatmsg"

DOUYU_USER_NAME: str = "0"
DOUYU_UID: str = "0"
DOUYU_CONFIG: dict = dict(username=DOUYU_USER_NAME, uid=DOUYU_UID)

ROOMS_TABLE_NAME = "room"

POSTGRES = {
    "database": "postgres",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": "5432",
}

try:
    from .settings.local import *  # noqa
except ImportError:
    pass