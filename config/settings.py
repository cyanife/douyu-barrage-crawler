import os

BARRAGE_MESSAGE_TYPE: str = "chatmsg"

DOUYU_USER_NAME: str = "0"
DOUYU_UID: str = "0"
DOUYU_CONFIG: dict = dict(username=DOUYU_USER_NAME, uid=DOUYU_UID)

ROOMS_TABLE_NAME = "room"

POSTGRES = {
    "database": os.getenv("DATABASE", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS", "password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

try:
    from .settings.local import *  # noqa
except ImportError:
    pass