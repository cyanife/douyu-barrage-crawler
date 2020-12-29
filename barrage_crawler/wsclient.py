import websockets
import asyncio
import logging
from random import randint
from typing import Optional

logger = logging.getLogger("crawler.wsclient")


class Client:
    DY_MAX_FRAME_SIZE = 2 ** 14
    ConnectionClosedOK = websockets.exceptions.ConnectionClosedOK

    def __init__(self):
        self.uri = f"wss://danmuproxy.douyu.com:{randint(8502,8506)}/"
        self._wsclient = None
        self._buffer = []

    async def open(self) -> bool:
        try:
            # set ping_interval with None to prevent unexpected disconnection
            self._wsclient = await websockets.connect(
                self.uri, ping_interval=None
            )
            return True
        except (websockets.InvalidHandshake, asyncio.TimeoutError) as ex:
            logging.exception(str(ex), exc_info=True)
            return False

    async def close(self) -> bool:
        if self._wsclient is not None:
            await self._wsclient.close()
        return True

    async def recv(self) -> Optional[bytes]:
        if self._wsclient is not None:
            try:
                while True:
                    frame = await self._wsclient.recv()
                    if len(frame) >= self.DY_MAX_FRAME_SIZE:
                        self._buffer.append(frame)
                    else:
                        break
                while self._buffer:
                    frame = self._buffer.pop() + frame
                return frame
            except (RuntimeError, asyncio.TimeoutError,) as ex:
                logging.exception(str(ex), exc_info=True)
                return None
            except (
                asyncio.CancelledError,
                websockets.exceptions.ConnectionClosedError,
            ):
                return None
        return None

    async def send(self, message: bytes) -> bool:
        if self._wsclient is not None:
            try:
                await self._wsclient.send(message)
                return True
            except (asyncio.TimeoutError) as ex:
                logging.exception(str(ex), exc_info=True)
                return False
        return False
