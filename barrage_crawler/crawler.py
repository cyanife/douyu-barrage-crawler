import asyncio
import websockets
import logging

from typing import Optional, Callable

from .sttutil import STTUtil
from .wsclient import Client

from .driver import Driver, default_driver
from config.settings import DOUYU_CONFIG, BARRAGE_MESSAGE_TYPE

logger = logging.getLogger("crawler.roomclient")


class RoomClient:
    HEARTBEAT_MSG = {"type": "mrkl"}
    LOGOUT_MSG = {"type": "logout"}
    LOGIN_MSG = {
        "type": "loginreq",
        "dfl": "sn@=105/ss@=1/",
        "ver": "20190610",
        "aver": "218101901",
        "ct": "0",
    }
    JOIN_GROUP_MSG = {"type": "joingroup", "gid": "-9999"}

    def __init__(
        self,
        room_id: str,
        config: dict = DOUYU_CONFIG,
        driver: Driver = default_driver,
        message_type: str = BARRAGE_MESSAGE_TYPE,
        table_prefix: str = BARRAGE_MESSAGE_TYPE,
        heartbeat_interval: int = 60,
    ):
        self.room_id = room_id
        self.login_msg = STTUtil.stt_render(
            dict(
                **self.LOGIN_MSG,
                **{
                    "room_id": room_id,
                    "username": config["username"],
                    "uid": config["uid"],
                },
            )
        )
        self.join_group_msg = STTUtil.stt_render(
            dict(**self.JOIN_GROUP_MSG, **{"rid": room_id})
        )
        self.heartbeat_msg = STTUtil.stt_render(self.HEARTBEAT_MSG)
        self.message_type = message_type

        self._wsclient = Client()
        self._driver = driver
        self._table = f"{table_prefix}_{room_id}"

        self._lock = asyncio.Lock()  # lock for atomic operation
        self._pausing: Optional[asyncio.Future] = None
        self._running: Optional[asyncio.Future] = None
        self._closed: bool = False
        self._heartbeat = heartbeat_interval

        self._main_task: Optional[asyncio.Task] = None

    async def _login(self) -> bool:
        return await self._wsclient.send(STTUtil.pack(self.login_msg))

    async def _join_group(self) -> bool:
        return await self._wsclient.send(STTUtil.pack(self.join_group_msg))

    async def _logout(self) -> bool:
        await self._wsclient.send(STTUtil.pack(self.LOGOUT_MSG))

    async def _receive_msg(self) -> bool:
        while self._pausing is None:
            frame = await self._wsclient.recv()
            # raw_msgs = list(
            #     map(STTUtil.stt_parses, STTUtil.unpack_from(frame))
            # )
            # print(raw_msgs)
            msgs = filter(
                lambda m: m.get("type", None) == self.message_type,
                map(STTUtil.stt_parse, STTUtil.unpack_from(frame)),
            )
            if msgs:
                await self._driver.save_jsons(msgs, self._table)
        return await self._logout()

    async def _keep_alive(self) -> None:
        while True:
            await self._wsclient.send(STTUtil.pack(self.heartbeat_msg))
            await asyncio.sleep(self._heartbeat)

    async def _connect(self) -> bool:
        if await self._wsclient.open():
            return await self._login() and await self._join_group()

    async def _prepare(self) -> bool:
        return await self._driver.create_json_table(self._table)

    async def _disconnect(self) -> bool:
        return await self._wsclient.close()

    async def start(self) -> None:
        self._running = asyncio.get_running_loop().create_future()
        while not self._closed:
            logging.info(f"Initializing...Room ID { self.room_id }")
            if self._pausing is not None:
                logging.info(f"Paused. Room ID { self.room_id }")
                await self._pausing

            # init connection(atomic operation)
            async with self._lock:
                if self._closed:
                    logging.info(f"Closing... Room ID { self.room_id }")
                    break
                if await self._prepare() and await self._connect():
                    tasks = []
                    self._main_task = asyncio.create_task(self._receive_msg())
                    tasks.append(self._main_task)
                    tasks.append(asyncio.create_task(self._keep_alive()))
                else:
                    continue  # reconnect
            logging.info(
                f"Initialized. Start receiving messages... Room ID {self.room_id}"
            )

            _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            # if a single task finished/aborted, cancel all other tasks and reconnect
            for task in pending:
                if task != self._main_task:
                    task.cancel()
            await self._disconnect()
            if pending:
                # wait for main task to finish
                done, _ = await asyncio.wait(pending)
                for task in done:
                    try:
                        task.result()
                    except asyncio.CancelledError:
                        pass
                    except Client.ConnectionClosedOK:
                        # recv() after close() throws ConnectionClosedOK
                        pass
                    except Exception as ex:
                        logger.exception(str(ex), exc_info=True)
                        logger.error(f"occurs in room: { self.room_id }")
            logger.info(
                f"Crawler teminated. Room ID {self.room_id}. Restarting or closing..."
            )
        self._running.set_result(False)

    async def stop(self) -> bool:
        if not self._closed:
            self._closed = True
            async with self._lock:
                # do not disconnect during initialization
                await self._disconnect()

            if self._running is not None:
                await self._running

    @property
    def paused(self) -> bool:
        return self._pausing is not None

    def pause(self):
        if self._pausing is None:
            self._pausing = asyncio.get_running_loop().create_future()

    def resume(self):
        if self._pausing is not None:
            self._pausing.set_result(False)
            self._pausing = None
