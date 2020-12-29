import asyncio
import logging
from typing import List, Dict, AsyncGenerator, Any
from asyncpg import Record

from .driver import Driver, default_driver
from .crawler import RoomClient

logger = logging.getLogger("crawler.manager")


class Manager:
    def __init__(
        self,
        rooms_table_name: str,
        json_field_name: str = "chatmsg",
        driver: Driver = default_driver,
    ):
        self.__rooms_table__ = rooms_table_name
        self.__rooms_query__ = f'SELECT * FROM "{rooms_table_name}";'
        self.__json_field_name__ = json_field_name
        self._driver = driver

        self._clients: Dict[str, RoomClient] = {}
        self._loop = asyncio.get_event_loop()

    def run(self) -> None:
        self._main_task = self._loop.create_task(self._main())
        try:
            self._loop.run_until_complete(self._main_task)
        except asyncio.CancelledError:
            pass
        finally:
            self._loop.run_until_complete(self.close())

    async def close(self) -> None:
        for room in self._clients.values():
            asyncio.create_task(room.stop())

    async def _main(self) -> None:
        logger.info("Starting room manager...")
        await self._driver.create_pool()
        async for _ in poll():
            await self._poll_iteration()

    async def _poll_iteration(self) -> None:
        room_records: List[Record] = await self._driver.select_all(
            self.__rooms_query__
        )
        room_record_ids = list(
            map(lambda r: r.get("room_id", None), room_records)
        )
        # Delete
        for room_id in list(self._clients):
            if room_id not in room_record_ids:
                room = self._clients.pop(room_id)
                asyncio.create_task(room.stop())
        # Update & Add
        for room_record in room_records:
            r_id = room_record.get("room_id")
            r_is_paused = room_record.get("is_paused")
            room = self._clients.get(r_id, None)
            if room:
                if not room.paused and r_is_paused:
                    room.pause()
                elif room.paused and not r_is_paused:
                    room.resume()
            else:
                room = RoomClient(r_id)
                if r_is_paused:
                    room.pause()
                self._clients[r_id] = room
                asyncio.create_task(room.start())
        # print(self._clients)


async def poll(step: float = 10) -> AsyncGenerator[float, None]:
    loop = asyncio.get_event_loop()
    start = loop.time()
    while True:
        before = loop.time()
        yield before - start
        after = loop.time()
        wait = max([0, step - after + before])
        await asyncio.sleep(wait)


def create_manager(rooms_table_name: str, **kwargs: Any) -> Manager:
    manager = Manager(rooms_table_name=rooms_table_name, **kwargs)
    return manager
