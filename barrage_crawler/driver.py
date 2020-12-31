import asyncpg
import logging
import rapidjson
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Iterable

from config.settings import POSTGRES

logger = logging.getLogger("crawler.driver")


class Driver:
    tz_utc_8 = timezone(timedelta(hours=8))

    def __init__(self, config: dict):
        self._config = config
        self._pool = None

    async def create_pool(self, **kw) -> bool:
        logger.info("creating connection pool...")
        logger.debug(f"CONFIG: {str(self._config)}")

        async def init(conn):
            await conn.set_type_codec(
                "jsonb",
                encoder=rapidjson.dumps,
                decoder=rapidjson.loads,
                schema="pg_catalog",
            )

        try:
            self._pool = await asyncpg.create_pool(
                host=self._config["host"],
                port=self._config["port"],
                user=self._config["user"],
                password=self._config["password"],
                database=self._config["database"],
                init=init,
                min_size=kw.get("maxsize", 5),
                max_size=kw.get("maxsize", 10),
            )
        except Exception as e:
            logger.exception(str(e), exc_info=True)
            return False
        else:
            return True

    async def select(self, query: str, args: tuple, single: bool = False):
        try:
            logger.debug(f"SQL: {query}")
            logger.debug(f"ARGS: {str(args)}")
            if single:
                return await self._pool.fetchrow(query, args)
            else:
                return await self._pool.fetch(query, args)
        except Exception as e:
            logger.exception(str(e), exc_info=True)
            return None

    async def select_all(self, query) -> List[asyncpg.Record]:
        try:
            logger.debug(f"SQL: {query}")
            return await self._pool.fetch(query)
        except Exception as e:
            logger.exception(str(e), exc_info=True)
            return []

    async def create_json_table(self, table: str) -> bool:
        query = f"""
            CREATE TABLE IF NOT EXISTS "{table}" (
            id serial PRIMARY KEY,
            userid varchar(20) NOT NULL,
            nickname varchar(100) NOT NULL,
            time timestamptz,
            chatmsg jsonb NOT NULL
            );
        """
        try:
            logger.debug(f"SQL: {query}")
            await self._pool.execute(query)
        except Exception as e:
            logger.error(str(e))
            return False
        else:
            return True

    async def save_json(self, data: dict, table: str) -> bool:
        query = f"""
            INSERT INTO "{table}" (userid, nickname, time, chatmsg) VALUES ($1, $2, $3, $4);
        """
        try:
            logger.debug(f"SQL: {query}")
            logger.debug(f"ARGS: {str(data)}")
            await self._pool.execute(query, self.generate_json_args(data))
        except Exception as e:
            logger.error(str(e))

    async def save_jsons(self, datas: Iterable[dict], table: str) -> bool:
        query = f"""
            INSERT INTO "{table}" (userid, nickname, time, chatmsg) VALUES ($1, $2, $3, $4);
        """
        try:
            logger.debug(f"SQL: {query}")
            logger.debug(f"ARGS: {str(datas)}")
            args = map(self.generate_json_args, datas)
            await self._pool.executemany(query, args)
        except Exception as e:
            logger.error(str(e))

    def generate_json_args(self, data: dict) -> Tuple[str, str, datetime, dict]:
        userid = data["uid"]
        nickname = data["nn"]
        time = None
        ts = data["cst"]
        if ts:
            timestamp = int(ts) if len(ts) == 10 else int(ts) / 1000
            time = datetime.fromtimestamp(timestamp, self.tz_utc_8)
        return (userid, nickname, time, data)


default_driver = Driver(POSTGRES)
