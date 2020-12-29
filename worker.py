import asyncio
import uvloop
import logging
from barrage_crawler.manager import Manager, create_manager
from config.settings import ROOMS_TABLE_NAME

logging.basicConfig(level=logging.INFO)
if __name__ == "__main__":
    uvloop.install()
    manager: Manager = create_manager(ROOMS_TABLE_NAME)
    manager.run()
