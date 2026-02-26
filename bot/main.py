import asyncio
import logging

from aiogram import Bot, Dispatcher

from bot.commands import setup_bot_commands
from bot.config import load_config
from bot.handlers import routers
from bot.storage import init_storage


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    config = load_config()
    init_storage(config.db_path)

    bot = Bot(token=config.bot_token)
    dp = Dispatcher()

    for router in routers:
        dp.include_router(router)

    await setup_bot_commands(bot)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
