from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message

router = Router()


@router.message(CommandStart())
async def on_start(message: Message) -> None:
    await message.answer("Бот запущен. Готов добавлять функционал.")
