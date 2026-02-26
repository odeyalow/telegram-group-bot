from __future__ import annotations

from aiogram import Bot, F, Router
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import Message
from aiogram.utils.deep_linking import decode_payload

from bot.anonymous_state import pop_pending_target, set_pending_target
from bot.storage import get_group, get_group_by_anonymous_token
from bot.texts import ANON_PROMPT_TEXT, PRIVATE_HELP_TEXT, PRIVATE_START_TEXT

router = Router()


async def _is_group_member(bot: Bot, chat_id: int, user_id: int) -> bool:
    member = await bot.get_chat_member(chat_id, user_id)
    return member.status not in {ChatMemberStatus.LEFT, ChatMemberStatus.KICKED}


def _extract_payload(command: CommandObject) -> str | None:
    if not command.args:
        return None

    try:
        return decode_payload(command.args)
    except ValueError:
        return command.args


@router.message(F.chat.type == "private", CommandStart(deep_link=True))
async def private_start_deep_link(message: Message, command: CommandObject, bot: Bot) -> None:
    payload = _extract_payload(command)
    if not payload or not payload.startswith("anon:"):
        await message.answer(PRIVATE_START_TEXT)
        return

    token = payload.split("anon:", maxsplit=1)[1].strip()
    settings = get_group_by_anonymous_token(token)
    if settings is None or not settings.bot_enabled or not settings.anonymous_enabled:
        await message.answer("Упс анонка походу ошп тур, кор группадан там группа статусн шгарп яма зяныы зяны")
        return

    if not message.from_user:
        return

    if not await _is_group_member(bot, settings.chat_id, message.from_user.id):
        await message.answer("Мна ссылканын группасы барго негызы сен жок сяхтснго ышынде, атак не путай натуре")
        return

    set_pending_target(message.from_user.id, settings.chat_id)
    await message.answer(ANON_PROMPT_TEXT)


@router.message(F.chat.type == "private", CommandStart())
async def private_start(message: Message) -> None:
    await message.answer(PRIVATE_START_TEXT)


@router.message(F.chat.type == "private", Command("help"))
async def private_help(message: Message) -> None:
    await message.answer(PRIVATE_HELP_TEXT)


@router.message(F.chat.type == "private", F.text)
async def private_text(message: Message, bot: Bot) -> None:
    if not message.from_user:
        return

    text = message.text or ""
    if text.startswith("/"):
        return

    target_chat_id = pop_pending_target(message.from_user.id)
    if target_chat_id is None:
        await message.answer("Вобщм анон жазу ушын группадан ссылканы алып ал там хелп мелп деп жазсан туснп, тупой емес шгарсын да родной")
        return

    settings = get_group(target_chat_id)
    if settings is None or not settings.bot_enabled or not settings.anonymous_enabled:
        await message.answer("Ебаа анонка ошп тур, админдарга айтндар коссын деп хз")
        return

    await bot.send_message(target_chat_id, f"Опааа бреу осндай брдеме жазп тстат:\n\n{text}")
    await message.answer("Опааа, чего мы тут написали, ай какие мы плохие, сообшени пешени палетел группага ес че.")
