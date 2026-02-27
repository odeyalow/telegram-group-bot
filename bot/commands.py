from aiogram import Bot
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllChatAdministrators,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
)


async def setup_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands(
        commands=[
            BotCommand(command="help", description="help"),
        ],
        scope=BotCommandScopeAllPrivateChats(),
    )

    await bot.set_my_commands(
        commands=[
            BotCommand(command="help", description="help"),
            BotCommand(command="group_info", description="group info"),
        ],
        scope=BotCommandScopeAllGroupChats(),
    )

    await bot.set_my_commands(
        commands=[
            BotCommand(command="help", description="help"),
            BotCommand(command="group_info", description="group info"),
            BotCommand(command="bot_on", description="enable bot"),
            BotCommand(command="bot_off", description="disable bot"),
            BotCommand(command="anon_on", description="enable anon"),
            BotCommand(command="anon_off", description="disable anon"),
            BotCommand(command="anon_link", description="show anon link"),
            BotCommand(command="ai_on", description="enable local ai replies"),
            BotCommand(command="ai_off", description="disable local ai replies"),
            BotCommand(command="ai_style", description="set ai style username"),
            BotCommand(command="ai_status", description="show ai status"),
        ],
        scope=BotCommandScopeAllChatAdministrators(),
    )
