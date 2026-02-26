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
            BotCommand(command="help", description="мнау крч тупойларга"),
        ],
        scope=BotCommandScopeAllPrivateChats(),
    )

    await bot.set_my_commands(
        commands=[
            BotCommand(command="help", description="мнау крч тупойларга"),
            BotCommand(command="group_info", description="осы группанын анау мынауысын корстет"),
        ],
        scope=BotCommandScopeAllGroupChats(),
    )

    await bot.set_my_commands(
        commands=[
            BotCommand(command="help", description="мнау крч тупойларга"),
            BotCommand(command="group_info", description="осы группанын анау мынауысын корстет"),
            BotCommand(command="bot_on", description="мены косад крч"),
            BotCommand(command="bot_off", description="мены ошред, но ошрмеш пж умаляю"),
            BotCommand(command="anon_on", description="анон сообтарды косады крч"),
            BotCommand(command="anon_off", description="анон сообтарды ошред"),
            BotCommand(command="anon_link", description="анонга жазу ушын ссылка берем"),
        ],
        scope=BotCommandScopeAllChatAdministrators(),
    )
