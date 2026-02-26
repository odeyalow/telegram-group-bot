from dataclasses import dataclass
import os

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    bot_token: str
    db_path: str


def load_config() -> Config:
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set. Add it to .env")
    db_path = os.getenv("BOT_DB_PATH", "bot.db")
    return Config(bot_token=bot_token, db_path=db_path)
