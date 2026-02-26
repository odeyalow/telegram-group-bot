from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
from pathlib import Path
import re
import secrets
from random import choice
from time import time
from time import monotonic
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import aiohttp

from aiogram import Bot, F, Router
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command
from aiogram.exceptions import TelegramAPIError
from aiogram.types import BufferedInputFile, ChatMemberUpdated, FSInputFile, Message
from aiogram.utils.deep_linking import create_start_link

from bot.storage import (
    add_meme_history,
    GroupSettings,
    ensure_anonymous_token,
    ensure_group,
    get_recent_meme_video_ids,
    set_anonymous_enabled,
    set_bot_enabled,
)
from bot.texts import (
    ALDIK_NAME_RESPONSES,
    BOT_JOIN_TEXT,
    GROUP_HELP_TEXT,
    MODERATOR_TRIGGER_TEXT,
)

router = Router()
logger = logging.getLogger(__name__)

_GROUP_CHAT_TYPES = {"group", "supergroup"}
_MODERATOR_PATTERN = re.compile(r"\bмодер(?:атор)?\b", re.IGNORECASE)
_HASHTAG_MEME_PATTERN = re.compile(r"#(?:meme|мем)\b", re.IGNORECASE)
_REPLY_SEEN_TTL_SECONDS = 15.0
_seen_reply_messages: dict[tuple[str, int, int], float] = {}
_MEM_WAIT_RESPONSES = (
    "болд болд родной ка бр миныт",
    "зяныыыым каз жберем",
    "ка кут",
    "жди",
    "аааа мема захотелось",
)
_WHO_AM_I_TRIGGERS = {"алдик кто я", "алдик мен кммн"}
_ALDIK_NAME_TRIGGERS = {"алдик", "алдияр", "алдош", "алдок", "адиял", "одеяло"}
_INSTA_USERNAME = "aramems"
_INSTA_POSTS_ENDPOINT = "https://inflact.com/downloader/api/viewer/posts/"
_INSTA_REFERER_URL = f"https://inflact.com/profiles/instagram/{_INSTA_USERNAME}/"
_INSTA_TOKEN_BLOCKS: tuple[tuple[int, ...], ...] = (
    (57, 100, 48, 54, 51, 60, 48, 102),
    (98, 53, 59, 55, 51, 100, 103, 100),
    (51, 50, 48, 101, 102, 53, 48, 63),
    (49, 103, 52, 50, 49, 100, 51, 100),
    (99, 48, 96, 98, 98, 96, 101, 62),
    (53, 49, 53, 54, 97, 50, 99, 62),
    (55, 57, 97, 55, 50, 61, 101, 62),
    (100, 101, 97, 55, 103, 51, 54, 97),
)
_INSTA_CLIENT_ID = secrets.token_hex(16)
_GIFS_DIR = Path(__file__).resolve().parents[1] / "gifs"
_PAROSHKA_MEDIA_PATH = next(
    (
        item
        for item in (_GIFS_DIR.iterdir() if _GIFS_DIR.exists() else ())
        if item.is_file() and item.stem.casefold() == "parochka"
    ),
    None,
)
_WHO_AM_I_RESPONSES = (
    "Ты рот закрой",
    "Сен прыщан андай типо ысыып жара жара болп жаткан",
    "Зяныы зянымснго",
    "Похумеспа саган",
    "Енды сен км екенынды мен айтып журин",
    "Викепедиядан караш",
    "Чорт",
    "Мал в пальто",
    "Лох",
    "Менын зурегыыыым сол ууу зянм",
    "Подчиненный алдика",
    "Раб алдика",
    "Подчиненный и раб алдика",
    "Бопаши",
    "хз",
    "ернп турм ответ беруге",
    "Крутой чел но не круче алдика",
    "Сисяпися",
    "Алдиктын бопесы",
    "Закрой свой вонючий рот щенок",
    "Маманнан сураш",
    "Папаннан сураш",
    "Кем бы ты ни был, ты для меня самый лучший человек, алдик тебя любит",
    "Тагы бррет сураш",
    "Любимчик алдика",
    "Геморойснго журген натуре",
    "Мен сены жаксы корем",
    "Жанмснго",
    "Жан журегм менын",
    "Жапырак дуниемснго",
    "Кудооой кудой менын балапаныыымснго",
    "Менын зайченогм",
    "Котигм сол",
    "Люблююю люблю тебя",
)


def _enabled_text(value: bool) -> str:
    return "вкл" if value else "выкл"


def _format_group_info(settings: GroupSettings) -> str:
    return (
        f"Группа хуня паиди но вот крч\n\n"
        f"Назбание: {settings.title or 'без названия'}\n"
        f"Айди но сендер ваще кажет жок: {settings.chat_id}\n"
        f"Бот статусы: {_enabled_text(settings.bot_enabled)}\n"
        f"Анонка статусы: {_enabled_text(settings.anonymous_enabled)}"
    )


def _normalize_text(text: str) -> str:
    cleaned = re.sub(r"[^\w\s]", " ", text.casefold())
    return " ".join(cleaned.split())


def _has_token_with_prefix(tokens: list[str], prefix: str) -> bool:
    return any(token.startswith(prefix) for token in tokens)


def _is_anon_link_request(normalized_text: str) -> bool:
    tokens = normalized_text.split()
    if "алдик" not in tokens:
        return False

    has_link_word = _has_token_with_prefix(tokens, "ссыл")
    has_anon_word = _has_token_with_prefix(tokens, "анон")
    return has_link_word or has_anon_word


def _is_aldik_name_trigger(normalized_text: str) -> bool:
    tokens = normalized_text.split()
    return any(token in _ALDIK_NAME_TRIGGERS for token in tokens)


def _is_mem_request(normalized_text: str) -> bool:
    tokens = normalized_text.split()
    if not tokens:
        return False

    has_aldik_name = any(token in _ALDIK_NAME_TRIGGERS for token in tokens)
    has_video_word = any(
        token.startswith("видо") or token.startswith("видео") or token == "video"
        for token in tokens
    )
    return has_aldik_name and has_video_word


def _is_mem_photo_request(normalized_text: str) -> bool:
    tokens = normalized_text.split()
    if not tokens:
        return False

    has_aldik_name = any(token in _ALDIK_NAME_TRIGGERS for token in tokens)
    has_photo_word = any(
        token.startswith("пото")
        or token.startswith("фото")
        or token.startswith("фотк")
        or token == "photo"
        for token in tokens
    )
    return has_aldik_name and has_photo_word


def _is_paroshka_trigger(normalized_text: str) -> bool:
    tokens = normalized_text.split()
    if not tokens:
        return False
    return any(token.startswith("паршк") for token in tokens)


def _xor_with_index(text: str) -> str:
    length = len(text)
    if length == 0:
        return text
    return "".join(chr(ord(char) ^ (idx % length)) for idx, char in enumerate(text))


def _build_insta_secret_key() -> str:
    parts: list[str] = []
    for block in _INSTA_TOKEN_BLOCKS:
        decoded = "".join(chr(value) for value in block)
        parts.append(_xor_with_index(decoded))
    return "".join(parts)


def _build_insta_auth_headers() -> dict[str, str]:
    payload = {
        "timestamp": int(time()),
        "clientId": _INSTA_CLIENT_ID,
        "nonce": secrets.token_hex(16),
    }
    payload_json = json.dumps(payload, separators=(",", ":"))
    signature = hmac.new(
        _build_insta_secret_key().encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    token = base64.b64encode(payload_json.encode("utf-8")).decode("ascii")
    return {
        "X-Client-Token": token,
        "X-Client-Signature": signature,
        "Referer": _INSTA_REFERER_URL,
    }


def _normalize_insta_image_url(raw_url: str) -> str:
    url = raw_url.strip()
    if not url:
        return ""

    parsed = urlparse(url)
    if "cdn.inflact.com" in parsed.netloc:
        # Wrapped CDN URL is more stable for downloading than direct scontent links.
        return url

    cleaned_query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() != "stp"
    ]
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(cleaned_query, doseq=True),
            parsed.fragment,
        )
    )


def _guess_image_filename(url: str, content_type: str) -> str:
    content_type = content_type.lower()
    if content_type.startswith("image/jpeg"):
        ext = ".jpg"
    elif content_type.startswith("image/png"):
        ext = ".png"
    elif content_type.startswith("image/webp"):
        ext = ".webp"
    else:
        path = urlparse(url).path.lower()
        if path.endswith(".png"):
            ext = ".png"
        elif path.endswith(".webp"):
            ext = ".webp"
        elif path.endswith(".jpeg"):
            ext = ".jpeg"
        else:
            ext = ".jpg"
    return f"meme_photo{ext}"


async def _download_photo_bytes(url: str) -> tuple[bytes, str] | None:
    timeout = aiohttp.ClientTimeout(total=20)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": _INSTA_REFERER_URL,
    }
    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(url, allow_redirects=True) as response:
                if response.status != 200:
                    return None
                content_type = str(response.headers.get("Content-Type") or "").split(";", 1)[0].strip()
                if content_type and not content_type.lower().startswith("image/"):
                    return None
                payload = await response.read()
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        return None

    if not payload:
        return None
    if len(payload) > 9_500_000:
        return None

    filename = _guess_image_filename(url, content_type)
    return payload, filename


def _extract_insta_images_from_post(node: dict) -> list[dict[str, str]]:
    typename = str(node.get("__typename") or "").strip()
    post_id = str(node.get("id") or "").strip()
    if typename in {"GraphImage", "XDTGraphImage"}:
        image = _normalize_insta_image_url(str(node.get("display_url") or ""))
        if not image:
            return []
        media_id = post_id or hashlib.sha256(image.encode("utf-8")).hexdigest()[:24]
        return [{"photo_id": f"insta_photo:{media_id}", "photo_url": image}]

    if typename not in {"GraphSidecar", "XDTGraphSidecar"}:
        return []

    sidecar = node.get("edge_sidecar_to_children") or {}
    if not isinstance(sidecar, dict):
        return []

    edges = sidecar.get("edges") or []
    if not isinstance(edges, list):
        return []

    images: list[dict[str, str]] = []
    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            continue
        child_node = edge.get("node") or {}
        if not isinstance(child_node, dict):
            continue

        child_type = str(child_node.get("__typename") or "").strip()
        if child_type not in {"GraphImage", "XDTGraphImage"}:
            continue

        image = _normalize_insta_image_url(str(child_node.get("display_url") or ""))
        if not image:
            continue

        child_id = str(child_node.get("id") or "").strip()
        media_id = child_id or post_id or f"noid:{index}"
        if not child_id and not post_id:
            media_id = hashlib.sha256(f"{index}:{image}".encode("utf-8")).hexdigest()[:24]

        images.append({"photo_id": f"insta_photo:{media_id}", "photo_url": image})

    return images


async def _fetch_instagram_photo_candidates() -> list[dict[str, str]]:
    timeout = aiohttp.ClientTimeout(total=20)
    headers = {
        "User-Agent": "Mozilla/5.0",
        **_build_insta_auth_headers(),
    }
    form = aiohttp.FormData()
    form.add_field("url", _INSTA_USERNAME)
    form.add_field("cursor", "")

    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.post(_INSTA_POSTS_ENDPOINT, data=form) as response:
                data = await response.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        return []

    if not isinstance(data, dict) or data.get("status") != "success":
        return []

    payload = data.get("data") or {}
    if not isinstance(payload, dict):
        return []

    posts = payload.get("posts") or {}
    if not isinstance(posts, dict):
        return []

    posts_data = posts.get("data") or {}
    if not isinstance(posts_data, dict):
        return []

    user = posts_data.get("user") or {}
    if not isinstance(user, dict):
        return []

    timeline = user.get("edge_owner_to_timeline_media") or {}
    if not isinstance(timeline, dict):
        return []

    edges = timeline.get("edges") or []
    if not isinstance(edges, list):
        return []

    unique_candidates: dict[str, str] = {}
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        node = edge.get("node") or {}
        if not isinstance(node, dict):
            continue

        images = _extract_insta_images_from_post(node)
        for image_info in images:
            photo_id = str(image_info.get("photo_id") or "").strip()
            photo_url = str(image_info.get("photo_url") or "").strip()
            if photo_id and photo_url:
                unique_candidates[photo_id] = photo_url

    return [{"photo_id": key, "photo_url": value} for key, value in unique_candidates.items()]


def _extract_tikwm_videos(data: object) -> list[dict]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if isinstance(data, dict):
        videos = data.get("videos")
        if isinstance(videos, list):
            return [item for item in videos if isinstance(item, dict)]

    return []


def _to_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _is_meme_video(item: dict) -> bool:
    title = str(item.get("title") or "").lower()
    descriptions = item.get("content_desc") or []
    if not isinstance(descriptions, list):
        descriptions = [str(descriptions)]

    combined = " ".join([title, *[str(part).lower() for part in descriptions]])
    return bool(_HASHTAG_MEME_PATTERN.search(combined)) or "meme" in combined or "мем" in combined


def _get_tiktok_web_url(item: dict) -> str | None:
    video_id = str(item.get("video_id") or "").strip()
    if not video_id:
        return None

    author = item.get("author") or {}
    if isinstance(author, dict):
        unique_id = str(author.get("unique_id") or "").strip()
        if unique_id:
            return f"https://www.tiktok.com/@{unique_id}/video/{video_id}"

    return f"https://www.tiktok.com/video/{video_id}"


async def _fetch_popular_meme_candidates() -> list[dict]:
    keywords = ("meme", "мем")
    endpoint = "https://www.tikwm.com/api/feed/search"
    timeout = aiohttp.ClientTimeout(total=15)
    headers = {"User-Agent": "Mozilla/5.0"}

    aggregated: dict[str, dict] = {}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        for keyword in keywords:
            for attempt in range(2):
                try:
                    async with session.get(endpoint, params={"keywords": keyword, "count": 40}) as response:
                        data = await response.json(content_type=None)
                except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
                    break

                if not isinstance(data, dict):
                    break

                if data.get("code") == -1 and attempt == 0:
                    await asyncio.sleep(1.1)
                    continue

                videos = _extract_tikwm_videos(data.get("data"))
                for item in videos:
                    if not _is_meme_video(item):
                        continue

                    video_id = str(item.get("video_id") or "").strip()
                    if not video_id:
                        continue

                    play_url = str(item.get("play") or item.get("wmplay") or "").strip()
                    web_url = _get_tiktok_web_url(item)
                    if not play_url and not web_url:
                        continue

                    play_count = _to_int(item.get("play_count"))
                    existing = aggregated.get(video_id)
                    if existing is None or play_count > _to_int(existing.get("play_count")):
                        aggregated[video_id] = {
                            "video_id": video_id,
                            "play_url": play_url,
                            "web_url": web_url,
                            "play_count": play_count,
                        }
                break

    candidates = list(aggregated.values())
    candidates.sort(key=lambda x: _to_int(x.get("play_count")), reverse=True)
    return candidates


def _is_duplicate_reply(kind: str, chat_id: int, message_id: int) -> bool:
    now = monotonic()
    for key, seen_at in tuple(_seen_reply_messages.items()):
        if now - seen_at > _REPLY_SEEN_TTL_SECONDS:
            _seen_reply_messages.pop(key, None)

    key = (kind, chat_id, message_id)
    if key in _seen_reply_messages:
        return True

    _seen_reply_messages[key] = now
    return False


async def _require_admin(message: Message, bot: Bot) -> bool:
    if not message.from_user:
        await message.answer("Чота бул адам жок или мен туснбедм")
        return False

    try:
        member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        if member.status in {ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR}:
            return True
    except TelegramAPIError:
        # Fallback for chats where get_chat_member may fail intermittently.
        try:
            admins = await bot.get_chat_administrators(message.chat.id)
            if any(admin.user.id == message.from_user.id for admin in admins):
                return True
        except TelegramAPIError:
            await message.answer(
                "Чота админснба или админ емесснба туснбедм"
                "Алдиктын праваларын группада тексереснго крч"
            )
            return False

    await message.answer("Еееебааа жагаласпашиш натуре или озынды админ сезнеснба")
    return False


async def _send_anonymous_link(message: Message, bot: Bot) -> None:
    settings = ensure_group(message.chat.id, message.chat.title or "")
    if not settings.anonymous_enabled:
        await message.answer("Анонка ошп калд извянки, ес че косу ушын мнаны жазаснго /anon_on.")
        return

    token = ensure_anonymous_token(message.chat.id)
    link = await create_start_link(bot, payload=f"anon:{token}", encode=True)
    await message.answer(f"Ма ма какал, натуре жазгын келп батрго\n{link}")


@router.message(F.chat.type.in_(_GROUP_CHAT_TYPES), Command("help"))
async def group_help(message: Message) -> None:
    ensure_group(message.chat.id, message.chat.title or "")
    await message.answer(GROUP_HELP_TEXT)


@router.message(F.chat.type.in_(_GROUP_CHAT_TYPES), Command("group_info"))
async def group_info(message: Message) -> None:
    settings = ensure_group(message.chat.id, message.chat.title or "")
    await message.answer(_format_group_info(settings))


@router.message(F.chat.type.in_(_GROUP_CHAT_TYPES), Command("bot_on"))
async def bot_on(message: Message, bot: Bot) -> None:
    ensure_group(message.chat.id, message.chat.title or "")
    if not await _require_admin(message, bot):
        return

    set_bot_enabled(message.chat.id, True)
    await message.answer("Аны суйтындершиш ошрми мены натуре")


@router.message(F.chat.type.in_(_GROUP_CHAT_TYPES), Command("bot_off"))
async def bot_off(message: Message, bot: Bot) -> None:
    ensure_group(message.chat.id, message.chat.title or "")
    if not await _require_admin(message, bot):
        return

    set_bot_enabled(message.chat.id, False)
    await message.answer("Ебааа базар жок ошрп тстап мены, пропало смотрю братское")


@router.message(F.chat.type.in_(_GROUP_CHAT_TYPES), Command("anon_on"))
async def anon_on(message: Message, bot: Bot) -> None:
    ensure_group(message.chat.id, message.chat.title or "")
    if not await _require_admin(message, bot):
        return

    set_anonymous_enabled(message.chat.id, True)
    token = ensure_anonymous_token(message.chat.id)
    link = await create_start_link(bot, payload=f"anon:{token}", encode=True)

    await message.answer(
        "Анонка доступна юхууу, жазындар акесн аузы чо хотите\n"
        "Мнау крч ссылка ес че\n"
        f"{link}"
    )


@router.message(F.chat.type.in_(_GROUP_CHAT_TYPES), Command("anon_off"))
async def anon_off(message: Message, bot: Bot) -> None:
    ensure_group(message.chat.id, message.chat.title or "")
    if not await _require_admin(message, bot):
        return

    set_anonymous_enabled(message.chat.id, False)
    await message.answer("Бляяяя анонка неушн ошрдн енды ка алдиктын бопелеры бунт шгарад")


@router.message(F.chat.type.in_(_GROUP_CHAT_TYPES), Command("anon_link"))
async def anon_link(message: Message, bot: Bot) -> None:
    ensure_group(message.chat.id, message.chat.title or "")
    if not await _require_admin(message, bot):
        return

    await _send_anonymous_link(message, bot)


@router.my_chat_member(F.chat.type.in_(_GROUP_CHAT_TYPES))
async def on_bot_added(event: ChatMemberUpdated, bot: Bot) -> None:
    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status
    was_out = old_status in {ChatMemberStatus.LEFT, ChatMemberStatus.KICKED}
    now_in = new_status in {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR}
    if was_out and now_in:
        ensure_group(event.chat.id, event.chat.title or "")
        await bot.send_message(event.chat.id, BOT_JOIN_TEXT)


@router.message(F.chat.type.in_(_GROUP_CHAT_TYPES), F.text)
async def on_group_text(message: Message, bot: Bot) -> None:
    if not message.from_user or message.from_user.is_bot:
        return

    text = message.text or ""
    if text.startswith("/"):
        return

    normalized_text = _normalize_text(text)
    is_mem_photo_request = _is_mem_photo_request(normalized_text)
    is_mem_request = _is_mem_request(normalized_text)
    is_paroshka_trigger = _is_paroshka_trigger(normalized_text)
    is_who_am_i = normalized_text in _WHO_AM_I_TRIGGERS
    is_anon_link_request = _is_anon_link_request(normalized_text)
    is_aldik_name_trigger = _is_aldik_name_trigger(normalized_text)
    is_moderator_word = bool(_MODERATOR_PATTERN.search(text))

    if not (
        is_mem_photo_request
        or is_mem_request
        or is_paroshka_trigger
        or is_who_am_i
        or is_anon_link_request
        or is_aldik_name_trigger
        or is_moderator_word
    ):
        return

    if is_mem_photo_request:
        kind = "mem_photo_request"
    elif is_mem_request:
        kind = "mem_request"
    elif is_paroshka_trigger:
        kind = "paroshka_trigger"
    elif is_who_am_i:
        kind = "who_am_i"
    elif is_anon_link_request:
        kind = "anon_link_text"
    elif is_aldik_name_trigger:
        kind = "aldik_name"
    else:
        kind = "moderator_word"

    if _is_duplicate_reply(kind, message.chat.id, message.message_id):
        return

    settings = ensure_group(message.chat.id, message.chat.title or "")
    if not settings.bot_enabled:
        return

    if is_mem_photo_request:
        try:
            await message.reply(choice(_MEM_WAIT_RESPONSES))

            since_ts = int(time()) - 43200
            recent_ids = get_recent_meme_video_ids(message.chat.id, since_ts)
            candidates = await _fetch_instagram_photo_candidates()
            fresh_candidates = [item for item in candidates if str(item.get("photo_id")) not in recent_ids]

            if not fresh_candidates:
                await message.reply("Ща не нашел мем, попробуй еще раз через пару секунд.")
                return

            pool = fresh_candidates[:]
            while pool:
                selected = choice(pool)
                pool.remove(selected)

                photo_id = str(selected.get("photo_id") or "").strip()
                photo_url = str(selected.get("photo_url") or "").strip()
                if not photo_id or not photo_url:
                    continue

                try:
                    downloaded = await _download_photo_bytes(photo_url)
                    if downloaded is not None:
                        photo_bytes, filename = downloaded
                        await message.answer_photo(BufferedInputFile(photo_bytes, filename=filename))
                    else:
                        await message.answer_photo(photo_url)
                    add_meme_history(message.chat.id, photo_id)
                    return
                except TelegramAPIError:
                    pass

            await message.reply("Ща не нашел мем, попробуй еще раз через пару секунд.")
            return
        except Exception:
            logger.exception("Unexpected error while handling meme photo request")
            await message.reply("Ща не нашел мем, попробуй еще раз через пару секунд.")
            return

    if is_mem_request:
        try:
            await message.reply(choice(_MEM_WAIT_RESPONSES))

            since_ts = int(time()) - 43200
            recent_ids = get_recent_meme_video_ids(message.chat.id, since_ts)
            candidates = await _fetch_popular_meme_candidates()
            fresh_candidates = [item for item in candidates if str(item.get("video_id")) not in recent_ids]

            if not fresh_candidates:
                await message.reply("Ща не нашел мем, попробуй еще раз через пару секунд.")
                return

            top_fresh = fresh_candidates[: min(20, len(fresh_candidates))]
            while top_fresh:
                selected = choice(top_fresh)
                top_fresh.remove(selected)

                video_id = str(selected.get("video_id") or "").strip()
                play_url = str(selected.get("play_url") or "").strip()
                web_url = str(selected.get("web_url") or "").strip()

                if play_url:
                    try:
                        await message.answer_video(play_url)
                        add_meme_history(message.chat.id, video_id)
                        return
                    except TelegramAPIError:
                        pass

                if web_url:
                    try:
                        await message.answer(web_url)
                        add_meme_history(message.chat.id, video_id)
                        return
                    except TelegramAPIError:
                        pass

            await message.reply("Ща не нашел мем, попробуй еще раз через пару секунд.")
            return
        except Exception:
            logger.exception("Unexpected error while handling meme video request")
            await message.reply("Ща не нашел мем, попробуй еще раз через пару секунд.")
            return

    if is_paroshka_trigger:
        if _PAROSHKA_MEDIA_PATH is None:
            logger.warning("parochka media file was not found in %s", _GIFS_DIR)
            return
        try:
            await message.answer_animation(FSInputFile(_PAROSHKA_MEDIA_PATH))
        except TelegramAPIError:
            logger.exception("Failed to send paroshka animation")
        return

    if is_who_am_i:
        await message.reply(choice(_WHO_AM_I_RESPONSES))
        return

    if is_anon_link_request:
        await _send_anonymous_link(message, bot)
        return

    if is_aldik_name_trigger:
        await message.reply(choice(ALDIK_NAME_RESPONSES))
        return

    await message.answer(MODERATOR_TRIGGER_TEXT)
