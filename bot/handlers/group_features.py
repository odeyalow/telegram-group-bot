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
from random import choice, randint
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
_EM_PATTERN = re.compile(r"э+м+", re.IGNORECASE)
_REPLY_SEEN_TTL_SECONDS = 15.0
_MEM_HISTORY_WINDOW_SECONDS = 30 * 24 * 60 * 60
_seen_reply_messages: dict[tuple[str, int, int], float] = {}
_yeuoia_reply_state: dict[tuple[int, int], tuple[int, int, float]] = {}
_otn_paroshka_state: dict[int, tuple[int, int, float]] = {}
_MEM_WAIT_RESPONSES = (
    "болд болд родной ка бр миныт",
    "зяныыыым каз жберем",
    "ка кут",
    "жди",
    "аааа смешного захотелось",
    "щя",
    "щясс",
    "ка жберлп жатр родной",
    "айтсан болдго",
    "чекай",
    "оп вот смешного чутка",
)
_PR_TRIGGERS = {"пр", "привет"}
_DO_IT_TRIGGER = "алдик делт неделт"
_WHO_AM_I_TRIGGERS = {"алдик кто я", "алдик мен кммн"}
_SAD_TRIGGERS_EXACT = {"алдик мен грусни", "алдик мен груснимн", "алдик груснимн"}
_ALDIK_NAME_TRIGGERS = {"алдик", "алдияр", "алдош", "алдок", "адиял", "одеяло"}
_YEUOIA_USERNAME = "yeuoia"
_ODEYALOW_USERNAME = "odeyalow"
_YEUOIA_REPLY_STATE_TTL_SECONDS = 24 * 60 * 60
_OTN_PAROSHKA_STATE_TTL_SECONDS = 24 * 60 * 60
_INSTA_USERNAMES = ("aramems", "wasteprod")
_SAD_INSTA_USERNAME = "famouszayo"
_INSTA_POSTS_ENDPOINT = "https://inflact.com/downloader/api/viewer/posts/"
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
_VOICE_DIR = Path(__file__).resolve().parents[1] / "voice"
_MODERATOR_VOICE_PATH = next(
    (
        item
        for item in (_VOICE_DIR.iterdir() if _VOICE_DIR.exists() else ())
        if item.is_file() and item.stem.casefold() == "moderator"
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
_PR_RESPONSES = (
    "пр",
    "прибет",
    "пр кд чд",
    "пр пр",
    "салам попалам",
    "салеееем",
    "самбердк",
    "приветики",
)
_DO_IT_RESPONSES = (
    "да",
    "нет",
    "нууу мб",
    "жб",
    "Делаешь",
    "ДЕЛАЙ",
    "алдик говорит да",
    "алдик говорит нет",
    "нееее",
    "ДАДАДА",
    "еще спрашиваешь? Делай кнч",
    "кнч",
    "ДАУАЙ БАЛЯ",
    "ага",
    "смело",
)
_YEUOIA_RESPONSES = (
    "Ернар кетп калш",
    "Ернар жапшы аузынды",
    "Сглп кетш пж",
    "Мал",
    "Чорт",
    "жапшы аузнды",
    "ЖАПШЫ АУЗНДЫ ПЖ",
    "Ернар, сены ешкм естыгысы келп турган жок",
    "Жогалш",
    "Асыла салш хусним",
    "Гнида",
    "Мудак",
    "Тварь",
    "Мразь",
    "Закрой свой вонючий рот щенок",
)
_SAD_RESPONSES = (
    "Тууу ненадо ато мен тоже грусни",
    "Бомаш грусни",
    "Грусни грусни видо жберем тагда",
    "Ну плачь а то я тоже заплачууу",
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
    return any(token.startswith("паршк") or token in {"отн", "oтн"} for token in tokens)


def _is_otn_trigger(normalized_text: str) -> bool:
    tokens = normalized_text.split()
    return any(token in {"отн", "oтн"} for token in tokens)


def _is_em_trigger(normalized_text: str) -> bool:
    return bool(_EM_PATTERN.search(normalized_text))


def _is_sad_trigger(normalized_text: str) -> bool:
    if normalized_text in _SAD_TRIGGERS_EXACT:
        return True
    tokens = normalized_text.split()
    return any(token.startswith("грусн") or token.startswith("грустн") for token in tokens)


def _is_pr_trigger(normalized_text: str) -> bool:
    tokens = normalized_text.split()
    return any(token in _PR_TRIGGERS for token in tokens)


def _is_yeuoia_user(message: Message) -> bool:
    username = (message.from_user.username if message.from_user else "") or ""
    return username.casefold() == _YEUOIA_USERNAME


def _is_yeuoia_reply_to_odeyalow(message: Message) -> bool:
    if not _is_yeuoia_user(message):
        return False

    reply = message.reply_to_message
    if reply is None or reply.from_user is None:
        return False
    reply_username = (reply.from_user.username or "").casefold()
    return reply_username == _ODEYALOW_USERNAME


def _should_reply_to_yeuoia(chat_id: int, user_id: int) -> bool:
    now = monotonic()
    for key, (_, _, seen_at) in tuple(_yeuoia_reply_state.items()):
        if now - seen_at > _YEUOIA_REPLY_STATE_TTL_SECONDS:
            _yeuoia_reply_state.pop(key, None)

    key = (chat_id, user_id)
    count, target, _ = _yeuoia_reply_state.get(key, (0, randint(2, 4), now))
    count += 1
    if count >= target:
        _yeuoia_reply_state[key] = (0, randint(2, 4), now)
        return True

    _yeuoia_reply_state[key] = (count, target, now)
    return False


def _should_send_otn_paroshka(chat_id: int) -> bool:
    now = monotonic()
    for key, (_, _, seen_at) in tuple(_otn_paroshka_state.items()):
        if now - seen_at > _OTN_PAROSHKA_STATE_TTL_SECONDS:
            _otn_paroshka_state.pop(key, None)

    count, target, _ = _otn_paroshka_state.get(chat_id, (0, randint(10, 15), now))
    count += 1
    if count >= target:
        _otn_paroshka_state[chat_id] = (0, randint(10, 15), now)
        return True

    _otn_paroshka_state[chat_id] = (count, target, now)
    return False


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


def _insta_referer_url(username: str) -> str:
    return f"https://inflact.com/profiles/instagram/{username}/"


def _build_insta_auth_headers(username: str) -> dict[str, str]:
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
        "Referer": _insta_referer_url(username),
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


async def _download_photo_bytes(url: str, source_username: str = _INSTA_USERNAMES[0]) -> tuple[bytes, str] | None:
    timeout = aiohttp.ClientTimeout(total=20)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": _insta_referer_url(source_username),
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


async def _fetch_instagram_timeline_edges(username: str) -> list[dict]:
    timeout = aiohttp.ClientTimeout(total=20)
    headers = {
        "User-Agent": "Mozilla/5.0",
        **_build_insta_auth_headers(username),
    }
    form = aiohttp.FormData()
    form.add_field("url", username)
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

    return [item for item in edges if isinstance(item, dict)]


def _extract_insta_images_from_post(node: dict, source_username: str) -> list[dict[str, str]]:
    typename = str(node.get("__typename") or "").strip()
    post_id = str(node.get("id") or "").strip()
    if typename in {"GraphImage", "XDTGraphImage"}:
        image = _normalize_insta_image_url(str(node.get("display_url") or ""))
        if not image:
            return []
        media_id = post_id or hashlib.sha256(image.encode("utf-8")).hexdigest()[:24]
        return [
            {
                "photo_id": f"insta_photo:{source_username}:{media_id}",
                "photo_url": image,
                "source_username": source_username,
            }
        ]

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

        images.append(
            {
                "photo_id": f"insta_photo:{source_username}:{media_id}",
                "photo_url": image,
                "source_username": source_username,
            }
        )

    return images


def _extract_instagram_post_media_candidates(node: dict, source_username: str) -> list[dict[str, str]]:
    typename = str(node.get("__typename") or "").strip()
    post_id = str(node.get("id") or "").strip()
    shortcode = str(node.get("shortcode") or "").strip()
    post_url = f"https://www.instagram.com/p/{shortcode}" if shortcode else ""
    candidates: list[dict[str, str]] = []

    if typename in {"GraphImage", "XDTGraphImage"}:
        image = _normalize_insta_image_url(str(node.get("display_url") or ""))
        if image:
            media_id = post_id or hashlib.sha256(image.encode("utf-8")).hexdigest()[:24]
            candidates.append(
                {
                    "post_id": f"insta_post:{source_username}:{media_id}",
                    "media_type": "photo",
                    "media_url": image,
                    "post_url": post_url,
                    "source_username": source_username,
                }
            )
        return candidates

    if typename in {"GraphVideo", "XDTGraphVideo"}:
        video = str(node.get("video_url") or "").strip()
        if video:
            media_id = post_id or hashlib.sha256(video.encode("utf-8")).hexdigest()[:24]
            candidates.append(
                {
                    "post_id": f"insta_post:{source_username}:{media_id}",
                    "media_type": "video",
                    "media_url": video,
                    "post_url": post_url,
                    "source_username": source_username,
                }
            )
        return candidates

    if typename not in {"GraphSidecar", "XDTGraphSidecar"}:
        return candidates

    sidecar = node.get("edge_sidecar_to_children") or {}
    if not isinstance(sidecar, dict):
        return candidates

    edges = sidecar.get("edges") or []
    if not isinstance(edges, list):
        return candidates

    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            continue
        child_node = edge.get("node") or {}
        if not isinstance(child_node, dict):
            continue

        child_type = str(child_node.get("__typename") or "").strip()
        child_id = str(child_node.get("id") or "").strip()

        if child_type in {"GraphImage", "XDTGraphImage"}:
            image = _normalize_insta_image_url(str(child_node.get("display_url") or ""))
            if not image:
                continue
            media_id = child_id or post_id or f"img:{index}"
            candidates.append(
                {
                    "post_id": f"insta_post:{source_username}:{media_id}",
                    "media_type": "photo",
                    "media_url": image,
                    "post_url": post_url,
                    "source_username": source_username,
                }
            )
            continue

        if child_type in {"GraphVideo", "XDTGraphVideo"}:
            video = str(child_node.get("video_url") or "").strip()
            if not video:
                continue
            media_id = child_id or post_id or f"vid:{index}"
            candidates.append(
                {
                    "post_id": f"insta_post:{source_username}:{media_id}",
                    "media_type": "video",
                    "media_url": video,
                    "post_url": post_url,
                    "source_username": source_username,
                }
            )

    return candidates


async def _fetch_instagram_photo_candidates() -> list[dict[str, str]]:
    unique_candidates: dict[str, dict[str, str]] = {}

    for username in _INSTA_USERNAMES:
        for edge in await _fetch_instagram_timeline_edges(username):
            node = edge.get("node") or {}
            if not isinstance(node, dict):
                continue

            images = _extract_insta_images_from_post(node, username)
            for image_info in images:
                photo_id = str(image_info.get("photo_id") or "").strip()
                photo_url = str(image_info.get("photo_url") or "").strip()
                source_username = str(image_info.get("source_username") or "").strip()
                if not photo_id or not photo_url or not source_username:
                    continue
                unique_candidates[photo_id] = {
                    "photo_id": photo_id,
                    "photo_url": photo_url,
                    "source_username": source_username,
                }

    return list(unique_candidates.values())


async def _fetch_instagram_post_candidates(username: str) -> list[dict[str, str]]:
    unique_candidates: dict[str, dict[str, str]] = {}
    for edge in await _fetch_instagram_timeline_edges(username):
        node = edge.get("node") or {}
        if not isinstance(node, dict):
            continue

        for candidate in _extract_instagram_post_media_candidates(node, username):
            post_id = str(candidate.get("post_id") or "").strip()
            media_type = str(candidate.get("media_type") or "").strip()
            media_url = str(candidate.get("media_url") or "").strip()
            source_username = str(candidate.get("source_username") or "").strip()
            if not post_id or not media_type or not media_url or not source_username:
                continue
            unique_candidates[post_id] = candidate
    return list(unique_candidates.values())


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


@router.message(F.chat.type.in_(_GROUP_CHAT_TYPES), F.text, ~F.text.startswith("/"))
async def on_group_text(message: Message, bot: Bot) -> None:
    if not message.from_user or message.from_user.is_bot:
        return

    text = message.text or ""

    normalized_text = _normalize_text(text)
    is_mem_photo_request = _is_mem_photo_request(normalized_text)
    is_mem_request = _is_mem_request(normalized_text)
    is_paroshka_trigger = _is_paroshka_trigger(normalized_text)
    is_otn_trigger = _is_otn_trigger(normalized_text)
    is_em_trigger = _is_em_trigger(normalized_text)
    is_sad_trigger = _is_sad_trigger(normalized_text)
    is_pr_trigger = _is_pr_trigger(normalized_text)
    is_do_it_trigger = normalized_text == _DO_IT_TRIGGER
    is_yeuoia_reply_to_odeyalow = _is_yeuoia_reply_to_odeyalow(message)
    is_who_am_i = normalized_text in _WHO_AM_I_TRIGGERS
    is_anon_link_request = _is_anon_link_request(normalized_text)
    is_aldik_name_trigger = _is_aldik_name_trigger(normalized_text)
    is_moderator_word = bool(_MODERATOR_PATTERN.search(text))
    has_regular_trigger = (
        is_mem_photo_request
        or is_mem_request
        or is_paroshka_trigger
        or is_em_trigger
        or is_sad_trigger
        or is_pr_trigger
        or is_do_it_trigger
        or is_who_am_i
        or is_anon_link_request
        or is_aldik_name_trigger
        or is_moderator_word
    )

    if not has_regular_trigger and not is_yeuoia_reply_to_odeyalow:
        return

    if is_mem_photo_request:
        kind = "mem_photo_request"
    elif is_mem_request:
        kind = "mem_request"
    elif is_paroshka_trigger:
        kind = "paroshka_trigger"
    elif is_em_trigger:
        kind = "em_trigger"
    elif is_sad_trigger:
        kind = "sad_trigger"
    elif is_pr_trigger:
        kind = "pr_trigger"
    elif is_do_it_trigger:
        kind = "do_it_trigger"
    elif is_who_am_i:
        kind = "who_am_i"
    elif is_anon_link_request:
        kind = "anon_link_text"
    elif is_aldik_name_trigger:
        kind = "aldik_name"
    elif is_yeuoia_reply_to_odeyalow:
        kind = "yeuoia_user"
    else:
        kind = "moderator_word"

    if _is_duplicate_reply(kind, message.chat.id, message.message_id):
        return

    settings = ensure_group(message.chat.id, message.chat.title or "")
    if not settings.bot_enabled:
        return

    should_reply_to_yeuoia = False
    if is_yeuoia_reply_to_odeyalow and message.from_user:
        should_reply_to_yeuoia = _should_reply_to_yeuoia(
            message.chat.id,
            message.from_user.id,
        )

    if should_reply_to_yeuoia:
        await message.reply(choice(_YEUOIA_RESPONSES))
        return

    if not has_regular_trigger:
        return

    if is_mem_photo_request:
        try:
            await message.reply(choice(_MEM_WAIT_RESPONSES))

            since_ts = int(time()) - _MEM_HISTORY_WINDOW_SECONDS
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
                source_username = str(selected.get("source_username") or "").strip()
                if not photo_id or not photo_url:
                    continue
                if not source_username:
                    source_username = _INSTA_USERNAMES[0]

                try:
                    downloaded = await _download_photo_bytes(photo_url, source_username)
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

            since_ts = int(time()) - _MEM_HISTORY_WINDOW_SECONDS
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

    if is_sad_trigger:
        try:
            if normalized_text in _SAD_TRIGGERS_EXACT:
                await message.reply(choice(_SAD_RESPONSES))

            candidates = await _fetch_instagram_post_candidates(_SAD_INSTA_USERNAME)
            if not candidates:
                await message.reply("Ща не нашел мем, попробуй еще раз через пару секунд.")
                return

            pool = candidates[:]
            while pool:
                selected = choice(pool)
                pool.remove(selected)

                media_type = str(selected.get("media_type") or "").strip()
                media_url = str(selected.get("media_url") or "").strip()
                post_url = str(selected.get("post_url") or "").strip()
                source_username = str(selected.get("source_username") or "").strip()
                if not media_type or not media_url:
                    continue

                if media_type == "photo":
                    try:
                        downloaded = await _download_photo_bytes(
                            media_url,
                            source_username or _SAD_INSTA_USERNAME,
                        )
                        if downloaded is not None:
                            photo_bytes, filename = downloaded
                            await message.answer_photo(BufferedInputFile(photo_bytes, filename=filename))
                            return
                        await message.answer_photo(media_url)
                        return
                    except TelegramAPIError:
                        if post_url:
                            try:
                                await message.answer(post_url)
                                return
                            except TelegramAPIError:
                                pass
                        continue

                if media_type == "video":
                    try:
                        await message.answer_video(media_url)
                        return
                    except TelegramAPIError:
                        if post_url:
                            try:
                                await message.answer(post_url)
                                return
                            except TelegramAPIError:
                                pass
                        continue

            await message.reply("Ща не нашел мем, попробуй еще раз через пару секунд.")
            return
        except Exception:
            logger.exception("Unexpected error while handling sad trigger request")
            await message.reply("Ща не нашел мем, попробуй еще раз через пару секунд.")
            return

    if is_paroshka_trigger:
        if is_otn_trigger and not _should_send_otn_paroshka(message.chat.id):
            return
        if _PAROSHKA_MEDIA_PATH is None:
            logger.warning("parochka media file was not found in %s", _GIFS_DIR)
            return
        try:
            await message.reply_animation(FSInputFile(_PAROSHKA_MEDIA_PATH))
        except TelegramAPIError:
            logger.exception("Failed to send paroshka animation")
        return

    if is_em_trigger:
        await message.reply("э" * randint(1, 10) + "м" * randint(1, 10))
        return

    if is_who_am_i:
        await message.reply(choice(_WHO_AM_I_RESPONSES))
        return

    if is_pr_trigger:
        await message.reply(choice(_PR_RESPONSES))
        return

    if is_do_it_trigger:
        await message.reply(choice(_DO_IT_RESPONSES))
        return

    if is_anon_link_request:
        await _send_anonymous_link(message, bot)
        return

    if is_aldik_name_trigger:
        await message.reply(choice(ALDIK_NAME_RESPONSES))
        return

    if _MODERATOR_VOICE_PATH is not None and randint(0, 1) == 1:
        try:
            await message.reply_voice(FSInputFile(_MODERATOR_VOICE_PATH))
            return
        except TelegramAPIError:
            logger.exception("Failed to send moderator voice message")

    await message.reply(MODERATOR_TRIGGER_TEXT)
