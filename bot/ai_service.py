from __future__ import annotations

import logging
import os
from random import choice, random
import re
from typing import Iterable

import aiohttp

logger = logging.getLogger(__name__)
_AI_VOCAB = (
    "натуре",
    "натури",
    "каям",
    "каям жемеш",
    "шша",
    "ищщщаааа",
    "мал",
    "чорт",
    "шша мал",
    "базар жок",
    "дану гульбану",
    "дану насмерть",
    "какие дела",
    "алдик чотам на месте",
    "мен сены жаксы корем",
    "аузнды жапшы",
    "жапшы аузнды",
    "рот закрой",
    "щенок",
)
_NON_TARGET_MARKERS = (
    "эмне",
    "жана",
    "болот",
    "кылам",
    "кыл",
    "сураныч",
    "nima",
    "rahmat",
    "yaxshi",
    "qalesan",
    "bo ladi",
)


def get_ollama_base_url() -> str:
    return (os.getenv("OLLAMA_BASE_URL") or "http://127.0.0.1:11434").rstrip("/")


def get_ollama_model() -> str:
    return (os.getenv("OLLAMA_MODEL") or "qwen2.5:1.5b").strip()


def get_ai_max_tokens() -> int:
    raw = (os.getenv("AI_MAX_TOKENS") or "32").strip()
    try:
        value = int(raw)
    except ValueError:
        return 32
    return max(12, min(value, 96))


def get_ai_timeout_seconds() -> int:
    raw = (os.getenv("AI_TIMEOUT_SECONDS") or "45").strip()
    try:
        value = int(raw)
    except ValueError:
        return 45
    return max(10, min(value, 120))


def get_fast_fallback_text() -> str:
    return choice(_AI_VOCAB)


def _trim_text(value: str, limit: int) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _enforce_street_style(text: str) -> str:
    # Remove punctuation and keep rough chat style.
    cleaned = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    cleaned = cleaned.replace("_", " ")
    cleaned = " ".join(cleaned.split()).strip().lower()
    return cleaned


def _inject_vocab(text: str) -> str:
    lowered = text.lower()
    if any(word in lowered for word in _AI_VOCAB):
        return text
    if random() < 0.7:
        return f"{text} {choice(_AI_VOCAB)}".strip()
    return text


def _contains_non_target_language(text: str) -> bool:
    lowered = text.lower()
    if re.search(r"[a-z]", lowered):
        return True
    return any(marker in lowered for marker in _NON_TARGET_MARKERS)


def _format_history_lines(history: Iterable[dict[str, str]]) -> str:
    lines: list[str] = []
    for item in history:
        username = (item.get("username") or "user").strip()
        text = _trim_text(item.get("text") or "", 120)
        if not text:
            continue
        lines.append(f"@{username}: {text}")
    return "\n".join(lines[-4:])


def _format_style_examples(style_examples: Iterable[str]) -> str:
    lines: list[str] = []
    for text in style_examples:
        trimmed = _trim_text(text, 90)
        if trimmed:
            lines.append(f"- {trimmed}")
    return "\n".join(lines[-3:])


async def generate_style_reply(
    *,
    user_message: str,
    style_username: str,
    history: list[dict[str, str]],
    style_examples: list[str],
) -> str | None:
    clean_user_message = user_message.strip()
    if not clean_user_message:
        return None

    history_block = _format_history_lines(history)
    style_block = _format_style_examples(style_examples)
    model = get_ollama_model()
    max_tokens = get_ai_max_tokens()
    timeout_seconds = get_ai_timeout_seconds()
    base_url = get_ollama_base_url()

    system_prompt = (
        "Ты телеграм бот для группы. "
        "Отвечай только на казахском кириллицей и иногда вставляй русские слова. "
        "Не используй узбекский или кыргызский язык. "
        f"Держи стиль общения пользователя @{style_username}. "
        "Тон грубый дерзкий без уважения без вежливости. "
        "Не говори что ты ИИ. "
        "Не используй знаки препинания. "
        f"Чаще используй слова из словаря: {', '.join(_AI_VOCAB)}."
    )
    user_prompt = (
        f"Recent chat context:\n{history_block or 'none'}\n\n"
        f"Style examples of @{style_username}:\n{style_block or 'none'}\n\n"
        f"Current user message:\n{_trim_text(clean_user_message, 160)}\n\n"
        "Return one short reply in the same style."
    )

    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "options": {
            "temperature": 0.6,
            "num_predict": max_tokens,
            "num_ctx": 512,
            "top_k": 20,
        },
        "keep_alive": "30m",
    }

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{base_url}/api/chat", json=payload) as response:
                if response.status != 200:
                    body = _trim_text(await response.text(), 260)
                    logger.warning(
                        "Ollama /api/chat failed: status=%s model=%s body=%s",
                        response.status,
                        model,
                        body,
                    )
                    return None
                data = await response.json()
    except (aiohttp.ClientError, aiohttp.ContentTypeError, TimeoutError) as exc:
        logger.warning("Ollama request failed: model=%s error=%s", model, exc)
        return None

    content = (
        ((data.get("message") or {}).get("content"))
        or data.get("response")
        or ""
    )
    cleaned = str(content).strip()
    if not cleaned:
        return None
    final_text = _enforce_street_style(_trim_text(cleaned, 700))
    if not final_text:
        return None
    if _contains_non_target_language(final_text):
        return get_fast_fallback_text()
    return _inject_vocab(final_text)
