from __future__ import annotations

import logging
import os
from typing import Iterable

import aiohttp

logger = logging.getLogger(__name__)


def get_ollama_base_url() -> str:
    return (os.getenv("OLLAMA_BASE_URL") or "http://127.0.0.1:11434").rstrip("/")


def get_ollama_model() -> str:
    return (os.getenv("OLLAMA_MODEL") or "qwen2.5:1.5b").strip()


def _trim_text(value: str, limit: int) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _format_history_lines(history: Iterable[dict[str, str]]) -> str:
    lines: list[str] = []
    for item in history:
        username = (item.get("username") or "user").strip()
        text = _trim_text(item.get("text") or "", 220)
        if not text:
            continue
        lines.append(f"@{username}: {text}")
    return "\n".join(lines[-18:])


def _format_style_examples(style_examples: Iterable[str]) -> str:
    lines: list[str] = []
    for text in style_examples:
        trimmed = _trim_text(text, 140)
        if trimmed:
            lines.append(f"- {trimmed}")
    return "\n".join(lines[-12:])


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
    base_url = get_ollama_base_url()

    system_prompt = (
        "Ты телеграм-бот для группы. Отвечай коротко и по делу, "
        f"в живом разговорном стиле пользователя @{style_username}. "
        "Не упоминай, что ты ИИ."
    )
    user_prompt = (
        f"Контекст:\n{history_block or 'нет'}\n\n"
        f"Примеры стиля @{style_username}:\n{style_block or 'пока нет'}\n\n"
        f"Текущее сообщение:\n{_trim_text(clean_user_message, 400)}\n\n"
        "Дай один краткий ответ в этом же стиле."
    )

    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "options": {
            "temperature": 0.8,
            "num_predict": 96,
        },
    }

    timeout = aiohttp.ClientTimeout(total=120)
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
    return _trim_text(cleaned, 700)
