from __future__ import annotations

import os
from typing import Iterable

import aiohttp


def get_ollama_base_url() -> str:
    return (os.getenv("OLLAMA_BASE_URL") or "http://127.0.0.1:11434").rstrip("/")


def get_ollama_model() -> str:
    return (os.getenv("OLLAMA_MODEL") or "qwen2.5:7b").strip()


def _trim_text(value: str, limit: int) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _format_history_lines(history: Iterable[dict[str, str]]) -> str:
    lines: list[str] = []
    for item in history:
        username = (item.get("username") or "user").strip()
        text = _trim_text(item.get("text") or "", 280)
        if not text:
            continue
        lines.append(f"@{username}: {text}")
    return "\n".join(lines[-30:])


def _format_style_examples(style_examples: Iterable[str]) -> str:
    lines: list[str] = []
    for text in style_examples:
        trimmed = _trim_text(text, 180)
        if trimmed:
            lines.append(f"- {trimmed}")
    return "\n".join(lines[-20:])


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
        "Ты чат-бот для Telegram группы. Отвечай в стиле живого человека, "
        f"подражая манере пользователя @{style_username}. "
        "Пиши коротко, разговорно, естественно. Не говори, что ты ИИ. "
        "Если вопрос требует конкретики, отвечай по сути."
    )
    user_prompt = (
        f"Контекст чата:\n{history_block or 'нет контекста'}\n\n"
        f"Примеры стиля @{style_username}:\n{style_block or 'примеров пока нет'}\n\n"
        f"Сообщение пользователя:\n{_trim_text(clean_user_message, 500)}\n\n"
        "Сделай один ответ в похожем стиле."
    )

    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "options": {
            "temperature": 0.9,
            "num_predict": 180,
        },
    }

    timeout = aiohttp.ClientTimeout(total=45)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{base_url}/api/chat", json=payload) as response:
                if response.status != 200:
                    return None
                data = await response.json()
    except (aiohttp.ClientError, aiohttp.ContentTypeError, TimeoutError):
        return None

    content = (
        ((data.get("message") or {}).get("content"))
        or data.get("response")
        or ""
    )
    cleaned = str(content).strip()
    if not cleaned:
        return None
    return _trim_text(cleaned, 900)
