"""Чанкинг сообщений Telegram и эмбеддинги OpenAI.

Используется ночным джобом качалки (download_telegram_files.py) и kb_backfill.py.
НЕ импортирует download_telegram_files — у того side-effects на импорте
(валидация env, создание клиента).
"""
from __future__ import annotations

import logging
import os

from telethon import errors
from telethon.tl.functions.channels import GetForumTopicsRequest

from kb_store import Chunk

logger = logging.getLogger(__name__)

GAP_SECONDS = 30 * 60    # пауза, разрывающая беседу на отдельные чанки
CHUNK_MAX_CHARS = 4000   # ~1200 токенов для русского текста
EMBED_BATCH = 96

_openai = None


def openai_client():
    global _openai
    if _openai is None:
        from openai import AsyncOpenAI
        _openai = AsyncOpenAI()
    return _openai


async def embed_texts(texts: list[str], client=None) -> list[list[float]]:
    if not texts:
        return []
    client = client or openai_client()
    model = os.getenv('EMBED_MODEL', 'text-embedding-3-small')
    dim = int(os.getenv('EMBED_DIM', '512'))
    out: list[list[float]] = []
    for i in range(0, len(texts), EMBED_BATCH):
        # обрезка — страховка от лимита 8192 токена на один вход
        batch = [t[:20000] for t in texts[i:i + EMBED_BATCH]]
        resp = await client.embeddings.create(model=model, input=batch, dimensions=dim)
        out.extend(d.embedding for d in resp.data)
    return out


async def fetch_topic_names(tg_client, chat_id: int) -> dict[int, str]:
    names: dict[int, str] = {}
    offset_topic = 0
    try:
        while True:
            res = await tg_client(GetForumTopicsRequest(
                channel=chat_id, offset_date=None, offset_id=0,
                offset_topic=offset_topic, limit=100))
            for t in res.topics:
                if hasattr(t, 'title'):  # у ForumTopicDeleted нет title
                    names[t.id] = t.title
            if len(res.topics) < 100:
                break
            offset_topic = res.topics[-1].id
    except (errors.RPCError, TypeError, ValueError):
        return {}  # чат без топиков
    return names


def message_topic_id(msg) -> int:
    r = msg.reply_to
    if r is not None and getattr(r, 'forum_topic', False):
        return r.reply_to_top_id or r.reply_to_msg_id or 1
    return 1  # General в форумах; в обычных чатах всё в одном "топике"


def _sender_name(msg) -> str:
    s = msg.sender  # кэш Telethon, без сетевых вызовов
    if s is None:
        return str(msg.sender_id or 'unknown')
    name = ' '.join(filter(None, [getattr(s, 'first_name', None),
                                  getattr(s, 'last_name', None)]))
    return (name or getattr(s, 'username', None) or getattr(s, 'title', None)
            or str(msg.sender_id))


def build_chunks(chat_id: int, records: list, topic_names: dict[int, str]) -> list[Chunk]:
    """records: [(msg_id, topic_id, date, author, line)] в хронологическом порядке."""
    by_topic: dict[int, list] = {}
    for rec in records:
        by_topic.setdefault(rec[1], []).append(rec)

    chunks: list[Chunk] = []

    def flush(topic_id: int, topic_name: str, buf: list) -> None:
        if not buf:
            return
        authors = ','.join(sorted({r[3] for r in buf}))[:500]
        header = f'Топик «{topic_name}»' if topic_name else 'Обсуждение'
        chunks.append(Chunk(
            chat_id=chat_id, topic_id=topic_id, topic_name=topic_name,
            date_from=buf[0][2].strftime('%Y-%m-%d'),
            date_to=buf[-1][2].strftime('%Y-%m-%d'),
            msg_first=buf[0][0], msg_last=buf[-1][0],
            authors=authors,
            text=header + '\n' + '\n'.join(r[4] for r in buf)))

    for topic_id, recs in by_topic.items():
        topic_name = topic_names.get(topic_id, '')
        buf: list = []
        size = 0
        prev_date = None
        for rec in recs:
            line_len = len(rec[4]) + 1
            if buf and (size + line_len > CHUNK_MAX_CHARS
                        or (rec[2] - prev_date).total_seconds() > GAP_SECONDS):
                flush(topic_id, topic_name, buf)
                buf = []
                size = 0
            buf.append(rec)
            size += line_len
            prev_date = rec[2]
        flush(topic_id, topic_name, buf)
    return chunks


async def ingest_chat(tg_client, store, chat_id: int, min_id: int | None = None,
                      progress=None) -> tuple[int, int]:
    """Инжест сообщений chat_id от last_seen_id (или явного min_id) до конца.

    Возвращает (обработано сообщений, добавлено новых чанков). State двигается
    только после успешной записи — упавший прогон повторится без потерь, а уже
    записанные чанки не переэмбеддятся (детерминированные id).
    """
    state_key = f'last_seen_id:{chat_id}'
    if min_id is None:
        min_id = int(store.get_state(state_key, '0'))
    topic_names = await fetch_topic_names(tg_client, chat_id)
    records = []
    max_id = min_id
    async for msg in tg_client.iter_messages(chat_id, min_id=min_id, reverse=True):
        if msg.id > max_id:
            max_id = msg.id
        text = (msg.raw_text or '').strip()
        if not text:
            continue  # сервисные и пустые медиа-сообщения
        line = f'[{msg.date:%Y-%m-%d %H:%M}] {_sender_name(msg)}: {text}'
        records.append((msg.id, message_topic_id(msg), msg.date, _sender_name(msg), line))
    chunks = build_chunks(chat_id, records, topic_names)
    known = store.existing_ids([c.id for c in chunks])
    new_chunks = [c for c in chunks if c.id not in known]
    # Порциями: не держим все вектора бэкфилла в памяти разом
    for i in range(0, len(new_chunks), EMBED_BATCH):
        part = new_chunks[i:i + EMBED_BATCH]
        vectors = await embed_texts([c.text for c in part])
        for c, v in zip(part, vectors):
            c.embedding = v
        store.upsert_chunks(part)
        if progress:
            progress(min(i + EMBED_BATCH, len(new_chunks)), len(new_chunks))
    if max_id > min_id:
        store.set_state(state_key, str(max_id))
    return len(records), len(new_chunks)


async def scan_chat(tg_client, chat_id: int) -> tuple[int, int]:
    """Для dry-run бэкфилла: (сообщений, символов текста) без API-вызовов."""
    n = 0
    chars = 0
    async for msg in tg_client.iter_messages(chat_id, reverse=True):
        n += 1
        chars += len(msg.raw_text or '')
    return n, chars
