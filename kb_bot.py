"""Answer-бот базы знаний: /ask и @упоминание в чатах KB_ANSWER_CHAT_IDS.

Отдельный контейнер (сервис kb-bot в docker-compose.yml). Логин по бот-токену
не требует интерактива; сессия хранится на томе /app/kb, чтобы не создавать
новую при каждом рестарте.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time

from telethon import TelegramClient, events

from kb_ingest import embed_texts, openai_client
from kb_store import open_store

API_ID = int(os.environ['TELEGRAM_API_ID'])
API_HASH = os.environ['TELEGRAM_API_HASH']
BOT_TOKEN = os.getenv('KB_BOT_TOKEN', '')
ANSWER_CHAT_IDS = {int(x) for x in os.getenv('KB_ANSWER_CHAT_IDS', '').split(',')
                   if x.strip()}
ANSWER_MODEL = os.getenv('ANSWER_MODEL', 'gpt-5-mini')
SESSION = os.getenv('KB_BOT_SESSION', 'kb_bot')
COOLDOWN_SECONDS = 30
TOP_K = 8

SYSTEM_PROMPT = (
    'Ты — ассистент чата по оборудованию Huawei. Отвечай кратко и по-русски, '
    'опираясь только на приведённый контекст из истории чата. Ссылайся на '
    'фрагменты номерами в квадратных скобках, например [1]. Если ответа в '
    'контексте нет — прямо скажи об этом, не выдумывай.'
)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('telethon').setLevel(logging.WARNING)

client = TelegramClient(SESSION, API_ID, API_HASH,
                        connection_retries=-1, retry_delay=300,
                        auto_reconnect=True, request_retries=5, timeout=30)
store = open_store()
_last_ask: dict[int, float] = {}
_bot_username = ''


def _msg_link(chat_id: int, msg_id: int) -> str | None:
    s = str(chat_id)
    if s.startswith('-100'):
        return f'https://t.me/c/{s[4:]}/{msg_id}'
    return None


def _extract_question(text: str) -> str | None:
    m = re.match(r'^/ask(@\w+)?\s*(.*)$', text, re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(2).strip()
    if _bot_username:
        mention = f'@{_bot_username}'
        if mention.lower() in text.lower():
            return re.sub(re.escape(mention), '', text, flags=re.IGNORECASE).strip()
    return None


async def answer_question(question: str) -> str:
    oa = openai_client()
    qvec = (await embed_texts([question], client=oa))[0]
    hits = store.search(question, qvec, top_k=TOP_K)
    if not hits:
        return 'В базе знаний пока ничего не нашлось по этому вопросу.'
    ctx_parts = []
    links = []
    for i, h in enumerate(hits, 1):
        where = f'топик «{h.topic_name}», {h.date_from}' if h.topic_name else h.date_from
        ctx_parts.append(f'[{i}] ({where})\n{h.text}')
        link = _msg_link(h.chat_id, h.msg_first)
        if link:
            links.append(f'[{i}] {link}')
    # temperature/max_tokens не передаём: модели класса gpt-5 их не принимают
    resp = await oa.chat.completions.create(
        model=ANSWER_MODEL,
        messages=[
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content':
                'Контекст:\n\n' + '\n\n'.join(ctx_parts) + f'\n\nВопрос: {question}'},
        ])
    answer = (resp.choices[0].message.content or '').strip()
    if links:
        answer += '\n\nИсточники:\n' + '\n'.join(links[:5])
    return answer[:4000]  # лимит сообщения Telegram — 4096


@client.on(events.NewMessage)
async def handler(event):
    if event.chat_id not in ANSWER_CHAT_IDS:
        return
    question = _extract_question(event.raw_text or '')
    if question is None:
        return
    if not question:
        await event.reply('Напиши вопрос после команды: /ask как прошить ONT')
        return
    now = time.monotonic()
    if now - _last_ask.get(event.sender_id, 0.0) < COOLDOWN_SECONDS:
        await event.reply('Подожди немного перед следующим вопросом.')
        return
    _last_ask[event.sender_id] = now
    logger.info('Question from %s in %s: %s',
                event.sender_id, event.chat_id, question[:100])
    try:
        answer = await answer_question(question)
    except Exception as e:
        logger.warning('Answer failed: %s', e)
        answer = 'Не получилось получить ответ, попробуй позже.'
    await event.reply(answer, link_preview=False)


async def run() -> None:
    global _bot_username
    if not BOT_TOKEN or not ANSWER_CHAT_IDS:
        # Не падаем: crash-loop с restart: unless-stopped только шумит в логах
        logger.error('KB_BOT_TOKEN / KB_ANSWER_CHAT_IDS не заданы — бот в режиме '
                     'ожидания. Заполни .env и передеплой.')
        while True:
            await asyncio.sleep(3600)
    if not os.getenv('OPENAI_API_KEY'):
        logger.warning('OPENAI_API_KEY не задан — вопросы будут падать до появления ключа')
    initial_backoff = 300
    max_backoff = 1800
    backoff = initial_backoff
    while True:
        try:
            await client.start(bot_token=BOT_TOKEN)
            me = await client.get_me()
            _bot_username = me.username or ''
            logger.info('KB bot online as @%s, чанков в базе: %d',
                        _bot_username, store.count())
            backoff = initial_backoff
            await client.run_until_disconnected()
        except (ConnectionError, OSError, asyncio.TimeoutError) as e:
            logger.warning('Top-level reconnect in %ds after: %s', backoff, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
        else:
            logger.info('KB bot disconnected cleanly')
            return


if __name__ == '__main__':
    asyncio.run(run())
