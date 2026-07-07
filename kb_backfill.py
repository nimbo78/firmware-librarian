"""Одноразовый бэкфилл истории чатов KB_CHAT_IDS в базу знаний.

ВАЖНО: скрипт использует ту же сессию bot.session, что и качалка.
Одновременная работа двух процессов с одной сессией роняет соединение
(AuthKeyDuplicatedError). Запускать ТОЛЬКО при остановленной качалке:

    docker compose stop telegram-file-downloader
    docker compose run --rm telegram-file-downloader python kb_backfill.py --dry-run
    docker compose run --rm telegram-file-downloader python kb_backfill.py
    docker compose start telegram-file-downloader

Порядок ввода в строй: сначала полный бэкфилл, потом включать ночной ingest —
иначе границы суточных чанков не совпадут с полными и появятся почти-дубли.
"""
from __future__ import annotations

import argparse
import asyncio
import os

from telethon import TelegramClient

from kb_ingest import ingest_chat, scan_chat
from kb_store import open_store

EMBED_PRICE_PER_MTOK = 0.02  # $ за 1M токенов text-embedding-3-small


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise SystemExit(f'{name} must be provided')
    return val


async def main() -> None:
    parser = argparse.ArgumentParser(description='Бэкфилл базы знаний')
    parser.add_argument('--dry-run', action='store_true',
                        help='только посчитать объём и стоимость, без OpenAI-вызовов')
    args = parser.parse_args()

    api_id = int(_require('TELEGRAM_API_ID'))
    api_hash = _require('TELEGRAM_API_HASH')
    chat_ids = [int(x) for x in _require('KB_CHAT_IDS').split(',') if x.strip()]

    client = TelegramClient(
        'bot', api_id, api_hash,
        connection_retries=-1,
        retry_delay=300,
        auto_reconnect=True,
        request_retries=5,
        timeout=30,
        # полный прогон истории: длинные FloodWait пересыпаем, а не падаем
        flood_sleep_threshold=86400,
    )
    await client.start(phone=lambda: input('Enter your phone: '))
    try:
        if args.dry_run:
            total_cost = 0.0
            for chat_id in chat_ids:
                n, chars = await scan_chat(client, chat_id)
                tokens = chars // 3
                cost = tokens / 1e6 * EMBED_PRICE_PER_MTOK
                total_cost += cost
                print(f'{chat_id}: {n} сообщений, ~{tokens} токенов, '
                      f'~${cost:.2f} на эмбеддинги')
            print(f'Итого: ~${total_cost:.2f}')
        else:
            store = open_store()
            for chat_id in chat_ids:
                print(f'Бэкфилл {chat_id}...', flush=True)
                msgs, chunks = await ingest_chat(
                    client, store, chat_id, min_id=0,
                    progress=lambda done, total: print(
                        f'  эмбеддинги: {done}/{total}', flush=True))
                print(f'{chat_id}: {msgs} сообщений -> {chunks} новых чанков')
            store.backup()
            print(f'Готово. Чанков в базе: {store.count()}')
    finally:
        await client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
