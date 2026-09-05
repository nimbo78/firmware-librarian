"""Список чатов аккаунта: id, название, тип — чтобы не искать id руками.

Печатает строки в том виде, в каком их ждёт spaces.toml, и подсказывает
топики форумов. Ничего не меняет: только читает список диалогов.

ВАЖНО: использует ту же сессию bot.session, что и качалка — запускать
при остановленной качалке (иначе AuthKeyDuplicatedError):

    docker compose stop librarian
    docker compose run --rm librarian python kb_chats.py
    docker compose run --rm librarian python kb_chats.py b4      # фильтр по имени
    docker compose start librarian

Топики форума (для answer = ["<чат>:<топик>"]):

    docker compose run --rm librarian python kb_chats.py --topics -1001234567890
"""
from __future__ import annotations

import asyncio
import os
import sys

from telethon import TelegramClient

from tg_conn import proxy_kwargs


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise SystemExit(f'{name} must be provided')
    return val


def _kind(dialog) -> str:
    if dialog.is_channel and not getattr(dialog.entity, 'megagroup', False):
        return 'канал'
    if dialog.is_group:
        return 'группа'
    if dialog.is_user:
        return 'личка'
    return '?'


async def _list_dialogs(client, needle: str) -> None:
    print(f'{"id":>16}  {"тип":<8} название')
    print('-' * 72)
    shown = 0
    async for d in client.iter_dialogs():
        title = (d.name or '').strip()
        if needle and needle.lower() not in title.lower():
            continue
        forum = ' [форум]' if getattr(d.entity, 'forum', False) else ''
        print(f'{d.id:>16}  {_kind(d):<8} {title}{forum}')
        shown += 1
    print('-' * 72)
    print(f'Показано: {shown}. В spaces.toml это chats = [<id>, ...];'
          ' у форумов топики — --topics <id>.')


async def _list_topics(client, chat_id: int) -> None:
    from kb_ingest import fetch_topics
    topics = await fetch_topics(client, chat_id)
    if not topics:
        print(f'{chat_id}: топиков нет (обычная группа или список недоступен) '
              f'— в answer достаточно "{chat_id}"')
        return
    print(f'Топики {chat_id}:')
    for tid, (title, closed) in sorted(topics.items()):
        mark = ' (закрыт — бот в нём молчит)' if closed else ''
        print(f'  {chat_id}:{tid}  {title}{mark}')
    print('В spaces.toml: answer = ["<чат>:<топик>", ...]')


async def main() -> None:
    args = [a for a in sys.argv[1:]]
    api_id = int(_require('TELEGRAM_API_ID'))
    api_hash = _require('TELEGRAM_API_HASH')
    client = TelegramClient('bot', api_id, api_hash,
                            connection_retries=3, retry_delay=5,
                            **proxy_kwargs())
    await client.start(phone=lambda: input('Enter your phone: '))
    try:
        if args and args[0] == '--topics':
            if len(args) < 2:
                raise SystemExit('Использование: kb_chats.py --topics <chat_id>')
            await _list_topics(client, int(args[1]))
        else:
            await _list_dialogs(client, args[0] if args else '')
    finally:
        await client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
