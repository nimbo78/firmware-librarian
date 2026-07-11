"""Одноразовый бэкфилл истории чатов KB_CHAT_IDS в базу знаний.

ВАЖНО: скрипт использует ту же сессию bot.session, что и качалка.
Одновременная работа двух процессов с одной сессией роняет соединение
(AuthKeyDuplicatedError). Запускать ТОЛЬКО при остановленной качалке:

    docker compose stop telegram-file-downloader
    docker compose run --rm telegram-file-downloader python kb_backfill.py --dry-run
    docker compose run --rm telegram-file-downloader python kb_backfill.py [--max-cost 10]
    docker compose start telegram-file-downloader

Порядок ввода в строй: сначала полный бэкфилл, потом включать ночной ingest —
иначе границы суточных чанков не совпадут с полными и появятся почти-дубли.

Боевой прогон идёт тремя этапами: [1/3] текст (быстро и дёшево — база отвечает
уже после него; медиа в чанках плейсхолдерами), [2/3] vision/whisper в кэш,
[3/3] пересборка чанков с описаниями медиа (переэмбеддятся только изменённые).
Полный прогон также удаляет чанки с устаревшими границами (prune).

Бюджет: --dry-run печатает разбивку стоимости по категориям (текст, картинки,
голосовые, PDF) с учётом включённых флагов KB_VISION/KB_VOICE/KB_PDF.
--max-cost N останавливает боевой прогон при достижении N$; всё обработанное
кэшируется, повторный запуск после пополнения продолжит без двойной оплаты.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os

from telethon import TelegramClient

from kb_ingest import (BudgetExceeded, EMBED_PRICE_PER_MTOK, VISION_COST_PER_IMAGE,
                       WHISPER_PRICE_PER_MIN, enrich_chat_media, ingest_chat,
                       pdf_enabled, scan_chat, vision_enabled, voice_enabled)
from kb_store import open_store
from tg_conn import proxy_kwargs


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise SystemExit(f'{name} must be provided')
    return val


def _progress(stage: str, done: int, total: int, cost: float) -> None:
    if stage == 'media':
        print(f'  медиа: {done} обработано, потрачено ~${cost:.2f}', flush=True)
    elif stage == 'embed':
        print(f'  эмбеддинги: {done}/{total}, потрачено ~${cost:.2f}', flush=True)
    elif stage == 'pdf':
        print(f'  PDF: {done} файлов, потрачено ~${cost:.2f}', flush=True)


async def _dry_run(client, chat_ids: list[int], download_folder: str) -> None:
    total = 0.0
    for chat_id in chat_ids:
        print(f'Сканирую {chat_id} — вся история, на большом чате это '
              f'минуты/десятки минут...', flush=True)
        st = await scan_chat(client, chat_id, progress=lambda n: print(
            f'  просмотрено сообщений: {n}', flush=True))
        embed = st.chars / 3 / 1e6 * EMBED_PRICE_PER_MTOK
        vision = st.images * VISION_COST_PER_IMAGE if vision_enabled() else 0.0
        voice = (st.voice_seconds / 60 * WHISPER_PRICE_PER_MIN
                 if voice_enabled() else 0.0)
        total += embed + vision + voice
        print(f'{chat_id}:')
        print(f'  сообщений: {st.messages}, ~{st.chars // 3} токенов '
              f'-> эмбеддинги ~${embed:.2f}')
        mark = '' if vision_enabled() else ' (KB_VISION выключен — не считается)'
        print(f'  картинок: {st.images} -> vision ~${vision:.2f}{mark}')
        mark = '' if voice_enabled() else ' (KB_VOICE выключен — не считается)'
        print(f'  голосовых: {st.voice_seconds // 60} мин -> whisper ~${voice:.2f}{mark}')
    if pdf_enabled():
        from kb_pdf import scan_pdfs
        files, pages = scan_pdfs(download_folder)
        # ~1800 символов на страницу мануала — грубая оценка
        pdf_cost = pages * 1800 / 3 / 1e6 * EMBED_PRICE_PER_MTOK
        total += pdf_cost
        print(f'PDF в {download_folder}: {files} файлов, {pages} страниц '
              f'-> эмбеддинги ~${pdf_cost:.2f}')
    else:
        print('PDF: KB_PDF выключен — не считается')
    print(f'\nИтого оценка: ~${total:.2f}')
    print('Подсказка: --max-cost N остановит боевой прогон при достижении N$.')


async def _backfill(client, chat_ids: list[int], download_folder: str,
                    max_cost: float | None) -> None:
    store = open_store()
    spent = 0.0
    stopped = False
    media_wanted = vision_enabled() or voice_enabled()

    def remaining() -> float | None:
        return None if max_cost is None else max(max_cost - spent, 0.0)

    # Этап 1: только текст (быстро и дёшево) — база отвечает уже после него.
    # Медиа входит в чанки плейсхолдерами, поэтому границы окончательные.
    for chat_id in chat_ids:
        print(f'[1/3] Текст: {chat_id}...', flush=True)
        try:
            stats = await ingest_chat(client, store, chat_id, min_id=0,
                                      progress=_progress, max_cost=remaining(),
                                      enrich_media=False)
        except BudgetExceeded as e:
            spent += e.cost
            stopped = True
            break
        spent += stats.cost
        extra = f', удалено устаревших чанков: {stats.pruned}' if stats.pruned else ''
        print(f'  {stats.messages} сообщений -> {stats.new_chunks} чанков'
              f'{extra}, ~${stats.cost:.2f}')
    if not stopped:
        # каталог: файлы, скачанные до его появления, получают md5 из журнала
        # дедупликации — без этого у них не будет кнопки 📎 в /fw;
        # reparse добирает связки после улучшения регулярок разбора имён
        from kb_firmware import link_local_files, reparse_files
        linked = link_local_files(store, download_folder)
        reparsed = reparse_files(store)
        if linked or reparsed:
            print(f'Каталог: привязано файлов {linked}, '
                  f'новых связок после перепарсинга {reparsed}')

    # Этап 2: vision/whisper — только наполнение кэша, чанки не трогаем
    if media_wanted and not stopped:
        for chat_id in chat_ids:
            print(f'[2/3] Медиа: {chat_id}...', flush=True)
            try:
                n, cost = await enrich_chat_media(client, store, chat_id,
                                                  progress=_progress,
                                                  max_cost=remaining())
            except BudgetExceeded as e:
                spent += e.cost
                stopped = True
                break
            spent += cost
            print(f'  обработано медиа: {n}, ~${cost:.2f}')

    # Этап 3: пересборка чанков с описаниями из кэша; переэмбеддятся
    # только изменившиеся (id те же — сравнение по хэшу текста)
    if media_wanted and not stopped:
        for chat_id in chat_ids:
            print(f'[3/3] Чанки с медиа: {chat_id}...', flush=True)
            try:
                stats = await ingest_chat(client, store, chat_id, min_id=0,
                                          progress=_progress,
                                          max_cost=remaining())
            except BudgetExceeded as e:
                spent += e.cost
                stopped = True
                break
            spent += stats.cost
            print(f'  обновлено чанков: {stats.new_chunks}, ~${stats.cost:.2f}')
    if pdf_enabled() and not stopped:
        from kb_pdf import ingest_pdfs
        try:
            files, chunks, cost = await ingest_pdfs(
                store, download_folder, progress=_progress, max_cost=remaining())
            spent += cost
            print(f'PDF: {files} файлов -> {chunks} чанков, ~${cost:.2f}')
        except BudgetExceeded as e:
            spent += e.cost
            stopped = True
    if not stopped:
        from kb_extract import run_extraction
        fw_added, dev_added, cost = await run_extraction(store)
        spent += cost
        print(f'LLM-экстракция: {fw_added} связок прошивок, {dev_added} серий, '
              f'~${cost:.2f}')
        pending = store.pending_review_count()
        if pending:
            print(f'На подтверждение (/review в личке бота): {pending}')
    store.backup()
    print(f'\nЧанков в базе: {store.count()}. Потрачено в этом прогоне: ~${spent:.2f}')
    if stopped:
        print('ЛИМИТ БЮДЖЕТА ДОСТИГНУТ. Обработанное закэшировано — пополни '
              'баланс API и запусти бэкфилл повторно, он продолжит с места '
              'остановки без двойной оплаты.')
        store.add_event('backfill',
                        f'Бэкфилл ОСТАНОВЛЕН по лимиту бюджета, чанков: {store.count()}',
                        spent)
    else:
        store.add_event('backfill',
                        f'Бэкфилл завершён, чанков в базе: {store.count()}', spent)


async def main() -> None:
    parser = argparse.ArgumentParser(description='Бэкфилл базы знаний')
    parser.add_argument('--dry-run', action='store_true',
                        help='посчитать объём и стоимость по категориям, без OpenAI')
    parser.add_argument('--max-cost', type=float, default=None, metavar='N',
                        help='остановиться при достижении бюджета N$ (безопасно: '
                             'повторный запуск продолжит с кэша)')
    args = parser.parse_args()

    api_id = int(_require('TELEGRAM_API_ID'))
    api_hash = _require('TELEGRAM_API_HASH')
    chat_ids = [int(x) for x in _require('KB_CHAT_IDS').split(',') if x.strip()]
    download_folder = os.getenv('DOWNLOAD_FOLDER', './downloads')

    client = TelegramClient(
        'bot', api_id, api_hash,
        connection_retries=-1,
        retry_delay=300,
        auto_reconnect=True,
        request_retries=5,
        timeout=30,
        # полный прогон истории: длинные FloodWait пересыпаем, а не падаем
        flood_sleep_threshold=86400,
        **proxy_kwargs(),
    )
    # телеграмные предупреждения (FloodWait, обрывы) — в stderr, а не в тишину
    logging.basicConfig(level=logging.WARNING,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    print('Подключаюсь к Telegram...', flush=True)
    await client.start(phone=lambda: input('Enter your phone: '))
    print('Подключился.', flush=True)
    try:
        if args.dry_run:
            await _dry_run(client, chat_ids, download_folder)
        else:
            await _backfill(client, chat_ids, download_folder, args.max_cost)
    finally:
        await client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
