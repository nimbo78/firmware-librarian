"""Одноразовый бэкфилл истории чатов KB_CHAT_IDS в базу знаний.

ВАЖНО: скрипт использует ту же сессию bot.session, что и качалка.
Одновременная работа двух процессов с одной сессией роняет соединение
(AuthKeyDuplicatedError). Запускать ТОЛЬКО при остановленной качалке:

    docker compose stop librarian
    docker compose run --rm librarian python kb_backfill.py --dry-run
    docker compose run --rm librarian python kb_backfill.py [--max-cost 10]
    docker compose start librarian

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

Пространства знаний (kb_spaces): по умолчанию обрабатываются все, --space b4
сужает прогон до одной области — так новое пространство заводится, не трогая
уже проинжещенные.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os

from telethon import TelegramClient

from kb_ingest import (BudgetExceeded, EMBED_PRICE_PER_MTOK, VISION_COST_PER_IMAGE,
                       WHISPER_PRICE_PER_MIN, enrich_chat_media, ingest_chat,
                       media_policy, pdf_enabled, scan_chat)
from kb_spaces import Spaces, load_spaces
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
    elif stage == 'hedex':
        print(f'  HedEx: {done} пакетов, {total} чанков, '
              f'потрачено ~${cost:.2f}', flush=True)
    elif stage == 'archive':
        print(f'  архивы: {done} просмотрено, {total} чанков, '
              f'потрачено ~${cost:.2f}', flush=True)


async def _dry_run(client, spaces) -> None:
    total = 0.0
    for space in spaces.all:
        media = media_policy(space.vision, space.voice)
        for chat_id in space.chats:
            print(f'Сканирую {chat_id} — вся история, на большом чате это '
                  f'минуты/десятки минут...', flush=True)
            st = await scan_chat(client, chat_id, progress=lambda n: print(
                f'  просмотрено сообщений: {n}', flush=True))
            embed = st.chars / 3 / 1e6 * EMBED_PRICE_PER_MTOK
            vision = st.images * VISION_COST_PER_IMAGE if media.vision else 0.0
            voice = (st.voice_seconds / 60 * WHISPER_PRICE_PER_MIN
                     if media.voice else 0.0)
            total += embed + vision + voice
            print(f'{chat_id} (область {space.slug}):')
            print(f'  сообщений: {st.messages}, ~{st.chars // 3} токенов '
                  f'-> эмбеддинги ~${embed:.2f}')
            mark = '' if media.vision else ' (vision выключен — не считается)'
            print(f'  картинок: {st.images} -> vision ~${vision:.2f}{mark}')
            mark = '' if media.voice else ' (whisper выключен — не считается)'
            print(f'  голосовых: {st.voice_seconds // 60} мин '
                  f'-> whisper ~${voice:.2f}{mark}')
    from kb_archive import archive_enabled
    from kb_hedex import hedex_enabled
    if not pdf_enabled():
        print('PDF: KB_PDF выключен — не считается')
    if not archive_enabled():
        print('Архивы: KB_ARCHIVE выключен — не считается')
    if not hedex_enabled():
        print('HedEx: KB_HEDEX выключен — не считается')
    store = open_store()
    for space in spaces.all:
        if not space.folder:
            continue
        if pdf_enabled():
            from kb_pdf import scan_pdfs
            files, pages = scan_pdfs(space.folder)
            # ~1800 символов на страницу мануала — грубая оценка
            pdf_cost = pages * 1800 / 3 / 1e6 * EMBED_PRICE_PER_MTOK
            total += pdf_cost
            print(f'PDF в {space.folder}: {files} файлов, {pages} страниц '
                  f'-> эмбеддинги ~${pdf_cost:.2f}')
        if archive_enabled():
            from kb_archive import scan_archives
            files, chars = scan_archives(store, space.folder, space=space.slug)
            arc_cost = chars / 3 / 1e6 * EMBED_PRICE_PER_MTOK
            total += arc_cost
            print(f'Архивы в {space.folder}: новых {files} '
                  f'-> эмбеддинги ~${arc_cost:.2f} (грубая оценка по листингам)')
        if hedex_enabled():
            from kb_hedex import scan_hdx
            files, chars = scan_hdx(store, space.folder, space=space.slug)
            hedex_cost = chars / 3 / 1e6 * EMBED_PRICE_PER_MTOK
            total += hedex_cost
            print(f'HedEx в {space.folder}: новых пакетов {files} '
                  f'-> эмбеддинги ~${hedex_cost:.2f} (без кросс-версионного '
                  f'дедупа — реально будет меньше)')
    print(f'\nИтого оценка: ~${total:.2f}')
    print('Подсказка: --max-cost N остановит боевой прогон при достижении N$.')


async def _local_only(spaces, max_cost: float | None) -> None:
    """--local-only: без подключения к Telegram (сессию не трогает, качалку
    можно не гасить) — весь пост-инжест конвейер kb_pipeline. Нужен только
    ключ эмбеддингов/OpenAI. Гонки с ночным джобом безопасны (state/md5),
    но осмысленнее не пересекаться по времени."""
    from kb_pipeline import run_post_ingest
    store = open_store()
    spent, stopped = await run_post_ingest(store, spaces, budget=max_cost,
                                           progress=_progress, report='print')
    print(f'\nЧанков в базе: {store.count()}. Потрачено: ~${spent:.2f}')
    if stopped:
        print('ЛИМИТ БЮДЖЕТА ДОСТИГНУТ — повторный запуск продолжит без '
              'двойной оплаты.')
    store.add_event('backfill',
                    f'Локальный прогон (--local-only) завершён, чанков: '
                    f'{store.count()}', spent)


async def _backfill(client, spaces, max_cost: float | None) -> None:
    store = open_store()
    # чат -> пространство; чаты качалки вне chats каталогизируем без инжеста
    # в RAG — иначе их файлы невидимы для /fw и кнопок 📎
    chat_space = {c: s for s in spaces.all for c in s.chats}
    chat_ids = list(chat_space)
    # только каталогизируем (без RAG): чаты качалки, не заявленные в chats
    catalog_only = {c: s for s in spaces.all for c in s.download_chats
                    if c not in chat_space}
    spent = 0.0
    stopped = False
    # политика обогащения — своя у каждой области (см. MediaPolicy)
    chat_media = {c: media_policy(s.vision, s.voice)
                  for s in spaces.all for c in s.chats}
    media_wanted = any(m.vision or m.voice for m in chat_media.values())

    def remaining() -> float | None:
        return None if max_cost is None else max(max_cost - spent, 0.0)

    # Этап 1: только текст (быстро и дёшево) — база отвечает уже после него.
    # Медиа входит в чанки плейсхолдерами, поэтому границы окончательные.
    for chat_id in chat_ids:
        print(f'[1/3] Текст: {chat_id}...', flush=True)
        try:
            stats = await ingest_chat(client, store, chat_id, min_id=0,
                                      progress=_progress, max_cost=remaining(),
                                      enrich_media=False,
                                      space=chat_space[chat_id].slug)
        except BudgetExceeded as e:
            spent += e.cost
            stopped = True
            break
        spent += stats.cost
        pruned_note = (f', удалено устаревших чанков: {stats.pruned}'
                       if stats.pruned else '')
        print(f'  {stats.messages} сообщений -> {stats.new_chunks} чанков'
              f'{pruned_note}, ~${stats.cost:.2f}')
    # Каталогизация документов из чатов качалки, не входящих в KB_CHAT_IDS:
    # без инжеста в RAG, только files/firmware — иначе файлы, скачанные из
    # «не-KB» чатов, невидимы для /fw и кнопок 📎
    if not stopped and catalog_only:
        from kb_firmware import document_filename, record_file
        from kb_ingest import fetch_topic_names, message_topic_id
        for chat_id, space in catalog_only.items():
            print(f'Каталог (без инжеста): {chat_id}...', flush=True)
            topics = await fetch_topic_names(client, chat_id)
            recorded = 0
            async for msg in client.iter_messages(chat_id, reverse=True):
                if msg.document is None:
                    continue
                fname = document_filename(msg)
                if not fname:
                    continue
                record_file(store, msg, fname,
                            topics.get(message_topic_id(msg), ''),
                            space=space.slug)
                recorded += 1
            print(f'  документов закаталогизировано: {recorded}')
    # Этап 2: vision/whisper — только наполнение кэша, чанки не трогаем
    if media_wanted and not stopped:
        for chat_id in chat_ids:
            print(f'[2/3] Медиа: {chat_id}...', flush=True)
            if not (chat_media[chat_id].vision or chat_media[chat_id].voice):
                print('  обогащение выключено для этой области — пропуск')
                continue
            try:
                n, cost = await enrich_chat_media(client, store, chat_id,
                                                  progress=_progress,
                                                  max_cost=remaining(),
                                                  media=chat_media[chat_id])
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
                                          max_cost=remaining(),
                                          space=chat_space[chat_id].slug,
                                          media=chat_media[chat_id])
            except BudgetExceeded as e:
                spent += e.cost
                stopped = True
                break
            spent += stats.cost
            print(f'  обновлено чанков: {stats.new_chunks}, ~${stats.cost:.2f}')
    if not stopped:
        # общий пост-инжест конвейер: каталог -> PDF -> архивы -> HedEx ->
        # экстракция -> бэкап (kb_pipeline, тот же путь, что у ночного джоба)
        from kb_pipeline import run_post_ingest
        more, stopped = await run_post_ingest(store, spaces,
                                              budget=remaining(),
                                              progress=_progress,
                                              report='print')
        spent += more
    else:
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
    parser.add_argument('--local-only', action='store_true',
                        help='только локальные конвейеры (архивы, HedEx, PDF, '
                             'экстракция) — БЕЗ подключения к Telegram: сессию '
                             'не трогает, качалку можно не останавливать')
    parser.add_argument('--space', default='', metavar='SLUG',
                        help='обработать только одну область знаний '
                             '(по умолчанию — все из spaces.toml)')
    args = parser.parse_args()

    spaces = load_spaces()
    if args.space:
        one = spaces.get(args.space)
        if one is None:
            raise SystemExit(f'нет области «{args.space}»; есть: '
                             + ', '.join(spaces.slugs))
        spaces = Spaces([one])

    if args.local_only:
        if args.dry_run:
            raise SystemExit('--local-only несовместим с --dry-run: оценка '
                             'печатается самими конвейерами')
        await _local_only(spaces, args.max_cost)
        return

    api_id = int(_require('TELEGRAM_API_ID'))
    api_hash = _require('TELEGRAM_API_HASH')
    if not any(s.chats for s in spaces.all):
        raise SystemExit('ни одна область не задаёт чаты-источники '
                         '(KB_CHAT_IDS или chats в spaces.toml)')

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
    # телеграмные предупреждения (FloodWait, обрывы) — в stderr, а не в тишину;
    # %(name)s подписывает источник (telethon.network.* vs наши модули)
    logging.basicConfig(level=logging.WARNING,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    print('Подключаюсь к Telegram...', flush=True)
    await client.start(phone=lambda: input('Enter your phone: '))
    print('Подключился.', flush=True)
    try:
        if args.dry_run:
            await _dry_run(client, spaces)
        else:
            await _backfill(client, spaces, args.max_cost)
    finally:
        await client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
