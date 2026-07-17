import asyncio
import hashlib
import logging
import os
import re
from datetime import datetime

from telethon import TelegramClient, errors, events
from telethon.tl.types import DocumentAttributeFilename

from tg_conn import proxy_kwargs


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise ValueError(f'{name} must be provided')
    return val


API_ID = int(_require('TELEGRAM_API_ID'))
API_HASH = _require('TELEGRAM_API_HASH')
CHAT_IDS = {int(x) for x in _require('CHAT_IDS').split(',') if x.strip()}
FILE_EXTENSIONS = {
    e.strip().lower()
    for e in os.getenv('FILE_EXTENSIONS', 'pdf,jpg,png').split(',')
    if e.strip()
}
DOWNLOAD_FOLDER = os.getenv('DOWNLOAD_FOLDER', './downloads')

# База знаний: чаты для ночного инжеста (пусто — подсистема выключена)
KB_CHAT_IDS = {int(x) for x in os.getenv('KB_CHAT_IDS', '').split(',') if x.strip()}
INGEST_HOUR = int(os.getenv('INGEST_HOUR', '5'))
KB_ADMIN_IDS = {int(x) for x in os.getenv('KB_ADMIN_IDS', '').split(',') if x.strip()}

_kb_event_store = None


def _kb_store_lazy():
    global _kb_event_store
    if _kb_event_store is None:
        from kb_store import open_store
        _kb_event_store = open_store()
    return _kb_event_store


def _kb_event(kind: str, text: str, cost: float = 0.0) -> None:
    """Событие в очередь админ-уведомлений (kb-bot разошлёт в личку).
    Любая ошибка KB-подсистемы не должна ломать скачивание файлов."""
    if not (KB_CHAT_IDS or KB_ADMIN_IDS):
        return
    try:
        _kb_store_lazy().add_event(kind, text, cost)
    except Exception as e:
        logger.debug('kb event skipped: %s', e)


def _kb_record_file(message, file_name: str) -> None:
    """Каталог файлов: метаданные документа + разбор имени прошивки."""
    if not (KB_CHAT_IDS or KB_ADMIN_IDS):
        return
    try:
        from kb_firmware import record_file
        record_file(_kb_store_lazy(), message, file_name)
    except Exception as e:
        logger.debug('kb file record skipped: %s', e)


def _kb_set_file_md5(message, file_md5: str) -> None:
    if not (KB_CHAT_IDS or KB_ADMIN_IDS):
        return
    try:
        if message.document is not None:
            _kb_store_lazy().set_file_md5(message.document.id, file_md5)
    except Exception as e:
        logger.debug('kb md5 update skipped: %s', e)

if not CHAT_IDS:
    raise ValueError('CHAT_IDS must contain at least one chat id')

downloaded_files_log = os.path.join(DOWNLOAD_FOLDER, 'downloaded_files.txt')
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# %(name)s подписывает источник: telethon.network.* — сетевой слой Telegram,
# downloader/kb_* — наши модули (иначе непонятно, чей варнинг)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('downloader')
logging.getLogger('telethon').setLevel(logging.WARNING)

_MD5_RE = re.compile(r'^[0-9a-f]{32}$')


def load_downloaded_files() -> dict:
    result: dict = {}
    if not os.path.exists(downloaded_files_log):
        return result
    with open(downloaded_files_log, 'r', encoding='utf-8') as f:
        for line in f.read().splitlines():
            if not line:
                continue
            head, sep, tail = line.partition(',')
            if not sep:
                continue
            if _MD5_RE.match(head):
                # new format: <md5>,<file_name>
                result[tail] = head
            elif _MD5_RE.match(tail):
                # legacy format: <file_name>,<md5>
                result[head] = tail
    return result


downloaded_files = load_downloaded_files()


def save_downloaded_file(file_name: str, file_md5: str) -> None:
    downloaded_files[file_name] = file_md5
    with open(downloaded_files_log, 'a', encoding='utf-8') as f:
        f.write(f'{file_md5},{file_name}\n')


def calculate_md5(file_path: str) -> str:
    hash_md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


client = TelegramClient(
    'bot', API_ID, API_HASH,
    connection_retries=-1,
    retry_delay=300,
    auto_reconnect=True,
    request_retries=5,
    timeout=30,
    flood_sleep_threshold=120,
    **proxy_kwargs(),
)


@client.on(events.NewMessage)
async def handler(event):
    in_download = event.chat_id in CHAT_IDS
    if not (in_download or event.chat_id in KB_CHAT_IDS):
        return

    if not (event.message.media and hasattr(event.message.media, 'document')):
        return

    document = event.message.media.document
    for attribute in document.attributes:
        if isinstance(attribute, DocumentAttributeFilename):
            file_name = attribute.file_name
            break
    else:
        return

    file_name = os.path.basename(file_name)
    if not file_name or file_name in ('.', '..'):
        return

    # Каталог: метаданные ВСЕХ документов из наблюдаемых чатов (включая
    # расширения, которые не скачиваем, — прошивки ищутся по /fw без файла)
    _kb_record_file(event.message, file_name)

    if not in_download:
        return

    if '.' not in file_name:
        return
    file_extension = file_name.rsplit('.', 1)[-1].lower()
    if file_extension not in FILE_EXTENSIONS:
        return

    file_path = os.path.join(DOWNLOAD_FOLDER, file_name)

    if file_name in downloaded_files and os.path.exists(file_path):
        if downloaded_files[file_name] == calculate_md5(file_path):
            logger.info('File %s already downloaded with the same MD5 hash.', file_name)
            return

    try:
        await event.message.download_media(file=file_path)
    except (OSError, asyncio.TimeoutError, errors.RPCError) as e:
        logger.warning('Download failed for %s: %s', file_name, e)
        _kb_event('error', f'Ошибка скачивания {file_name}: {e}')
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass
        return

    file_md5 = calculate_md5(file_path)
    save_downloaded_file(file_name, file_md5)
    logger.info('Downloaded %s to %s', file_name, file_path)
    _kb_set_file_md5(event.message, file_md5)
    size_mb = os.path.getsize(file_path) / 1e6
    _kb_event('download', f'Скачан {file_name} ({size_mb:.1f} МБ)')


async def kb_ingest_loop() -> None:
    """Инжест базы знаний: ночью в INGEST_HOUR и по запросу админа
    (/ingest в kb-bot ставит state ingest_request). Живёт в этом процессе,
    потому что одна Telethon-сессия не может использоваться двумя процессами
    одновременно. Тик раз в минуту, сделанность суток — в state."""
    if not KB_CHAT_IDS:
        return
    if not os.getenv('OPENAI_API_KEY'):
        logger.warning('KB_CHAT_IDS задан, но OPENAI_API_KEY отсутствует — '
                       'ночной ingest выключен')
        return
    from kb_ingest import ingest_chat, pdf_enabled
    from kb_store import open_store
    store = open_store()
    logger.info('KB ingest scheduled daily at %02d:00 for chats %s '
                '(+ /ingest по запросу)', INGEST_HOUR, sorted(KB_CHAT_IDS))
    while True:
        await asyncio.sleep(60)
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')
        due_daily = (now.hour == INGEST_HOUR
                     and store.get_state('ingest_done_date') != today)
        on_demand = store.get_state('ingest_request', '') == '1'
        if not (due_daily or on_demand):
            continue
        if not client.is_connected():
            continue  # флажки не сбрасываем — попробуем через минуту
        store.set_state('ingest_request', '')
        if due_daily:
            store.set_state('ingest_done_date', today)
        if on_demand:
            logger.info('KB ingest: on-demand run requested via /ingest')
        for chat_id in KB_CHAT_IDS:
            try:
                stats = await ingest_chat(client, store, chat_id)
                logger.info('KB ingest %s: %d messages -> %d new chunks, '
                            'media %d, ~$%.2f', chat_id, stats.messages,
                            stats.new_chunks, stats.media_items, stats.cost)
                store.add_event('ingest',
                                f'Ночной инжест {chat_id}: {stats.messages} сообщ. '
                                f'-> {stats.new_chunks} чанков, медиа {stats.media_items}',
                                stats.cost)
            except Exception as e:
                logger.warning('KB ingest failed for %s: %s', chat_id, e)
                store.add_event('error', f'Инжест {chat_id} упал: {e}')
        try:
            from kb_firmware import (auto_resolve_firmware, link_local_files,
                                     reparse_files)
            linked = link_local_files(store, DOWNLOAD_FOLDER)
            reparsed = reparse_files(store)  # новые регулярки -> старые файлы
            resolved = auto_resolve_firmware(store)  # medium, подтверждённые парсером
            confirmed_series = store.confirm_all_series()
            if linked or reparsed or resolved or confirmed_series:
                logger.info('KB catalog: linked %d, reparsed %d, auto-resolved %d, '
                            'series %d', linked, reparsed, resolved, confirmed_series)
                store.add_event('catalog',
                                f'Каталог: привязано {linked}, связок +{reparsed}, '
                                f'авто-снято medium {resolved}, серий {confirmed_series}')
        except Exception as e:
            logger.warning('KB catalog maintenance failed: %s', e)
        if pdf_enabled():
            try:
                from kb_pdf import ingest_pdfs
                files, chunks, cost = await ingest_pdfs(store, DOWNLOAD_FOLDER)
                if files:
                    logger.info('KB PDF ingest: %d files -> %d chunks, ~$%.2f',
                                files, chunks, cost)
                    store.add_event('pdf',
                                    f'PDF-инжест: {files} файлов -> {chunks} чанков',
                                    cost)
            except Exception as e:
                logger.warning('KB PDF ingest failed: %s', e)
                store.add_event('error', f'PDF-инжест упал: {e}')
        try:
            from kb_extract import run_extraction
            fw_added, dev_added, cost = await run_extraction(store)
            if fw_added or dev_added:
                logger.info('KB extract: %d firmware links, %d series, ~$%.2f',
                            fw_added, dev_added, cost)
                store.add_event('extract',
                                f'LLM-экстракция: {fw_added} связок прошивок, '
                                f'{dev_added} серий', cost)
            pending = store.pending_review_count()
            if pending:
                store.add_event('review',
                                f'{pending} записей каталога ждут подтверждения — /review')
        except Exception as e:
            logger.warning('KB extract failed: %s', e)
        try:
            store.backup()
        except Exception as e:
            logger.warning('KB backup failed: %s', e)
            store.add_event('error', f'Бэкап базы знаний упал: {e}')


async def run() -> None:
    initial_backoff = 300
    max_backoff = 1800
    backoff = initial_backoff
    ingest_task = asyncio.create_task(kb_ingest_loop())  # держим ссылку: живёт поверх реконнектов
    while True:
        try:
            await client.start(phone=lambda: input('Enter your phone: '))
            backoff = initial_backoff
            await client.run_until_disconnected()
        except (ConnectionError, OSError, asyncio.TimeoutError) as e:
            logger.warning('Top-level reconnect in %ds after: %s', backoff, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
        else:
            logger.info('Client disconnected cleanly, exiting loop')
            return


def main() -> None:
    logger.info('Starting client...')
    asyncio.run(run())


if __name__ == '__main__':
    main()
