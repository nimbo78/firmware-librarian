import asyncio
import hashlib
import logging
import os
import re
import time
from datetime import datetime

from telethon import TelegramClient, errors, events
from telethon.tl.types import DocumentAttributeFilename

from kb_spaces import load_spaces
from tg_conn import proxy_kwargs


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise ValueError(f'{name} must be provided')
    return val


API_ID = int(_require('TELEGRAM_API_ID'))
API_HASH = _require('TELEGRAM_API_HASH')
_require('CHAT_IDS')  # без spaces.toml качалке нужен хотя бы один чат

# Пространства знаний (kb_spaces): без spaces.toml — одно неявное из env,
# то есть прежнее поведение. Скачивание идёт по download_chats пространства
# в его же папку, инжест — по chats.
SPACES = load_spaces()
# чат -> пространство, из которого качаем файлы (у чата одно пространство)
DOWNLOAD_SPACES = {chat: s for s in SPACES.all for chat in s.download_chats}
CHAT_IDS = set(DOWNLOAD_SPACES)
KB_CHAT_SPACES = {chat: s for s in SPACES.all for chat in s.chats}

# База знаний: чаты для ночного инжеста (пусто — подсистема выключена)
KB_CHAT_IDS = set(KB_CHAT_SPACES)
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


def _kb_record_file(message, file_name: str, space: str = '') -> None:
    """Каталог файлов: метаданные документа + разбор имени прошивки."""
    if not (KB_CHAT_IDS or KB_ADMIN_IDS):
        return
    try:
        from kb_firmware import record_file
        record_file(_kb_store_lazy(), message, file_name, space=space)
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

# Журнал дедупликации лежит в папке пространства: у каждой папки свой
# (файл с одним именем в разных пространствах — разные файлы)
for _space in {s.slug: s for s in DOWNLOAD_SPACES.values()}.values():
    os.makedirs(_space.folder, exist_ok=True)

# %(name)s подписывает источник: telethon.network.* — сетевой слой Telegram,
# downloader/kb_* — наши модули (иначе непонятно, чей варнинг)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('downloader')
logging.getLogger('telethon').setLevel(logging.WARNING)

_MD5_RE = re.compile(r'^[0-9a-f]{32}$')


def journal_path(folder: str) -> str:
    return os.path.join(folder, 'downloaded_files.txt')


def load_downloaded_files(folder: str) -> dict:
    result: dict = {}
    log = journal_path(folder)
    if not os.path.exists(log):
        return result
    with open(log, 'r', encoding='utf-8') as f:
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


# Журнал на папку: у каждого пространства свой (файл с тем же именем в
# другом пространстве — другой файл, дедуплицировать их вместе нельзя)
_journals: dict[str, dict] = {}


def downloaded_files(folder: str) -> dict:
    if folder not in _journals:
        _journals[folder] = load_downloaded_files(folder)
    return _journals[folder]


def save_downloaded_file(folder: str, file_name: str, file_md5: str) -> None:
    downloaded_files(folder)[file_name] = file_md5
    with open(journal_path(folder), 'a', encoding='utf-8') as f:
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
    # чат может быть и качаемым, и источником знаний: dl_space — откуда
    # качаем (None = только каталогизируем), space — чьё это знание
    dl_space = DOWNLOAD_SPACES.get(event.chat_id)
    space = dl_space or KB_CHAT_SPACES.get(event.chat_id)
    if space is None:
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
    _kb_record_file(event.message, file_name, space=space.slug)

    if dl_space is None:
        return

    if '.' not in file_name:
        return
    file_extension = file_name.rsplit('.', 1)[-1].lower()
    if file_extension not in dl_space.download_extensions:
        return

    folder = dl_space.folder
    file_path = os.path.join(folder, file_name)

    journal = downloaded_files(folder)
    if file_name in journal and os.path.exists(file_path):
        if journal[file_name] == calculate_md5(file_path):
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
    save_downloaded_file(folder, file_name, file_md5)
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
    from kb_ingest import ingest_chat, media_policy
    from kb_store import open_store
    store = open_store()
    logger.info('KB ingest scheduled daily at %02d:00 for chats %s '
                '(+ /ingest по запросу)', INGEST_HOUR,
                ', '.join(f'{c} [{s.slug}]'
                          for c, s in sorted(KB_CHAT_SPACES.items())))
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
        from kb_ingest import check_embed_cfg, embed_cfg
        mismatch = check_embed_cfg(store)
        if mismatch:
            # смешанные вектора = молча мусорный поиск; лучше не инжестить
            logger.error('KB ingest blocked: embed cfg %s в базе, %s в env — '
                         'прогони kb_reembed.py', mismatch, embed_cfg())
            store.add_event('error',
                            f'Инжест заблокирован: база на {mismatch}, env '
                            f'{embed_cfg()} — нужен kb_reembed.py')
            store.set_state('ingest_request', '')
            if due_daily:
                store.set_state('ingest_done_date', today)
            continue
        store.set_state('ingest_request', '')
        if due_daily:
            store.set_state('ingest_done_date', today)
        if on_demand:
            logger.info('KB ingest: on-demand run requested via /ingest')
        for chat_id, space in KB_CHAT_SPACES.items():
            try:
                stats = await ingest_chat(
                    client, store, chat_id, space=space.slug,
                    # обогащение медиа — решение области: в чужом чате
                    # платить за каждый скриншот каждую ночь незачем
                    media=media_policy(space.vision, space.voice))
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
        # Прогресс длинных шагов: в логи и в state (его показывает /status).
        # Троттлинг — состояние обновляется не чаще раза в 20 секунд.
        _last_note = [0.0]
        _labels = {'pdf': 'PDF', 'archive': 'архивы', 'hedex': 'HedEx',
                   'embed': 'эмбеддинги', 'media': 'медиа'}

        def _pipeline_progress(kind, done, total, cost):
            now = time.monotonic()
            if now - _last_note[0] < 20:
                return
            _last_note[0] = now
            msg = f'{_labels.get(kind, kind)}: обработано {done}'
            if total:
                # у медиа и эмбеддингов total — сколько всего работы,
                # у документных шагов — сколько чанков получилось
                msg += (f' из {total}' if kind in ('media', 'embed')
                        else f', чанков {total}')
            if cost:
                msg += f', ~${cost:.2f}'
            logger.info('KB pipeline: %s', msg)
            try:
                store.set_state('pipeline_status',
                                f'{datetime.now():%H:%M} {msg}')
            except Exception as e:
                logger.debug('pipeline status skipped: %s', e)

        try:
            # каталог -> PDF -> архивы -> HedEx -> экстракция -> бэкап;
            # шаги изолированы внутри, отчёт — событиями админу
            from kb_pipeline import run_post_ingest
            await run_post_ingest(store, SPACES,
                                  progress=_pipeline_progress)
        except Exception as e:
            logger.warning('KB post-ingest pipeline failed: %s', e)
            store.add_event('error', f'Пост-инжест конвейер упал: {e}')


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
