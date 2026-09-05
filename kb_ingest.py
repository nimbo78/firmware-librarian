"""Чанкинг сообщений Telegram, обогащение медиа и эмбеддинги OpenAI.

Используется ночным джобом качалки (librarian.py) и kb_backfill.py.
НЕ импортирует librarian — у того side-effects на импорте
(валидация env, создание клиента).

Обогащение медиа (опционально, флаги в .env):
- KB_VISION=1 — картинки через vision-модель (описание + OCR текста на них);
- KB_VOICE=1  — голосовые через Whisper (транскрипция).
Видео сознательно не обрабатываются. Результаты кэшируются в media_cache —
повторные прогоны (ретрай бэкфилла после пополнения бюджета) не платят дважды.
"""
from __future__ import annotations

import base64
import hashlib
import io
import logging
import math
import os
from dataclasses import dataclass

from telethon import errors

try:  # Telethon <=1.43: форум-топики в channels
    from telethon.tl.functions.channels import GetForumTopicsRequest
    _FORUM_PEER_KW = 'channel'
except ImportError:  # 1.44+: Telegram перенёс метод в messages, channel -> peer
    from telethon.tl.functions.messages import GetForumTopicsRequest
    _FORUM_PEER_KW = 'peer'

from kb_firmware import document_filename, record_file
from kb_store import Chunk

logger = logging.getLogger(__name__)

GAP_SECONDS = 30 * 60    # пауза, разрывающая беседу на отдельные чанки
CHUNK_MAX_CHARS = 4000   # ~1200 токенов для русского текста
EMBED_BATCH = 96
EMBED_MAX_CHARS = 8000   # потолок одного входа эмбеддера (лимит модели — 8192 токена)

# Оценки стоимости для расчёта бюджета (реальные цены могут меняться).
# Цена эмбеддингов берётся по EMBED_MODEL из карты; для модели не из карты
# или нестандартного тарифа — переопредели EMBED_PRICE_PER_MTOK в .env.
EMBED_PRICES = {
    'text-embedding-3-small': 0.02,
    'text-embedding-3-large': 0.13,
    'BAAI/bge-m3': 0.01,        # DeepInfra
}
EMBED_PRICE_PER_MTOK = (
    float(os.getenv('EMBED_PRICE_PER_MTOK', '0') or 0)
    or EMBED_PRICES.get(os.getenv('EMBED_MODEL', 'text-embedding-3-small'), 0.02))
VISION_COST_PER_IMAGE = 0.004   # $ / изображение (усреднённо)
WHISPER_PRICE_PER_MIN = 0.006   # $ / минута, whisper-1

MAX_IMAGE_BYTES = 6 * 1024 * 1024
MAX_VOICE_SECONDS = 15 * 60
# Vision API принимает только эти форматы; остальное конвертируем через Pillow
SUPPORTED_IMAGE_MIME = {'image/png', 'image/jpeg', 'image/gif', 'image/webp'}

VISION_PROMPT = (
    'Изображение из технического чата про сетевое оборудование Huawei. '
    'Опиши его одним-двумя предложениями по-русски. Если на изображении есть '
    'текст (вывод консоли, конфиг, шильдик с моделью, схема) — распознай и '
    'приведи его полностью.'
)

_openai = None
_embed = None


def openai_client():
    global _openai
    if _openai is None:
        from openai import AsyncOpenAI
        _openai = AsyncOpenAI()
    return _openai


def embed_client():
    """Клиент ТОЛЬКО для эмбеддингов. EMBED_API_BASE переключает на
    OpenAI-совместимого провайдера (DeepInfra и т.п.: base_url + ключ
    EMBED_API_KEY); пусто — общий клиент OpenAI. Ответы, vision и whisper
    всегда остаются на OpenAI (openai_client)."""
    global _embed
    if _embed is None:
        base = os.getenv('EMBED_API_BASE', '').strip()
        if base:
            key = os.getenv('EMBED_API_KEY', '').strip()
            if not key:
                raise RuntimeError('EMBED_API_BASE задан без EMBED_API_KEY')
            from openai import AsyncOpenAI
            _embed = AsyncOpenAI(base_url=base, api_key=key)
        else:
            _embed = openai_client()
    return _embed


def vision_enabled() -> bool:
    return os.getenv('KB_VISION', '0') == '1'


def voice_enabled() -> bool:
    return os.getenv('KB_VOICE', '0') == '1'


def pdf_enabled() -> bool:
    return os.getenv('KB_PDF', '0') == '1'


@dataclass
class IngestStats:
    messages: int = 0
    new_chunks: int = 0
    media_items: int = 0   # обработано медиа в этом прогоне (кэш не считается)
    cost: float = 0.0      # оценка потраченного, $
    pruned: int = 0        # удалено чанков с устаревшими границами (полный прогон)


class BudgetExceeded(Exception):
    """Достигнут --max-cost. Всё обработанное уже в базе/кэше — повторный
    запуск после пополнения бюджета продолжит с места остановки."""

    def __init__(self, cost: float):
        super().__init__(f'достигнут лимит бюджета: ~${cost:.2f}')
        self.cost = cost


async def embed_texts(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    client = embed_client()
    model = os.getenv('EMBED_MODEL', 'text-embedding-3-small')
    dim = int(os.getenv('EMBED_DIM', '512'))
    # dimensions (Matryoshka-обрезка) — фича моделей OpenAI; у сторонних
    # провайдеров размерность фиксирована моделью, параметр не передаём
    kwargs = {} if os.getenv('EMBED_API_BASE', '').strip() else {'dimensions': dim}
    out: list[list[float]] = []
    for i in range(0, len(texts), EMBED_BATCH):
        # Страховка от лимита 8192 токена на один вход. 20000 симв. было
        # мало: на технической документации выходит ~1.9 симв./токен, то
        # есть ~10500 токенов — прод падал с HTTP 400. EMBED_MAX_CHARS
        # безопасен даже при 1 симв./токен (CJK). Режется только то, что
        # уходит в эмбеддер: в базе чанк остаётся целиком (нужен LLM как
        # контекст), а сверхдлинные чанки и так рубит split_text.
        batch = [t[:EMBED_MAX_CHARS] for t in texts[i:i + EMBED_BATCH]]
        resp = await client.embeddings.create(model=model, input=batch, **kwargs)
        got = len(resp.data[0].embedding)
        if got != dim:
            raise RuntimeError(
                f'модель {model} вернула размерность {got} при EMBED_DIM={dim} '
                f'— поправь EMBED_DIM (все вектора в базе должны совпадать)')
        # нормализация: KNN sqlite-vec ранжирует по L2, что эквивалентно
        # косинусу только на единичных векторах; у OpenAI это no-op,
        # сторонние провайдеры нормализацию не гарантируют
        for d in resp.data:
            norm = math.sqrt(sum(x * x for x in d.embedding)) or 1.0
            out.append([x / norm for x in d.embedding])
    return out


def embed_cost(texts: list[str]) -> float:
    return sum(len(t) for t in texts) / 3 / 1e6 * EMBED_PRICE_PER_MTOK


def embed_cfg() -> str:
    return (f"{os.getenv('EMBED_MODEL', 'text-embedding-3-small')}"
            f":{int(os.getenv('EMBED_DIM', '512'))}")


def check_embed_cfg(store) -> str | None:
    """Защита от смешанных векторов: эмбеддинги разных моделей несравнимы,
    и молчаливая смена EMBED_MODEL/EMBED_DIM в .env дала бы мусорный поиск.

    None — конфигурация совпадает (или зафиксирована впервые); иначе —
    строка со старой конфигурацией: нужно прогнать kb_reembed.py."""
    current = embed_cfg()
    stored = store.get_state('embed_cfg')
    if not stored:
        store.set_state('embed_cfg', current)
        return None
    return None if stored == current else stored


async def describe_image(data: bytes, mime: str = 'image/jpeg') -> str:
    oa = openai_client()
    b64 = base64.b64encode(data).decode('ascii')
    model = os.getenv('KB_VISION_MODEL', 'gpt-5-mini')
    resp = await oa.chat.completions.create(
        model=model,
        messages=[{
            'role': 'user',
            'content': [
                {'type': 'text', 'text': VISION_PROMPT},
                {'type': 'image_url',
                 'image_url': {'url': f'data:{mime};base64,{b64}'}},
            ],
        }])
    return (resp.choices[0].message.content or '').strip()[:1500]


async def transcribe_voice(data: bytes) -> str:
    oa = openai_client()
    resp = await oa.audio.transcriptions.create(
        model='whisper-1', file=('voice.ogg', io.BytesIO(data)))
    return (resp.text or '').strip()[:3000]


def _to_jpeg(data: bytes) -> bytes | None:
    """BMP/TIFF и прочая экзотика -> JPEG. None, если Pillow формат не осилил
    (например, HEIC без pillow-heif) — такие кэшируются как пропуск."""
    try:
        from PIL import Image, ImageFile
        ImageFile.LOAD_TRUNCATED_IMAGES = True  # битые хвосты — не повод падать
        img = Image.open(io.BytesIO(data))
        if img.mode not in ('RGB', 'L'):
            img = img.convert('RGB')
        out = io.BytesIO()
        img.save(out, format='JPEG', quality=85)
        return out.getvalue()
    except Exception:
        return None


def _is_image(msg) -> bool:
    if getattr(msg, 'sticker', None) is not None:
        return False
    if getattr(msg, 'photo', None) is not None:
        return True
    if getattr(msg, 'gif', None) is not None:
        return False
    mime = getattr(msg.file, 'mime_type', '') if msg.file else ''
    return bool(mime and mime.startswith('image/'))


def _voice_duration(msg) -> int:
    if getattr(msg, 'voice', None) is None:
        return 0
    return int(getattr(msg.file, 'duration', 0) or 0)


async def enrich_message(store, msg, enrich_media: bool = True) -> tuple[str, float]:
    """Текст-довесок для медиа-сообщения и стоимость обработки.

    Медиа ВСЕГДА даёт плейсхолдер ([изображение] / [голосовое]), даже при
    выключенных флагах: сообщение попадает в чанк, и границы чанков не зависят
    от того, включено ли обогащение (иначе включение флага потом сдвинуло бы
    границы и наплодило дубликатов). Если обогащение включено и vision/whisper
    дали текст — плейсхолдер расширяется описанием: id чанка тот же, чанк
    обновляется по хэшу текста. Успех кэшируется, ошибки — нет (ретрай).
    """
    parts: list[str] = []
    cost = 0.0

    if _is_image(msg):
        text = ''
        if enrich_media and vision_enabled():
            key = f'img:{msg.chat_id}:{msg.id}'
            cached = store.get_media_text(key)
            if cached is None:
                try:
                    data = await msg.download_media(file=bytes)
                    if data and len(data) <= MAX_IMAGE_BYTES:
                        mime = (getattr(msg.file, 'mime_type', None) or 'image/jpeg'
                                if msg.file else 'image/jpeg')
                        if mime not in SUPPORTED_IMAGE_MIME:
                            converted = _to_jpeg(data)
                            if converted is None:
                                # формат не осилили — пропуск навсегда, не ретраим
                                store.put_media_text(key, '', 0.0)
                                logger.info('vision skip (unsupported format %s) '
                                            'for %s/%s', mime, msg.chat_id, msg.id)
                                data = None
                            else:
                                data, mime = converted, 'image/jpeg'
                        if data:
                            cached = await describe_image(data, mime)
                            cost += VISION_COST_PER_IMAGE
                            store.put_media_text(key, cached, VISION_COST_PER_IMAGE)
                    else:
                        store.put_media_text(key, '', 0.0)  # слишком большое
                except Exception as e:
                    logger.warning('vision failed for %s/%s: %s',
                                   msg.chat_id, msg.id, e)
            text = cached or ''
        parts.append(f'[изображение: {text}]' if text else '[изображение]')

    duration = _voice_duration(msg)
    if duration > 0:
        text = ''
        if enrich_media and voice_enabled():
            key = f'voice:{msg.chat_id}:{msg.id}'
            cached = store.get_media_text(key)
            if cached is None:
                if duration > MAX_VOICE_SECONDS:
                    store.put_media_text(key, '', 0.0)
                else:
                    try:
                        data = await msg.download_media(file=bytes)
                        if data:
                            cached = await transcribe_voice(data)
                            c = duration / 60.0 * WHISPER_PRICE_PER_MIN
                            cost += c
                            store.put_media_text(key, cached, c)
                    except Exception as e:
                        logger.warning('whisper failed for %s/%s: %s',
                                       msg.chat_id, msg.id, e)
            text = cached or ''
        parts.append(f'[голосовое: {text}]' if text else '[голосовое]')

    return ' '.join(parts), cost


async def enrich_chat_media(tg_client, store, chat_id: int, progress=None,
                            max_cost: float | None = None) -> tuple[int, float]:
    """Этап 2 бэкфилла: только наполняет media_cache (vision/whisper), чанки
    не трогает. Возвращает (обработано в этом прогоне, стоимость $)."""
    if not (vision_enabled() or voice_enabled()):
        return 0, 0.0
    done = 0
    cost = 0.0
    async for msg in tg_client.iter_messages(chat_id, reverse=True):
        if not (_is_image(msg) or _voice_duration(msg) > 0):
            continue
        _, c = await enrich_message(store, msg)
        if c > 0:
            done += 1
            cost += c
            if progress and done % 20 == 0:
                progress('media', done, 0, cost)
            if max_cost is not None and cost >= max_cost:
                raise BudgetExceeded(cost)
    return done, cost


async def fetch_topics(tg_client, chat_id: int) -> dict[int, tuple[str, bool]]:
    """{topic_id: (заголовок, закрыт ли)} — один RPC на страницу топиков.

    Пустой словарь = у чата нет форума (или Telegram не ответил). Вызывающий
    обязан трактовать это как «ограничений нет»: в обычной группе закрытых
    топиков не бывает, и молчать из-за неудачного запроса нельзя."""
    topics: dict[int, tuple[str, bool]] = {}
    offset_topic = 0
    try:
        while True:
            res = await tg_client(GetForumTopicsRequest(
                **{_FORUM_PEER_KW: chat_id}, offset_date=None, offset_id=0,
                offset_topic=offset_topic, limit=100))
            for t in res.topics:
                if hasattr(t, 'title'):  # у ForumTopicDeleted нет title
                    topics[t.id] = (t.title, bool(getattr(t, 'closed', False)))
            if len(res.topics) < 100:
                break
            offset_topic = res.topics[-1].id
    except (errors.RPCError, TypeError, ValueError):
        return {}  # чат без топиков
    return topics


async def fetch_topic_names(tg_client, chat_id: int) -> dict[int, str]:
    """Только заголовки — инжесту статус закрытости не нужен."""
    return {tid: title for tid, (title, _)
            in (await fetch_topics(tg_client, chat_id)).items()}


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
                      progress=None, max_cost: float | None = None,
                      enrich_media: bool = True, space: str = '') -> IngestStats:
    """Инжест сообщений chat_id от last_seen_id (или явного min_id) до конца.

    progress(stage, done, total, cost) — колбэк прогресса ('media' | 'embed').
    max_cost — потолок бюджета в $, при достижении кидает BudgetExceeded;
    state при этом не двигается, а кэш медиа и записанные чанки сохраняются,
    так что повторный запуск продолжает, не тратя деньги повторно.
    enrich_media=False — этап «сначала текст» бэкфилла: медиа идёт
    плейсхолдерами, vision/whisper не вызываются.
    Полный прогон (min_id=0) дополнительно удаляет чанки чата с устаревшими
    границами (prune) — история полностью пересобрана, они больше не валидны.
    """
    stats = IngestStats()
    state_key = f'last_seen_id:{chat_id}'
    if min_id is None:
        min_id = int(store.get_state(state_key, '0'))
    full_run = (min_id == 0)
    topic_names = await fetch_topic_names(tg_client, chat_id)
    records = []
    max_id = min_id
    async for msg in tg_client.iter_messages(chat_id, min_id=min_id, reverse=True):
        if msg.id > max_id:
            max_id = msg.id
        # Каталог файлов: все документы чата попадают в files/firmware
        if msg.document is not None:
            fname = document_filename(msg)
            if fname:
                try:
                    record_file(store, msg, fname,
                                topic_names.get(message_topic_id(msg), ''))
                except Exception as e:
                    logger.warning('file record failed for %s/%s: %s',
                                   chat_id, msg.id, e)
        text = (msg.raw_text or '').strip()
        extra, cost = await enrich_message(store, msg, enrich_media)
        if cost > 0:
            stats.media_items += 1
            stats.cost += cost
            if progress and stats.media_items % 20 == 0:
                progress('media', stats.media_items, 0, stats.cost)
            if max_cost is not None and stats.cost >= max_cost:
                raise BudgetExceeded(stats.cost)
        full = f'{text} {extra}'.strip()
        if not full:
            continue  # сервисные и пустые сообщения без обогащения
        line = f'[{msg.date:%Y-%m-%d %H:%M}] {_sender_name(msg)}: {full}'
        records.append((msg.id, message_topic_id(msg), msg.date,
                        _sender_name(msg), line))
    stats.messages = len(records)
    chunks = build_chunks(chat_id, records, topic_names)
    # Переэмбеддим новое И изменившееся (обогащение медиа меняет текст
    # чанка при том же id) — сравнение по хэшу текста
    known_hashes = store.chunk_hashes([c.id for c in chunks])
    new_chunks = [
        c for c in chunks
        if known_hashes.get(c.id) != hashlib.sha1(c.text.encode('utf-8')).hexdigest()
    ]
    # Порциями: не держим все вектора бэкфилла в памяти разом
    for i in range(0, len(new_chunks), EMBED_BATCH):
        part = new_chunks[i:i + EMBED_BATCH]
        vectors = await embed_texts([c.text for c in part])
        for c, v in zip(part, vectors):
            # space — пространство знаний чата (kb_spaces); пусто = по умолчанию
            c.embedding, c.space = v, space
        store.upsert_chunks(part)
        stats.cost += embed_cost([c.text for c in part])
        stats.new_chunks += len(part)
        if progress:
            progress('embed', min(i + EMBED_BATCH, len(new_chunks)),
                     len(new_chunks), stats.cost)
        if max_cost is not None and stats.cost >= max_cost:
            raise BudgetExceeded(stats.cost)
    if full_run:
        stats.pruned = store.prune_chunks(chat_id, [c.id for c in chunks])
    if max_id > min_id:
        store.set_state(state_key, str(max_id))
    return stats


@dataclass
class ScanStats:
    messages: int = 0
    chars: int = 0
    images: int = 0
    voice_seconds: int = 0


async def scan_chat(tg_client, chat_id: int, progress=None) -> ScanStats:
    """Для dry-run бэкфилла: объёмы по категориям без API-вызовов.
    progress(n) вызывается каждые 5000 сообщений — полная история большого
    чата листается минуты, без прогресса это выглядит как зависание."""
    st = ScanStats()
    async for msg in tg_client.iter_messages(chat_id, reverse=True):
        st.messages += 1
        st.chars += len(msg.raw_text or '')
        if _is_image(msg):
            st.images += 1
        st.voice_seconds += _voice_duration(msg)
        if progress and st.messages % 5000 == 0:
            progress(st.messages)
    return st
