# План внедрения «хранителя знаний» (RAG) для telegram-file-downloader

> Документ — техзадание для исполнителя (Claude Opus 4.8). Все архитектурные
> решения уже приняты и согласованы с владельцем — **не пересматривать**,
> реализовывать как написано. Прочитай также CLAUDE.md проекта.

## 1. Контекст и цель

Существующий сервис (см. CLAUDE.md) — Telethon **user-клиент** (не Bot API),
который слушает чаты и скачивает файлы на NAS Synology DS720+ (x86_64, 6 ГБ RAM,
Docker + Compose V2). Нужно добавить подсистему базы знаний:

1. **Ночной ingest**: раз в сутки (в `INGEST_HOUR`, по умолчанию 5:00 локального
   времени) выгружать новые сообщения из чатов `KB_CHAT_IDS`, резать на чанки,
   получать эмбеддинги через OpenAI API и складывать в локальную базу.
2. **Бэкфилл**: одноразовый прогон всей истории форумного чата (чат с топиками)
   с сохранением названий топиков в метаданных.
3. **Answer-бот**: отдельный бот (токен BotFather), добавленный в чат, отвечает
   на `/ask <вопрос>` и `@упоминание`, используя гибридный поиск по базе +
   OpenAI chat completion, с цитатами-ссылками на исходные сообщения.

Всё крутится на Synology в том же docker-compose. LLM локально НЕ запускаем —
только API. База — SQLite (sqlite-vec + FTS5), с заложенной возможностью
переезда на Qdrant (см. §8).

## 2. Зафиксированные архитектурные решения

| Решение | Обоснование (не менять) |
|---|---|
| Ingest живёт **внутри процесса качалки** | Одна Telethon-сессия = один процесс. Второе подключение с тем же `bot.session` убивает первое (`AuthKeyDuplicatedError`). Качалка уже онлайн 24/7. |
| Answer-бот — **отдельный контейнер**, вход по `bot_token` | Автоответы с личного аккаунта — риск бана. Bot-логин неинтерактивный, tty не нужен. |
| Хранилище — SQLite + sqlite-vec + FTS5, один файл | ~30–60k чанков: brute-force векторный поиск — миллисекунды, HNSW не нужен. FTS5 ловит артикулы («MA5608T»). |
| Весь доступ к базе — через `kb_store.py` | Единственная точка для будущего переезда на Qdrant (`KB_BACKEND=sqlite`). |
| Детерминированные ID чанков `sha1(chat_id:msg_first:msg_last)` | Идемпотентный re-ingest, дешёвый ретрай бэкфилла (уже существующие чанки не переэмбеддятся), тривиальная миграция. |
| Операционный state (`last_seen_id:<chat_id>`) — всегда в SQLite | При переезде на Qdrant уезжают только чанки. |
| Ingest идёт от `last_seen_id`, а не «за последние сутки» | Пропущенный запуск догоняется сам; state двигается только после успеха. |
| Вектора храним в базе (не только индекс) | Миграция без переэмбеддинга. |

## 3. Новые файлы

### 3.1 `kb_store.py` — хранилище

```python
@dataclass
class Chunk:
    chat_id: int; topic_id: int; topic_name: str
    date_from: str; date_to: str          # 'YYYY-MM-DD'
    msg_first: int; msg_last: int
    authors: str                          # CSV имён, обрезать до 500 символов
    text: str
    embedding: list | None = None
    # property id: sha1(f'{chat_id}:{msg_first}:{msg_last}').hexdigest()

@dataclass
class ScoredChunk:
    score: float; chat_id: int; topic_name: str
    date_from: str; msg_first: int; text: str

class SqliteVecStore:
    def __init__(self, db_path: str, embed_dim: int): ...
    def upsert_chunks(self, chunks: list[Chunk]) -> None: ...
    def existing_ids(self, ids: list[str]) -> set[str]: ...   # батчами по 500 (лимит параметров sqlite)
    def search(self, query_text: str, query_vector: list, top_k: int = 8,
               candidates: int = 24) -> list[ScoredChunk]: ...
    def get_state(self, key: str, default=None) -> str | None: ...
    def set_state(self, key: str, value: str) -> None: ...
    def count(self) -> int: ...
    def backup(self, dest: str | None = None) -> None: ...    # VACUUM INTO, по умолчанию db_path + '.bak'
    def close(self) -> None: ...

def open_store():   # фабрика: KB_BACKEND=sqlite -> SqliteVecStore(KB_DB_PATH, EMBED_DIM)
```

Схема (создавать в `__init__`, `IF NOT EXISTS`):

```sql
CREATE TABLE IF NOT EXISTS chunks(
    id TEXT UNIQUE NOT NULL, chat_id INTEGER NOT NULL,
    topic_id INTEGER NOT NULL DEFAULT 0, topic_name TEXT NOT NULL DEFAULT '',
    date_from TEXT NOT NULL, date_to TEXT NOT NULL,
    msg_first INTEGER NOT NULL, msg_last INTEGER NOT NULL,
    authors TEXT NOT NULL DEFAULT '', text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')));
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_vec USING vec0(embedding float[{dim}]);
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(text, tokenize='unicode61 remove_diacritics 2');
CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY, value TEXT NOT NULL);
```

**Критичные детали реализации:**

- Подключение: `sqlite3.connect(path)`, затем `enable_load_extension(True)` →
  `sqlite_vec.load(db)` → `enable_load_extension(False)`, затем
  `PRAGMA journal_mode=WAL` и `PRAGMA busy_timeout=5000` (базу одновременно
  пишет качалка и читает бот — два процесса).
- **Связь таблиц — через rowid таблицы `chunks`.** `chunks_vec` и `chunks_fts`
  вставляются с явным `rowid`, равным `chunks.rowid`.
  **НЕЛЬЗЯ** использовать `INSERT OR REPLACE` в `chunks` — REPLACE меняет rowid
  и вектор/FTS отвязываются. Upsert делать так: `SELECT rowid WHERE id=?` →
  если есть: `UPDATE chunks`, `DELETE FROM chunks_vec WHERE rowid=?`,
  `DELETE FROM chunks_fts WHERE rowid=?`; если нет: `INSERT`, взять `lastrowid`.
  Затем в обоих случаях вставить vec и fts. Всё — в одной транзакции (`with db:`).
- `chunks_fts` — **обычная** FTS5-таблица (не external content): иначе для
  удаления строки нужен старый текст, усложнение не окупается.
- Вектор сериализуется как `struct.pack(f'{len(v)}f', *v)` (float32 little-endian).
- KNN-запрос sqlite-vec:
  `SELECT rowid, distance FROM chunks_vec WHERE embedding MATCH ? AND k = ? ORDER BY distance`.
- FTS-запрос: пользовательский текст **нельзя** подставлять в MATCH сырым
  (синтаксис FTS5 упадёт на кавычках/минусах). Санитизация:
  `tokens = re.findall(r'\w+', query.lower())[:12]`, запрос
  `' OR '.join(f'"{t}"' for t in tokens)`,
  `SELECT rowid FROM chunks_fts WHERE chunks_fts MATCH ? ORDER BY rank LIMIT ?`,
  обёрнутый в `try/except sqlite3.OperationalError` (на случай экзотики).
- Гибрид — Reciprocal Rank Fusion: `score[rowid] += 1/(60 + rank)` по обоим
  спискам кандидатов (по `candidates=24` из каждого), сортировка по убыванию,
  топ `top_k`, затем добрать поля из `chunks` по rowid.
- `backup()`: удалить старый `.bak`, затем `VACUUM INTO ?`. Горячее копирование
  файла с WAL небезопасно — поэтому VACUUM INTO.
- `set_state`: `INSERT ... ON CONFLICT(key) DO UPDATE SET value=excluded.value`.
- В конце файла — `_selftest()` под `if __name__ == '__main__':` на временной
  базе (tempfile) с фейковыми ортогональными векторами dim=8: вставка 3 чанков,
  повторный upsert тех же (count не растёт), векторный хит, лексический хит по
  слову, которого нет в векторном топе, state, backup. **На Windows обязательно
  `store.close()` до выхода из TemporaryDirectory**, иначе PermissionError.
- Все `open()` в проекте — с `encoding='utf-8'`; во всех новых файлах —
  `from __future__ import annotations` (совместимость аннотаций).

### 3.2 `kb_ingest.py` — чанкер, эмбеддинги, пайплайн

Константы: `GAP_SECONDS = 30*60`, `CHUNK_MAX_CHARS = 4000` (~1200 токенов RU),
`EMBED_BATCH = 96`.

```python
def openai_client():                       # ленивый singleton AsyncOpenAI()
async def embed_texts(texts, client=None) -> list[list[float]]:
    # батчи по EMBED_BATCH, model=EMBED_MODEL, dimensions=EMBED_DIM,
    # каждый текст обрезать до 20000 символов (страховка от лимита 8192 токена)
async def fetch_topic_names(client, chat_id) -> dict[int, str]:
    # GetForumTopicsRequest(channel, offset_date=None, offset_id=0,
    #   offset_topic=<пагинация>, limit=100) из telethon.tl.functions.channels;
    # у ForumTopicDeleted нет .title -> hasattr; не-форум кидает RPCError -> вернуть {}
def message_topic_id(msg) -> int:
    # msg.reply_to и getattr(reply_to, 'forum_topic', False)
    #   -> reply_to.reply_to_top_id or reply_to.reply_to_msg_id or 1
    # иначе 1 (General / не-форум)
def build_chunks(chat_id, records, topic_names) -> list[Chunk]:
    # records: [(msg_id, topic_id, date, author, line)] хронологически.
    # Группировка по topic_id; внутри топика новый чанк при паузе > GAP_SECONDS
    # или превышении CHUNK_MAX_CHARS. Текст чанка: 'Топик «X»\n' + строки.
async def ingest_chat(tg_client, store, chat_id, min_id=None, progress=None) -> tuple[int, int]:
async def scan_chat(tg_client, chat_id) -> tuple[int, int]:   # (сообщений, символов) для dry-run
```

**Критичные детали:**

- Строка сообщения: `f'[{msg.date:%Y-%m-%d %H:%M}] {sender}: {msg.raw_text}'`.
  Использовать `raw_text` (без markdown), пустые/сервисные сообщения пропускать.
  Имя отправителя — из кэшированного `msg.sender` (first/last name → username →
  title → sender_id), **не** делать `await get_sender()` на каждое сообщение.
- `ingest_chat`: `min_id=None` → читать из state `last_seen_id:<chat_id>`;
  явный `min_id=0` (бэкфилл) — полный прогон.
  `iter_messages(chat_id, min_id=min_id, reverse=True)` — хронологический порядок.
- После построения чанков: `store.existing_ids()` → эмбеддить и upsert-ить
  **только новые**, порциями по `EMBED_BATCH` (embed → attach → upsert → progress).
  НЕ собирать все вектора в память разом (бэкфилл 40k чанков × 512 float —
  сотни МБ в python-списках).
- `set_state(last_seen_id)` — только в самом конце, если `max_id > min_id`.
  Исключение по пути → state не двинулся → завтра всё повторится.
- **НЕ импортировать `download_telegram_files` из kb-модулей** — у него
  side-effects на импорте (валидация env, создание клиента).

### 3.3 `kb_backfill.py` — CLI бэкфилла

- argparse: `--dry-run` (посчитать объём/стоимость без OpenAI-вызовов).
- Свой `TelegramClient('bot', ...)` с теми же ретрай-параметрами, что у качалки
  (скопировать 8 строк, не импортировать), но `flood_sleep_threshold=86400`
  (при полном прогоне истории FloodWait может быть долгим — спать, не падать).
- dry-run: по каждому чату из `KB_CHAT_IDS`: `scan_chat` → сообщений, ~токенов
  (`chars//3`), ~стоимость (`tokens/1e6*0.02` $). Итого по всем.
- Боевой режим: `ingest_chat(client, store, chat_id, min_id=0, progress=print)`,
  в конце `store.backup()` и `store.count()`.
- В docstring файла — **крупное предупреждение**: скрипт использует ту же
  сессию `bot.session`, что и качалка; запускать ТОЛЬКО при остановленной
  качалке:
  ```
  docker compose stop telegram-file-downloader
  docker compose run --rm telegram-file-downloader python kb_backfill.py --dry-run
  docker compose run --rm telegram-file-downloader python kb_backfill.py
  docker compose start telegram-file-downloader
  ```
- Порядок ввода в строй: **сначала полный бэкфилл, потом включать ночной
  ingest** (иначе границы чанков у суточных прогонов не совпадут с полным и
  появятся почти-дубли).

### 3.4 `kb_bot.py` — answer-бот (отдельный контейнер)

- Конфиг: `KB_BOT_TOKEN`, `KB_ANSWER_CHAT_IDS` — читать мягко (`os.getenv`);
  если не заданы — **не падать** (контейнер уйдёт в crash-loop), а логировать
  ошибку и `while True: await asyncio.sleep(3600)`. `OPENAI_API_KEY`
  отсутствует — warning на старте, ошибки отвечать текстом «попробуй позже».
- Клиент: `TelegramClient(KB_BOT_SESSION, api_id, api_hash, <ретраи как у качалки>)`,
  `KB_BOT_SESSION` по умолчанию `kb_bot`, в Docker — `/app/kb/kb_bot`
  (персистентность на томе, чтобы не создавать сессию на каждый рестарт).
  Старт: `client.start(bot_token=...)` — интерактива нет.
- Внешний цикл реконнекта — тот же паттерн exponential backoff 300→1800, что в
  `download_telegram_files.py:run()`.
- Хендлер `NewMessage`: чат ∈ `KB_ANSWER_CHAT_IDS`; вопрос из
  `^/ask(@botusername)?\s*(.*)` (regex, DOTALL, IGNORECASE) или из упоминания
  `@botusername` (username взять из `get_me()` после старта, вырезать упоминание
  из текста). `/ask` без текста → подсказка-usage. Кулдаун 30 сек на
  `sender_id` (dict в памяти, `time.monotonic`), при нарушении — короткий
  вежливый reply.
- Ответ: `embed_texts([question])` → `store.search(question, vec, top_k=8)` →
  пусто → «ничего не нашлось»; иначе prompt:
  - system: «Ты — ассистент чата по оборудованию Huawei. Отвечай кратко и
    по-русски, опираясь только на приведённый контекст из истории чата.
    Ссылайся на фрагменты номерами [1]. Если ответа в контексте нет — прямо
    скажи, не выдумывай.»
  - user: нумерованные фрагменты `[i] (топик «X», дата)\n<text>` + вопрос.
  - `chat.completions.create(model=ANSWER_MODEL, messages=...)`.
    **НЕ передавать `temperature` и `max_tokens`** — модели класса gpt-5 их
    не принимают / требуют `max_completion_tokens`; дефолты ок.
- К ответу добавить блок «Источники:» — до 5 ссылок
  `https://t.me/c/{str(chat_id)[4:]}/{msg_first}` (только для chat_id,
  начинающихся с `-100`). Обрезать ответ до 4000 символов (лимит Telegram 4096).
  Отправлять `event.reply(answer, link_preview=False)` — reply автоматически
  попадает в нужный топик форума.
- Логи: как в качалке (basicConfig INFO + telethon→WARNING); логировать вопрос
  (первые 100 символов) и ошибки.

### 3.5 `kb_search.py` — отладочный CLI

`python kb_search.py "вопрос"` → embed → `store.search` → печать топ-8:
score, топик, дата, msg_id, первые 200 символов текста (переносы → ` | `).
Нужен `OPENAI_API_KEY`. Без аргументов — usage и выход.

## 4. Правки существующих файлов

### 4.1 `download_telegram_files.py`

Добавить (менять существующую логику качалки МИНИМАЛЬНО):

```python
KB_CHAT_IDS = {int(x) for x in os.getenv('KB_CHAT_IDS', '').split(',') if x.strip()}
INGEST_HOUR = int(os.getenv('INGEST_HOUR', '5'))

def _seconds_until_hour(hour): ...   # до ближайшего hour:00 локального времени

async def kb_ingest_loop():
    if not KB_CHAT_IDS: return
    if not os.getenv('OPENAI_API_KEY'):
        logger.warning('KB_CHAT_IDS задан, но OPENAI_API_KEY отсутствует — ingest выключен')
        return
    from kb_ingest import ingest_chat      # ленивый импорт
    from kb_store import open_store
    store = open_store()
    while True:
        await asyncio.sleep(_seconds_until_hour(INGEST_HOUR))
        for chat_id in KB_CHAT_IDS:
            try:
                msgs, chunks = await ingest_chat(client, store, chat_id)
                logger.info('KB ingest %s: %d messages -> %d new chunks', chat_id, msgs, chunks)
            except Exception as e:
                logger.warning('KB ingest failed for %s: %s', chat_id, e)
        try: store.backup()
        except Exception as e: logger.warning('KB backup failed: %s', e)
```

В `run()` — `ingest_task = asyncio.create_task(kb_ingest_loop())` **один раз до
цикла** while (не внутри итераций реконнекта). Ошибка одного чата не должна
ронять ни цикл ingest, ни качалку.

### 4.2 `requirements.txt`

```
telethon==1.36.0
openai>=1.55,<3
sqlite-vec>=0.1.6
```

### 4.3 `Dockerfile`

- После FROM добавить `RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /var/lib/apt/lists/*`
  (иначе `TZ` не работает и INGEST_HOUR трактуется как UTC).
- COPY всех новых `kb_*.py`.
- `bot.session` копируется как раньше (см. CLAUDE.md — это намеренно).

### 4.4 `docker-compose.yml`

Существующему сервису добавить env: `TZ: ${TZ:-UTC}`,
`OPENAI_API_KEY: ${OPENAI_API_KEY:-}`, `KB_CHAT_IDS: ${KB_CHAT_IDS:-}`,
`KB_DB_PATH: /app/kb/kb.sqlite`, `EMBED_MODEL: ${EMBED_MODEL:-text-embedding-3-small}`,
`EMBED_DIM: ${EMBED_DIM:-512}`, `INGEST_HOUR: ${INGEST_HOUR:-5}`;
и том `- /volume1/docker/tg-kb:/app/kb`.

Новый сервис:

```yaml
  kb-bot:
    build: .
    command: ["python", "kb_bot.py"]
    restart: unless-stopped
    environment:
      TELEGRAM_API_ID: ${TELEGRAM_API_ID}
      TELEGRAM_API_HASH: ${TELEGRAM_API_HASH}
      TZ: ${TZ:-UTC}
      OPENAI_API_KEY: ${OPENAI_API_KEY:-}
      KB_BOT_TOKEN: ${KB_BOT_TOKEN:-}
      KB_ANSWER_CHAT_IDS: ${KB_ANSWER_CHAT_IDS:-}
      KB_DB_PATH: /app/kb/kb.sqlite
      KB_BOT_SESSION: /app/kb/kb_bot
      EMBED_MODEL: ${EMBED_MODEL:-text-embedding-3-small}
      EMBED_DIM: ${EMBED_DIM:-512}
      ANSWER_MODEL: ${ANSWER_MODEL:-gpt-5-mini}
    volumes:
      - /volume1/docker/tg-kb:/app/kb
    logging: *default-logging          # тот же json-file 10m x 3 (сделать YAML-якорь)
```

### 4.5 `CLAUDE.md`

После реализации добавить раздел про KB-подсистему (архитектура, env,
процедура бэкфилла с остановкой качалки, переезд на Qdrant) и **перепроверить
все ссылки на номера строк** в изменённых файлах — они сместятся.

## 5. Переменные `.env` (добавляет владелец)

```
OPENAI_API_KEY=sk-...
KB_BOT_TOKEN=<токен из BotFather>
KB_CHAT_IDS=<CSV id чатов-источников знаний>
KB_ANSWER_CHAT_IDS=<CSV id чатов, где бот отвечает>
TZ=Europe/Moscow          # или свой пояс — от него зависит INGEST_HOUR
```

Подготовка бота (делает владелец): BotFather → `/newbot` → токен;
`/setprivacy` → **Disable** (без этого упоминания не доходят до бота);
добавить бота в чат.

## 6. Порядок реализации и верификация

1. `kb_store.py` + selftest: `python kb_store.py` локально
   (`pip install sqlite-vec`, OpenAI не нужен) → `kb_store selftest: OK`.
2. `kb_ingest.py`, `kb_backfill.py`, `kb_search.py`, `kb_bot.py`,
   правки качалки и Docker-файлов. Проверить синтаксис всего:
   `python -m py_compile *.py`.
3. Деплой на NAS (`./redeploy.sh`), затем dry-run бэкфилла (процедура из §3.3)
   — проверить счётчики и стоимость.
4. Боевой бэкфилл → `kb_search.py "<реальный вопрос про huawei>"` изнутри
   контейнера (`docker compose run --rm telegram-file-downloader python kb_search.py "..."`)
   — чанки релевантны.
5. Запустить качалку обратно, проверить `/ask` и `@упоминание` в чате: ответ
   в том же топике, ссылки кликабельны, кулдаун работает, `/ask` без текста
   даёт подсказку.
6. Ночной job: временно выставить `INGEST_HOUR` на ближайший час → в логах
   `KB ingest ...`, `last_seen_id` сдвинулся, появился `kb.sqlite.bak`.

## 7. Явные НЕ-цели (не делать)

- Не писать `QdrantStore` и не добавлять qdrant-client в зависимости.
- Не менять логику скачивания файлов, форматы `downloaded_files.txt`, ретраи.
- Не добавлять тесты/CI сверх selftest в `kb_store.py`.
- Не делать OCR/капшены картинок (только текст и подписи к медиа).
- Не делать live-захват сообщений ботом — единственный путь ингеста:
  user-клиент по расписанию.

## 8. Переезд на Qdrant (справочно, для будущего)

Триггеры: миллионы чанков; сетевые потребители базы; тяжёлые фильтры.
Процедура: контейнер Qdrant → класс `QdrantStore` с тем же интерфейсом →
ветка в `open_store()` по `KB_BACKEND=qdrant` → скрипт переливки
(`SELECT` чанков + готовых векторов из sqlite, upsert в Qdrant, id сохраняются)
→ лексическую часть гибрида заменить на sparse-вектора (BM25 через fastembed)
внутри `QdrantStore`. State и журнал ингеста остаются в SQLite.
