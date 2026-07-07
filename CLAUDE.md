# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Назначение

Два сервиса в одном docker-compose на NAS Synology DS720+:

1. **Качалка** ([download_telegram_files.py](download_telegram_files.py)) — Telethon user-клиент, слушает чаты `CHAT_IDS`, скачивает документы с подходящим расширением, дедуплицирует по имени + MD5, складывает на том NAS. Плюс ночной инжест базы знаний (см. ниже).
2. **KB-бот** ([kb_bot.py](kb_bot.py)) — бот (токен BotFather), отвечает на `/ask` и `@упоминание` в чатах `KB_ANSWER_CHAT_IDS`, используя RAG по истории чатов: гибридный поиск sqlite-vec + FTS5, ответы через OpenAI API.

## Команды

- Локальный запуск качалки: `pip install -r requirements.txt` затем `python download_telegram_files.py`. При отсутствии валидного `bot.session` Telethon запросит номер телефона в интерактиве — поэтому первый запуск делается с tty.
- Docker: `docker compose up --build` (Compose V2 — без дефиса). Переменные окружения подтягиваются из `.env` рядом с `docker-compose.yml`.
- Передеплой на NAS: [`./redeploy.sh`](redeploy.sh) — проверяет наличие `.env` и `bot.session`, делает `down --remove-orphans` + `up --build -d` + выводит статус и последние 30 строк логов.
- Selftest хранилища (не требует Telegram и OpenAI): `pip install sqlite-vec && python kb_store.py`.
- Бэкфилл истории в базу знаний — см. процедуру в разделе «База знаний».
- Отладочный поиск по базе: `docker compose run --rm telegram-file-downloader python kb_search.py "вопрос"`.
- Тестов, линтеров и CI в репозитории нет (кроме selftest в `kb_store.py`).

## Переменные окружения

Шаблон — [.env.example](.env.example): `cp .env.example .env` и заполнить.

Качалка:

- `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` — обязательны.
- `CHAT_IDS` — обязательна, CSV целых чисел (id чатов, как видит их Telegram, часто отрицательные).
- `FILE_EXTENSIONS` — CSV расширений **без точки**, по умолчанию `pdf,jpg,png`. Регистр и пробелы нормализуются ([download_telegram_files.py:22-26](download_telegram_files.py#L22-L26), [download_telegram_files.py:119-120](download_telegram_files.py#L119-L120)).
- `DOWNLOAD_FOLDER` — по умолчанию `./downloads`; в Docker переопределяется на `/app/downloads`.

База знаний (все опциональны — без них KB-подсистема выключена):

- `OPENAI_API_KEY` — эмбеддинги и ответы.
- `KB_CHAT_IDS` — CSV чатов-источников знаний (ночной инжест).
- `KB_BOT_TOKEN`, `KB_ANSWER_CHAT_IDS` — токен BotFather и чаты, где бот отвечает.
- `TZ` — часовой пояс контейнера; от него зависит `INGEST_HOUR` (по умолчанию 5).
- `EMBED_MODEL`/`EMBED_DIM` (`text-embedding-3-small`/512), `ANSWER_MODEL` (`gpt-5-mini`), `KB_DB_PATH` (`/app/kb/kb.sqlite`), `KB_BACKEND` (`sqlite`).

## Архитектура и неочевидные детали

- **Качалка — user-клиент Telethon, а не Bot API.** Имя `bot.session` и переменная `client` вводят в заблуждение: [download_telegram_files.py:190](download_telegram_files.py#L190) вызывает `client.start(phone=...)` — аутентификация идёт по телефону пользователя. Бот-токен использует только `kb_bot.py` (отдельная сессия на томе `/app/kb`).
- **`bot.session` НЕ коммитится (в `.gitignore`), но копируется в Docker-образ** ([Dockerfile:14](Dockerfile#L14)). Файл = полный доступ к аккаунту, поэтому в репозиторий не попадает, но обязан присутствовать в каталоге сборки на хосте, где выполняется `docker compose build` (иначе сборка упадёт на `COPY bot.session`, а без сессии контейнер заблокировался бы на интерактивном вводе телефона). Создаётся/пересоздаётся локальным запуском с tty; на новом хосте файл переносится вручную (не через git).
- **Одна Telethon-сессия = один процесс.** Второе подключение с тем же `bot.session` убивает первое (`AuthKeyDuplicatedError`). Поэтому ночной инжест живёт внутри процесса качалки, а `kb_backfill.py` запускается только при остановленной качалке.
- **Состояние дедупликации живёт в томе загрузок.** `downloaded_files.txt` пишется в `DOWNLOAD_FOLDER` ([download_telegram_files.py:36](download_telegram_files.py#L36)); в [docker-compose.yml:19](docker-compose.yml#L19) этот путь смонтирован на NAS `/volume1/public/Repository/Hardware/Huawei/fromChat`. Журнал переживает перезапуски, но привязан к конкретному пути NAS — при смене тома история дедупликации теряется.
- **Формат журнала: `<md5>,<file_name>`** ([download_telegram_files.py:75](download_telegram_files.py#L75)). MD5 — фиксированные 32 hex-символа, поэтому `partition(',')` корректно отделяет хэш от имени, в котором могут быть запятые. На чтение поддерживается также старый формат `<file_name>,<md5>` ([download_telegram_files.py:53-66](download_telegram_files.py#L53-L66)) — миграция файла на диске не требуется.
- **Дедупликация** ([download_telegram_files.py:125-128](download_telegram_files.py#L125-L128)): ключ — имя файла + MD5. При каждом событии перехэшируется уже лежащий локально файл. Если `downloaded_files.txt` потерян, но файлы остались — они скачаются повторно и **затрут** локальные копии. Файлы с тем же именем и новым содержимым обрабатываются как «перезапись», старая версия теряется.
- **Огрызки от обрывов скачивания удаляются.** `download_media` обёрнут в try/except ([download_telegram_files.py:130-139](download_telegram_files.py#L130-L139)); при `OSError`/`TimeoutError`/`RPCError` частично скачанный файл удаляется, в журнал ничего не пишется.
- **Path traversal невозможен.** Имя из Telegram прогоняется через `os.path.basename` и проверяется на `.`/`..` ([download_telegram_files.py:113-115](download_telegram_files.py#L113-L115)) до сборки `file_path`.
- **Обрабатываются только документы** ([download_telegram_files.py:102](download_telegram_files.py#L102)): `event.message.media` должен иметь атрибут `document`. Фото, отправленные как изображения (не как файл), пропускаются — Telegram кладёт их в `MessageMediaPhoto`. Документы без расширения отсекаются явно ([download_telegram_files.py:117-121](download_telegram_files.py#L117-L121)).

## База знаний (RAG)

- **Хранилище — один файл SQLite** (`/volume1/docker/tg-kb/kb.sqlite`): таблица `chunks` + векторная `chunks_vec` (sqlite-vec) + полнотекстовая `chunks_fts` (FTS5) + `state`. Гибридный поиск: KNN + FTS, слияние через Reciprocal Rank Fusion ([kb_store.py](kb_store.py)).
- **Весь доступ к базе — только через `kb_store.open_store()`.** Это точка будущего переезда на Qdrant (`KB_BACKEND`): новый класс с тем же интерфейсом + переливка чанков с готовыми векторами. Вектора хранятся в базе именно ради миграции без переэмбеддинга; state всегда остаётся в SQLite.
- **rowid-связка таблиц.** `chunks_vec` и `chunks_fts` привязаны к `chunks.rowid`. В `chunks` нельзя писать через `INSERT OR REPLACE` — REPLACE меняет rowid и отвязывает вектор/FTS ([kb_store.py:91-120](kb_store.py#L91-L120)).
- **ID чанка детерминирован**: `sha1(chat_id:msg_first:msg_last)`. Повторный инжест идемпотентен, ретрай бэкфилла не переэмбеддит уже записанное ([kb_ingest.py:148-149](kb_ingest.py#L148-L149)).
- **Чанк — это фрагмент беседы**, не сообщение: сообщения топика группируются до паузы >30 минут или ~4000 символов ([kb_ingest.py:85-124](kb_ingest.py#L85-L124)). Названия топиков форума берутся через `GetForumTopicsRequest`.
- **Инжест идёт от `last_seen_id`** (state в базе), а не «за сутки»: пропущенные запуски догоняются сами; state двигается только после успешной записи ([kb_ingest.py:125-166](kb_ingest.py#L125-L166)).
- **Ночной джоб** — `kb_ingest_loop()` в качалке ([download_telegram_files.py:154-180](download_telegram_files.py#L154-L180)), срабатывает в `INGEST_HOUR` локального времени (tzdata ставится в Dockerfile ради `TZ`). После инжеста — бэкап через `VACUUM INTO` (горячее копирование файла с WAL небезопасно).
- **Бэкфилл** (`kb_backfill.py`) запускать только при остановленной качалке (общая сессия!) и **до** включения ночного инжеста:
  `docker compose stop telegram-file-downloader` → `docker compose run --rm telegram-file-downloader python kb_backfill.py --dry-run` (оценка объёма/стоимости) → без `--dry-run` → `docker compose start telegram-file-downloader`.
- **kb-bot не падает без конфига**: без `KB_BOT_TOKEN`/`KB_ANSWER_CHAT_IDS` уходит в вечный sleep с ошибкой в логе (чтобы не крутить crash-loop под `restart: unless-stopped`). Для упоминаний боту нужен выключенный privacy mode (`/setprivacy` → Disable в BotFather).
- **Запросы FTS санитизируются** ([kb_store.py:136-148](kb_store.py#L136-L148)) — сырой пользовательский текст в `MATCH` роняет FTS5-синтаксис.
- **Для моделей класса gpt-5 не передавать `temperature`/`max_tokens`** ([kb_bot.py:82-91](kb_bot.py#L82-L91)).

## Устойчивость к сетевым сбоям

- **Внутренние реконнекты Telethon** — `connection_retries=-1`, `retry_delay=300`, `auto_reconnect=True` ([download_telegram_files.py:86-94](download_telegram_files.py#L86-L94)). Telethon бесконечно пытается восстановить разорванную сессию, выжидая 5 минут между попытками — короткий ретрай при кратковременном разрыве сети бесполезен и только засоряет журнал.
- **Внешний exponential backoff** — `run()` оборачивает `client.start()` + `run_until_disconnected()` в цикл с задержкой 300 → 600 → 1200 → 1800 секунд ([download_telegram_files.py:183-199](download_telegram_files.py#L183-L199)). Тот же паттерн — в `kb_bot.py`. Backoff сбрасывается до 300 после успешного коннекта.
- **Подавление логов Telethon до WARNING** ([download_telegram_files.py:44](download_telegram_files.py#L44)). Без этого журналы NAS забивались сообщениями вида `Got difference for channel X updates`, `Closing current connection`, `Attempt N at connecting failed`. Остаются только реальные предупреждения.
- **Авто-перезапуск контейнеров** — `restart: unless-stopped` у обоих сервисов поднимает контейнер, если процесс всё-таки упадёт (OOM, segfault, исчерпание backoff).
- **Ограничение логов Docker** — `json-file` драйвер с `max-size: 10m` × `max-file: 3` ([docker-compose.yml:21-25](docker-compose.yml#L21-L25), общий YAML-якорь для обоих сервисов). Без этого `/var/lib/docker/containers/<id>/<id>-json.log` рос бы бесконечно.
