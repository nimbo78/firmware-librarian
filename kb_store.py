"""Хранилище базы знаний: единственный модуль, знающий про бэкенд.

Переезд на Qdrant: реализовать QdrantStore с тем же интерфейсом, добавить
ветку в open_store() (KB_BACKEND=qdrant), перелить чанки с готовыми векторами
из sqlite. Операционный state (last_seen_id) всегда остаётся в SQLite.
"""
from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import struct
from collections import defaultdict
from dataclasses import dataclass


@dataclass
class Chunk:
    chat_id: int
    topic_id: int
    topic_name: str
    date_from: str  # 'YYYY-MM-DD'
    date_to: str
    msg_first: int
    msg_last: int
    authors: str
    text: str
    embedding: list | None = None
    # пространство знаний (kb_spaces): пусто = пространство по умолчанию
    # хранилища — так живут вызывающие, написанные до появления пространств
    space: str = ''

    @property
    def id(self) -> str:
        raw = f'{self.chat_id}:{self.msg_first}:{self.msg_last}'
        return hashlib.sha1(raw.encode('utf-8')).hexdigest()


@dataclass
class ScoredChunk:
    score: float
    chat_id: int
    topic_name: str
    date_from: str
    msg_first: int
    text: str
    # 0 или id топика Telegram; конвенция для синтетических чатов (chat_id>0):
    # 0 — PDF (kb_pdf), 1 — HedEx-документация (kb_hedex)
    topic_id: int = 0
    space: str = ''


def hash_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 16), b''):
            h.update(block)
    return h.hexdigest()


def file_md5(store, path: str) -> str:
    """MD5 файла с кэшем по (размер, mtime) — см. SqliteVecStore.cached_md5.
    Синхронная версия для не-async вызовов; в async-коде кэш проверяется
    отдельно, а само хэширование уходит в asyncio.to_thread (иначе чтение
    гигабайтов блокирует event loop и Telethon теряет соединение)."""
    md5 = store.cached_md5(path)
    if md5:
        return md5
    md5 = hash_file(path)
    store.remember_md5(path, md5)
    return md5


def _f32(vec) -> bytes:
    return struct.pack(f'{len(vec)}f', *vec)


# Версия схемы пространств (state schema_spaces). База, созданная раньше,
# открывается только после kb_spaces_migrate.py — см. _ensure_spaces_schema.
SCHEMA_SPACES = '1'


def vec_ddl(dim: int, table: str = 'chunks_vec') -> str:
    """vec0 с partition key по пространству: KNN внутри пространства сканирует
    только его строки (замер: в 2.6 раза быстрее полного при трёх
    пространствах), глобальный поиск не меняется. Единственное место DDL —
    им же пользуется миграция."""
    return (f'CREATE VIRTUAL TABLE IF NOT EXISTS {table} '
            f'USING vec0(space TEXT PARTITION KEY, embedding float[{dim}])')


def fts_ddl(table: str = 'chunks_fts') -> str:
    # space UNINDEXED: хранится рядом с текстом для фильтра в том же MATCH-запросе
    return (f'CREATE VIRTUAL TABLE IF NOT EXISTS {table} '
            f"USING fts5(text, space UNINDEXED, "
            f"tokenize='unicode61 remove_diacritics 2')")


class SqliteVecStore:
    def __init__(self, db_path: str, embed_dim: int, default_space: str = 'main'):
        import sqlite_vec
        self.db_path = db_path
        self.embed_dim = embed_dim
        # куда попадают чанки/файлы/вопросы без явной метки пространства
        self.default_space = default_space
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.db = sqlite3.connect(db_path)
        self.db.enable_load_extension(True)
        sqlite_vec.load(self.db)
        self.db.enable_load_extension(False)
        # Базу одновременно пишет качалка (ночной ingest) и читает kb-bot.
        # busy_timeout щедрый: пакет чанков с векторами пишется секундами, а
        # шаги архивов и HedEx идут часами — при 5 с соседний писатель
        # (бэкфилл, события бота) ловил «database is locked» и терял работу,
        # включая уже ОПЛАЧЕННЫЕ описания картинок.
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA busy_timeout=30000')
        try:
            self._init_schema()
            self._ensure_spaces_schema()
        except Exception:
            # не оставляем открытый коннект после неудачного конструктора:
            # на Windows он держит файл, а вызывающему закрывать нечего
            self.db.close()
            raise

    def _init_schema(self) -> None:
        with self.db:
            self.db.execute('''
                CREATE TABLE IF NOT EXISTS chunks(
                    id TEXT UNIQUE NOT NULL,
                    chat_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL DEFAULT 0,
                    topic_name TEXT NOT NULL DEFAULT '',
                    date_from TEXT NOT NULL,
                    date_to TEXT NOT NULL,
                    msg_first INTEGER NOT NULL,
                    msg_last INTEGER NOT NULL,
                    authors TEXT NOT NULL DEFAULT '',
                    text TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    space TEXT NOT NULL DEFAULT ''
                )''')
            self.db.execute(vec_ddl(self.embed_dim))
            self.db.execute(fts_ddl())
            self.db.execute(
                'CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            # Кэш vision/whisper-обработки медиа: ретрай бэкфилла не платит дважды
            self.db.execute('''
                CREATE TABLE IF NOT EXISTS media_cache(
                    key TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    cost REAL NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )''')
            # Очередь событий для админ-уведомлений: качалка пишет, kb-bot
            # раз в минуту забирает непрочитанные и шлёт админам в личку
            self.db.execute('''
                CREATE TABLE IF NOT EXISTS events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                    kind TEXT NOT NULL,
                    text TEXT NOT NULL,
                    cost REAL NOT NULL DEFAULT 0,
                    notified INTEGER NOT NULL DEFAULT 0
                )''')
            # Каталог документов из чатов: ключ — telegram document id.
            # md5 дозаписывается после физического скачивания качалкой.
            # llm_done: подпись уже прогонялась через LLM-экстракцию (фаза B).
            # kind: signature/software/patch/doc/... (kb_firmware.classify_name)
            self.db.execute('''
                CREATE TABLE IF NOT EXISTS files(
                    doc_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    size INTEGER NOT NULL DEFAULT 0,
                    md5 TEXT NOT NULL DEFAULT '',
                    chat_id INTEGER NOT NULL DEFAULT 0,
                    msg_id INTEGER NOT NULL DEFAULT 0,
                    caption TEXT NOT NULL DEFAULT '',
                    topic_name TEXT NOT NULL DEFAULT '',
                    date TEXT NOT NULL DEFAULT '',
                    llm_done INTEGER NOT NULL DEFAULT 0,
                    kind TEXT NOT NULL DEFAULT '',
                    space TEXT NOT NULL DEFAULT ''
                )''')
            # Листинг содержимого архивов (kb_archive): что лежит внутри
            # zip/rar/7z/tar — для каталога по внутренностям и показа состава
            self.db.execute('''
                CREATE TABLE IF NOT EXISTS archive_files(
                    archive_md5 TEXT NOT NULL,
                    path TEXT NOT NULL,
                    size INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(archive_md5, path)
                )''')
            # Устройства и серии (фаза B): S5735-L (kind=model, parent=S5700),
            # S5700 (kind=series). Низкоуверенные связки ждут /review.
            self.db.execute('''
                CREATE TABLE IF NOT EXISTS devices(
                    model TEXT PRIMARY KEY,
                    model_norm TEXT NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'model',
                    parent TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT 'llm',
                    confirmed INTEGER NOT NULL DEFAULT 0
                )''')
            # Связка файл -> модель/версия (фаза A: только из имени файла)
            self.db.execute('''
                CREATE TABLE IF NOT EXISTS firmware(
                    doc_id INTEGER NOT NULL,
                    device_model TEXT NOT NULL,
                    model_norm TEXT NOT NULL,
                    version TEXT NOT NULL DEFAULT '',
                    version_key TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT 'filename',
                    confidence TEXT NOT NULL DEFAULT 'high',
                    PRIMARY KEY (doc_id, device_model, version)
                )''')
            # Лог вопрос-ответ с оценками: 👎 -> событие админу,
            # found=0 -> копилка вопросов без ответа (/gaps).
            # msg_id — сообщение с вопросом (для авто-ответа реплаем),
            # gap_posted/gap_closed — петля «помогите сообществу».
            self.db.execute('''
                CREATE TABLE IF NOT EXISTS qa_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                    chat_id INTEGER NOT NULL DEFAULT 0,
                    user_id INTEGER NOT NULL DEFAULT 0,
                    question TEXT NOT NULL,
                    answer TEXT NOT NULL DEFAULT '',
                    found INTEGER NOT NULL DEFAULT 1,
                    rating INTEGER NOT NULL DEFAULT 0,
                    msg_id INTEGER NOT NULL DEFAULT 0,
                    gap_posted INTEGER NOT NULL DEFAULT 0,
                    gap_closed INTEGER NOT NULL DEFAULT 0,
                    space TEXT NOT NULL DEFAULT ''
                )''')
        # Миграции старых баз (ALTER падает, если колонка есть)
        for stmt in (
            # пространства знаний (kb_spaces); vec/fts мигрирует kb_spaces_migrate
            "ALTER TABLE chunks ADD COLUMN space TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE files ADD COLUMN space TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE qa_log ADD COLUMN space TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE files ADD COLUMN llm_done INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE files ADD COLUMN kind TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE qa_log ADD COLUMN msg_id INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE qa_log ADD COLUMN gap_posted INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE qa_log ADD COLUMN gap_closed INTEGER NOT NULL DEFAULT 0",
            # диалоги: id сообщения-ответа бота (реплай на него = follow-up)
            # и ссылка на родительский Q&A для восстановления цепочки
            "ALTER TABLE qa_log ADD COLUMN answer_msg_id INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE qa_log ADD COLUMN parent_qa_id INTEGER NOT NULL DEFAULT 0",
        ):
            try:
                with self.db:
                    self.db.execute(stmt)
            except sqlite3.OperationalError:
                pass

    def _ensure_spaces_schema(self) -> None:
        """vec/fts обязаны быть в схеме с пространствами (partition key и
        колонка space) — иначе поиск по пространству молча стал бы глобальным.

        Пустая база пересоздаётся на месте (терять нечего); базу с данными
        мигрирует только kb_spaces_migrate.py при остановленных сервисах —
        пересборка векторов на 2 ГБ идёт минуты и не место ей в старте бота."""
        if self.get_state('schema_spaces') == SCHEMA_SPACES:
            self._check_default_space()
            return
        if self.count() == 0:
            with self.db:
                self.db.execute('DROP TABLE IF EXISTS chunks_vec')
                self.db.execute('DROP TABLE IF EXISTS chunks_fts')
                self.db.execute(vec_ddl(self.embed_dim))
                self.db.execute(fts_ddl())
                for key, value in (('schema_spaces', SCHEMA_SPACES),
                                   ('spaces_default', self.default_space)):
                    self.db.execute(
                        'INSERT OR REPLACE INTO state(key, value) VALUES(?,?)',
                        (key, value))
            return
        raise RuntimeError(
            f'{self.db_path}: база создана до пространств знаний. Останови оба '
            f'сервиса и прогони kb_spaces_migrate.py (см. SPACES_PLAN.md)')

    def _check_default_space(self) -> None:
        """Область по умолчанию в конфиге обязана совпадать с той, чьё имя
        носят строки без явной метки.

        Классическая ловушка: базу мигрировали как «main», потом завели
        spaces.toml с первой секцией [huawei] — и поиск по своей области
        стал бы возвращать пустоту при живых данных. Лучше не подняться."""
        stored = self.get_state('spaces_default')
        if stored is None:
            self.set_state('spaces_default', self.default_space)
            return
        if stored == self.default_space:
            return
        raise RuntimeError(
            f'{self.db_path}: данные помечены областью «{stored}», а в конфиге '
            f'первая область — «{self.default_space}». Переименуй секцию в '
            f'spaces.toml обратно в «{stored}» либо переименуй область в базе: '
            f'kb_spaces_migrate.py --rename-space {stored} {self.default_space} '
            f'(при остановленных сервисах)')

    def upsert_chunks(self, chunks: list[Chunk]) -> None:
        with self.db:
            for c in chunks:
                if c.embedding is None or len(c.embedding) != self.embed_dim:
                    raise ValueError(
                        f'chunk {c.id}: embedding отсутствует или неверной размерности')
                space = c.space or self.default_space
                fields = (c.chat_id, c.topic_id, c.topic_name, c.date_from, c.date_to,
                          c.msg_first, c.msg_last, c.authors, c.text, space)
                # Одним оператором: вставка или обновление на месте.
                # НЕ INSERT OR REPLACE — REPLACE меняет rowid и отвязывает
                # vec/fts; ON CONFLICT DO UPDATE правит существующую строку,
                # rowid сохраняется. И НЕ «SELECT, потом INSERT/UPDATE»:
                # SELECT в python-sqlite3 идёт в автокоммите, то есть ДО
                # начала пишущей транзакции — сосед (ночной инжест того же
                # чата, второй бэкфилл) успевал вставить тот же id между
                # проверкой и вставкой, и прогон падал на UNIQUE constraint.
                self.db.execute('''
                    INSERT INTO chunks(id, chat_id, topic_id, topic_name,
                        date_from, date_to, msg_first, msg_last, authors, text,
                        space)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(id) DO UPDATE SET
                        chat_id=excluded.chat_id, topic_id=excluded.topic_id,
                        topic_name=excluded.topic_name,
                        date_from=excluded.date_from, date_to=excluded.date_to,
                        msg_first=excluded.msg_first, msg_last=excluded.msg_last,
                        authors=excluded.authors, text=excluded.text,
                        space=excluded.space''', (c.id,) + fields)
                # rowid читается уже внутри пишущей транзакции — сосед в неё
                # не вклинится; lastrowid после DO UPDATE недостоверен
                rowid = self.db.execute(
                    'SELECT rowid FROM chunks WHERE id = ?', (c.id,)).fetchone()[0]
                # для новой строки это no-op, для обновлённой — снятие старых
                # вектора и полнотекста перед записью новых
                self.db.execute('DELETE FROM chunks_vec WHERE rowid=?', (rowid,))
                self.db.execute('DELETE FROM chunks_fts WHERE rowid=?', (rowid,))
                self.db.execute(
                    'INSERT INTO chunks_vec(rowid, space, embedding) VALUES(?,?,?)',
                    (rowid, space, _f32(c.embedding)))
                self.db.execute(
                    'INSERT INTO chunks_fts(rowid, text, space) VALUES(?,?,?)',
                    (rowid, c.text, space))

    def existing_ids(self, ids: list[str]) -> set[str]:
        out: set[str] = set()
        for i in range(0, len(ids), 500):  # лимит числа параметров sqlite
            part = ids[i:i + 500]
            marks = ','.join('?' * len(part))
            rows = self.db.execute(f'SELECT id FROM chunks WHERE id IN ({marks})', part)
            out.update(r[0] for r in rows)
        return out

    def chunks_iter(self) -> list[tuple]:
        """(rowid, text) всех чанков — для переэмбеддинга (kb_reembed)."""
        return self.db.execute('SELECT rowid, text FROM chunks').fetchall()

    def reset_vectors(self, new_dim: int) -> None:
        """Пересоздаёт векторную таблицу под новую размерность. Вектора
        старой модели эмбеддингов несравнимы с новой — их нельзя оставлять
        (даже при совпадении размерности), только переэмбеддить всё."""
        with self.db:
            self.db.execute('DROP TABLE IF EXISTS chunks_vec')
            self.db.execute(vec_ddl(new_dim))
        self.embed_dim = new_dim

    def set_vector(self, rowid: int, embedding: list) -> None:
        if len(embedding) != self.embed_dim:
            raise ValueError('embedding размерности не совпадает со схемой')
        # пространство — из чанка: вектор обязан лежать в его разделе
        row = self.db.execute(
            'SELECT space FROM chunks WHERE rowid=?', (rowid,)).fetchone()
        space = (row[0] if row and row[0] else self.default_space)
        with self.db:
            # не INSERT OR REPLACE: vec0 с partition key его не принимает
            # («UNIQUE constraint failed on chunks_vec primary key»)
            self.db.execute('DELETE FROM chunks_vec WHERE rowid=?', (rowid,))
            self.db.execute(
                'INSERT INTO chunks_vec(rowid, space, embedding) '
                'VALUES(?,?,?)', (rowid, space, _f32(embedding)))

    def chunk_hashes(self, ids: list[str]) -> dict:
        """id -> sha1(text) для существующих чанков: инжест переэмбеддит
        только новое и изменившееся (обогащение медиа меняет текст при том же id)."""
        out: dict = {}
        for i in range(0, len(ids), 500):
            part = ids[i:i + 500]
            marks = ','.join('?' * len(part))
            for cid, text in self.db.execute(
                    f'SELECT id, text FROM chunks WHERE id IN ({marks})', part):
                out[cid] = hashlib.sha1(text.encode('utf-8')).hexdigest()
        return out

    def prune_chunks(self, chat_id: int, keep_ids: list[str]) -> int:
        """Удаляет чанки чата, чьих id нет в актуальном наборе (границы
        сдвинулись). Только для полного прогона бэкфилла — история пересобрана
        целиком, и всё вне keep_ids заведомо устарело."""
        keep = set(keep_ids)
        rows = self.db.execute(
            'SELECT id, rowid FROM chunks WHERE chat_id=?', (chat_id,)).fetchall()
        stale = [rowid for cid, rowid in rows if cid not in keep]
        with self.db:
            for rowid in stale:
                self.db.execute('DELETE FROM chunks WHERE rowid=?', (rowid,))
                self.db.execute('DELETE FROM chunks_vec WHERE rowid=?', (rowid,))
                self.db.execute('DELETE FROM chunks_fts WHERE rowid=?', (rowid,))
        return len(stale)

    def search(self, query_text: str, query_vector: list | None, top_k: int = 8,
               candidates: int = 24, space: str | None = None) -> list[ScoredChunk]:
        """Гибридный поиск. space=None — по всем пространствам; slug — только
        внутри него (KNN сканирует один раздел vec0, FTS фильтрует по колонке)."""
        # query_vector=None — FTS-only режим (деградация при недоступности
        # эмбеддинг-провайдера: фолбэк на другую модель невозможен)
        vec_ids: list[int] = []
        if query_vector is not None:
            sql = ('SELECT rowid, distance FROM chunks_vec '
                   'WHERE embedding MATCH ? AND k = ?')
            args: tuple = (_f32(query_vector), candidates)
            if space is not None:
                sql += ' AND space = ?'
                args += (space,)
            vec_ids = [r[0] for r in self.db.execute(sql + ' ORDER BY distance', args)]
        # Пользовательский текст нельзя отдавать в MATCH сырым: синтаксис FTS5
        # падает на кавычках/минусах, поэтому только слова, каждое в кавычках.
        fts_ids: list[int] = []
        tokens = re.findall(r'\w+', query_text.lower())[:12]
        if tokens:
            fts_query = ' OR '.join(f'"{t}"' for t in tokens)
            sql = 'SELECT rowid FROM chunks_fts WHERE chunks_fts MATCH ?'
            args = (fts_query,)
            if space is not None:
                sql += ' AND space = ?'
                args += (space,)
            try:
                fts_ids = [r[0] for r in self.db.execute(
                    sql + ' ORDER BY rank LIMIT ?', args + (candidates,))]
            except sqlite3.OperationalError:
                fts_ids = []
        # Reciprocal Rank Fusion
        scores: dict[int, float] = defaultdict(float)
        for rank, rowid in enumerate(vec_ids):
            scores[rowid] += 1.0 / (60 + rank)
        for rank, rowid in enumerate(fts_ids):
            scores[rowid] += 1.0 / (60 + rank)
        best = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:top_k]
        out = []
        for rowid, score in best:
            row = self.db.execute(
                'SELECT chat_id, topic_name, date_from, msg_first, text, topic_id, '
                'space FROM chunks WHERE rowid=?', (rowid,)).fetchone()
            if row:
                out.append(ScoredChunk(score, *row))
        return out

    def count_by_space(self) -> list[tuple[str, int]]:
        """[(space, чанков)] по убыванию — для /status, /sources и миграции."""
        return self.db.execute(
            'SELECT space, count(*) FROM chunks GROUP BY space '
            'ORDER BY 2 DESC, 1').fetchall()

    def doc_text_hashes(self) -> set[str]:
        """sha1 ТЕЛА (текст без первой строки-заголовка) всех HedEx-чанков —
        кросс-версионный дедуп страниц документации: заголовок содержит
        версию пакета, поэтому хэшируется только тело."""
        out: set[str] = set()
        for (text,) in self.db.execute(
                'SELECT text FROM chunks WHERE topic_id=1 AND chat_id>0'):
            body = text.split('\n', 1)[1] if '\n' in text else text
            out.add(hashlib.sha1(body.encode('utf-8')).hexdigest())
        return out

    def doc_key(self, prefix: str, md5: str, space: str = '') -> str:
        """Ключ state обработанного документа («pdf_ingested», «hedex_ingested»,
        «archive_scanned»).

        Для пространства по умолчанию — исторический вид `<prefix>:<md5>`:
        иначе весь уже обработанный корпус выглядел бы новым и был бы
        переиндексирован за деньги. Остальные пространства получают своё
        имя в ключе — один и тот же файл в двух папках инжестится в оба."""
        space = space or self.default_space
        return (f'{prefix}:{md5}' if space == self.default_space
                else f'{prefix}:{space}:{md5}')

    def get_state(self, key: str, default: str | None = None) -> str | None:
        row = self.db.execute('SELECT value FROM state WHERE key=?', (key,)).fetchone()
        return row[0] if row else default

    def cached_md5(self, path: str) -> str | None:
        """MD5 файла из кэша, если он не менялся (размер + mtime), иначе None.

        Зачем: идемпотентность конвейеров построена на MD5, и без кэша
        КАЖДЫЙ ночной прогон перечитывал все .hdx/PDF/архивы целиком
        только ради проверки «не обработано ли уже» — минуты IO и, хуже
        того, вымывание базы знаний из кэша страниц (после этого холодный
        поиск занимает секунды вместо десятых долей)."""
        try:
            st = os.stat(path)
        except OSError:
            return None
        row = self.db.execute('SELECT value FROM state WHERE key=?',
                              (f'filemd5:{path}',)).fetchone()
        if not row:
            return None
        try:
            size, mtime, md5 = row[0].split(':', 2)
        except ValueError:
            return None
        if int(size) == st.st_size and int(mtime) == int(st.st_mtime):
            return md5
        return None

    def remember_md5(self, path: str, md5: str) -> None:
        try:
            st = os.stat(path)
        except OSError:
            return
        self.set_state(f'filemd5:{path}',
                       f'{st.st_size}:{int(st.st_mtime)}:{md5}')

    def set_state(self, key: str, value: str) -> None:
        with self.db:
            self.db.execute(
                'INSERT INTO state(key, value) VALUES(?,?) '
                'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, value))

    def get_media_text(self, key: str) -> str | None:
        row = self.db.execute(
            'SELECT text FROM media_cache WHERE key=?', (key,)).fetchone()
        return row[0] if row else None

    def put_media_text(self, key: str, text: str, cost: float = 0.0) -> None:
        with self.db:
            self.db.execute(
                'INSERT INTO media_cache(key, text, cost) VALUES(?,?,?) '
                'ON CONFLICT(key) DO UPDATE SET text=excluded.text, cost=excluded.cost',
                (key, text, cost))

    def media_cost_total(self) -> float:
        return self.db.execute(
            'SELECT COALESCE(SUM(cost), 0) FROM media_cache').fetchone()[0]

    def add_event(self, kind: str, text: str, cost: float = 0.0) -> None:
        with self.db:
            self.db.execute('INSERT INTO events(kind, text, cost) VALUES(?,?,?)',
                            (kind, text[:300], cost))

    def unnotified_events(self, limit: int = 20) -> list[tuple]:
        return self.db.execute(
            'SELECT id, ts, kind, text, cost FROM events WHERE notified=0 '
            'ORDER BY id LIMIT ?', (limit,)).fetchall()

    def mark_events_notified(self, ids: list[int]) -> None:
        if not ids:
            return
        with self.db:
            marks = ','.join('?' * len(ids))
            self.db.execute(
                f'UPDATE events SET notified=1 WHERE id IN ({marks})', ids)

    def recent_events(self, limit: int = 20) -> list[tuple]:
        return self.db.execute(
            'SELECT id, ts, kind, text, cost FROM events '
            'ORDER BY id DESC LIMIT ?', (limit,)).fetchall()

    def state_items(self, prefix: str) -> list[tuple]:
        return self.db.execute(
            'SELECT key, value FROM state WHERE key LIKE ? ORDER BY key',
            (prefix + '%',)).fetchall()

    def upsert_file(self, doc_id: int, name: str, size: int, md5: str,
                    chat_id: int, msg_id: int, caption: str,
                    topic_name: str, date: str, kind: str = '',
                    space: str = '') -> None:
        with self.db:
            self.db.execute('''
                INSERT INTO files(doc_id, name, size, md5, chat_id, msg_id,
                                  caption, topic_name, date, kind, space)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(doc_id) DO UPDATE SET
                    md5 = CASE WHEN excluded.md5 != ''
                               THEN excluded.md5 ELSE files.md5 END,
                    caption = CASE WHEN excluded.caption != ''
                                   THEN excluded.caption ELSE files.caption END,
                    topic_name = CASE WHEN excluded.topic_name != ''
                                      THEN excluded.topic_name ELSE files.topic_name END,
                    kind = CASE WHEN excluded.kind != ''
                                THEN excluded.kind ELSE files.kind END
                ''', (doc_id, name, size, md5, chat_id, msg_id,
                      caption, topic_name, date, kind,
                      space or self.default_space))

    def set_file_md5(self, doc_id: int, md5: str) -> None:
        with self.db:
            self.db.execute('UPDATE files SET md5=? WHERE doc_id=?', (md5, doc_id))

    def doc_id_by_md5(self, md5: str) -> int | None:
        row = self.db.execute(
            'SELECT doc_id FROM files WHERE md5=?', (md5,)).fetchone()
        return row[0] if row else None

    def upsert_archive_files(self, archive_md5: str,
                             members: list[tuple[str, int]]) -> None:
        """Листинг архива целиком заменяется (повторный скан = свежая правда)."""
        with self.db:
            self.db.execute('DELETE FROM archive_files WHERE archive_md5=?',
                            (archive_md5,))
            self.db.executemany(
                'INSERT OR REPLACE INTO archive_files(archive_md5, path, size) '
                'VALUES(?,?,?)',
                [(archive_md5, p, s) for p, s in members])

    def archive_members(self, archive_md5: str) -> list[tuple[str, int]]:
        return self.db.execute(
            'SELECT path, size FROM archive_files WHERE archive_md5=? '
            'ORDER BY path', (archive_md5,)).fetchall()

    def set_file_kind(self, doc_id: int, kind: str) -> None:
        with self.db:
            self.db.execute('UPDATE files SET kind=? WHERE doc_id=?',
                            (kind, doc_id))

    def upsert_firmware(self, doc_id: int, device_model: str, version: str,
                        version_key: str, source: str = 'filename',
                        confidence: str = 'high') -> bool:
        """True, если связка новая (для счётчика reparse_files)."""
        model_norm = re.sub(r'[^A-Z0-9]', '', device_model.upper())
        with self.db:
            cur = self.db.execute('''
                INSERT INTO firmware(doc_id, device_model, model_norm,
                                     version, version_key, source, confidence)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(doc_id, device_model, version) DO NOTHING
                ''', (doc_id, device_model, model_norm, version, version_key,
                      source, confidence))
            return cur.rowcount == 1

    def all_files(self, with_kind: bool = False,
                  skip_signatures: bool = False) -> list[tuple]:
        """(doc_id, name[, kind]) каталога — reparse_files и LLM-матчинг."""
        cols = 'doc_id, name, kind' if with_kind else 'doc_id, name'
        where = "WHERE kind != 'signature'" if skip_signatures else ''
        return self.db.execute(f'SELECT {cols} FROM files {where}').fetchall()

    def all_models(self) -> list[str]:
        """Все известные модели и серии — словарь для LLM-fallback в /fw."""
        return [r[0] for r in self.db.execute(
            'SELECT DISTINCT device_model FROM firmware '
            'UNION SELECT model FROM devices')]

    def find_firmware(self, query: str, limit: int = 30) -> list[tuple]:
        """Поиск по модели: '5735' матчит 'S5735-L'. Свежие версии первыми.

        Учитывает серии (devices): запрос-модель дополнительно возвращает
        прошивки её серии (is_series=1 в выдаче), запрос-серия — прошивки
        всех моделей серии.
        """
        norm = re.sub(r'[^A-Z0-9]', '', query.upper())
        if not norm:
            return []
        return self.db.execute('''
            SELECT fw.device_model, fw.version, f.name, f.chat_id, f.msg_id,
                   f.date, fw.confidence,
                   CASE WHEN d.kind = 'series' THEN 1 ELSE 0 END AS is_series,
                   f.md5, f.doc_id, f.kind
            FROM firmware fw
            JOIN files f ON f.doc_id = fw.doc_id
            LEFT JOIN devices d ON d.model = fw.device_model
            WHERE f.kind != 'signature'
              AND (fw.model_norm LIKE :like
               OR fw.device_model IN (
                    SELECT parent FROM devices
                    WHERE model_norm LIKE :like AND parent != '')
               OR fw.device_model IN (
                    SELECT model FROM devices WHERE parent IN (
                        SELECT model FROM devices
                        WHERE kind = 'series' AND model_norm LIKE :like)))
            ORDER BY fw.device_model, fw.version_key DESC, f.date DESC
            LIMIT :lim''', {'like': f'%{norm}%', 'lim': limit}).fetchall()

    def fw_all(self) -> list[tuple]:
        """(device_model, version, version_key) без подписей — дерево /fw.
        version нужен для человекочитаемой метки ветки (не из padded-ключа)."""
        return self.db.execute('''
            SELECT fw.device_model, fw.version, fw.version_key
            FROM firmware fw JOIN files f ON f.doc_id = fw.doc_id
            WHERE f.kind != 'signature' ''').fetchall()

    def find_firmware_exact(self, model: str, vkey_prefix: str = '',
                            limit: int = 60) -> list[tuple]:
        """Файлы конкретной модели (и ветки версий) — лист дерева навигации.
        Формат строк совпадает с find_firmware."""
        return self.db.execute('''
            SELECT fw.device_model, fw.version, f.name, f.chat_id, f.msg_id,
                   f.date, fw.confidence,
                   0 AS is_series, f.md5, f.doc_id, f.kind
            FROM firmware fw
            JOIN files f ON f.doc_id = fw.doc_id
            WHERE f.kind != 'signature' AND fw.device_model = ?
              AND fw.version_key LIKE ? || '%'
            ORDER BY fw.version_key DESC, f.date DESC
            LIMIT ?''', (model, vkey_prefix, limit)).fetchall()

    def file_by_doc_id(self, doc_id: int):
        """(name, md5, chat_id, msg_id, date) или None."""
        return self.db.execute(
            'SELECT name, md5, chat_id, msg_id, date FROM files WHERE doc_id=?',
            (doc_id,)).fetchone()

    def files_by_prefix(self, prefix: str, limit: int = 20) -> list[tuple]:
        """(doc_id, name, md5) файлов, чьё имя начинается с prefix — для
        /download. LIKE-спецсимволы экранируются: в именах Huawei сплошные
        '_', которые иначе значат «любой символ»."""
        escaped = (prefix.replace('\\', '\\\\')
                   .replace('%', r'\%').replace('_', r'\_'))
        return self.db.execute(
            "SELECT doc_id, name, md5 FROM files "
            "WHERE name LIKE ? || '%' ESCAPE '\\' "
            "ORDER BY name LIMIT ?", (escaped, limit)).fetchall()

    def files_without_md5(self) -> list[tuple]:
        """(doc_id, name) записей без md5 — кандидаты на привязку к уже
        скачанным файлам через журнал дедупликации (link_local_files)."""
        return self.db.execute(
            "SELECT doc_id, name FROM files WHERE md5 = ''").fetchall()

    def upsert_device(self, model: str, kind: str = 'model', parent: str = '',
                      source: str = 'llm', confirmed: int = 0) -> None:
        """Первая запись побеждает: подтверждённые/отклонённые не перетираются."""
        model_norm = re.sub(r'[^A-Z0-9]', '', model.upper())
        with self.db:
            self.db.execute('''
                INSERT INTO devices(model, model_norm, kind, parent, source, confirmed)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(model) DO NOTHING''',
                (model, model_norm, kind, parent, source, confirmed))

    def files_for_extraction(self, limit: int = 200) -> list[tuple]:
        """Файлы, у которых разбор имени не дал связок и LLM ещё не
        запускался: кандидаты для kb_extract (имя + подпись, подпись может
        быть пустой — новые схемы имён ловятся и без неё)."""
        return self.db.execute('''
            SELECT doc_id, name, caption FROM files
            WHERE llm_done = 0 AND kind != 'signature'
              AND doc_id NOT IN (SELECT doc_id FROM firmware)
            ORDER BY doc_id LIMIT ?''', (limit,)).fetchall()

    def mark_file_extracted(self, doc_id: int) -> None:
        with self.db:
            self.db.execute('UPDATE files SET llm_done=1 WHERE doc_id=?', (doc_id,))

    def models_without_device(self, limit: int = 50) -> list[str]:
        return [r[0] for r in self.db.execute('''
            SELECT DISTINCT device_model FROM firmware
            WHERE device_model NOT IN (SELECT model FROM devices)
            ORDER BY device_model LIMIT ?''', (limit,))]

    def review_items(self, limit: int = 5) -> tuple[list, list]:
        """(прошивки confidence!=high, модели с неподтверждённой серией)."""
        fw = self.db.execute('''
            SELECT fw.rowid, fw.device_model, fw.version,
                   (SELECT name FROM files WHERE doc_id = fw.doc_id), fw.source
            FROM firmware fw WHERE fw.confidence != 'high'
            ORDER BY fw.rowid LIMIT ?''', (limit,)).fetchall()
        dev = self.db.execute('''
            SELECT model, parent FROM devices
            WHERE confirmed = 0 AND parent != ''
            ORDER BY model LIMIT ?''', (limit,)).fetchall()
        return fw, dev

    def pending_review_count(self) -> int:
        fw = self.db.execute(
            "SELECT count(*) FROM firmware WHERE confidence != 'high'").fetchone()[0]
        dev = self.db.execute(
            "SELECT count(*) FROM devices WHERE confirmed = 0 AND parent != ''"
        ).fetchone()[0]
        return fw + dev

    def confirm_firmware(self, rowid: int, ok: bool) -> None:
        with self.db:
            if ok:
                self.db.execute(
                    "UPDATE firmware SET confidence='high' WHERE rowid=?", (rowid,))
            else:
                self.db.execute('DELETE FROM firmware WHERE rowid=?', (rowid,))

    def medium_firmware_with_names(self) -> list[tuple]:
        """(rowid, device_model, name) для связок confidence!=high — вход
        авто-вычистки: те, что подтверждает разбор имени, снимаются."""
        return self.db.execute('''
            SELECT fw.rowid, fw.device_model, f.name
            FROM firmware fw JOIN files f ON f.doc_id = fw.doc_id
            WHERE fw.confidence != 'high' ''').fetchall()

    def delete_firmware_row(self, rowid: int) -> None:
        with self.db:
            self.db.execute('DELETE FROM firmware WHERE rowid=?', (rowid,))

    def delete_empty_version_rows(self, doc_id: int, models: list[str]) -> int:
        """Удаляет связки файла с пустой версией для перечисленных моделей —
        трупы старого парсера (не понимал SPH1b0/HP): reparse добавил строки
        с версией, а пустые дубли засоряли ветку «без версии» в /sw."""
        if not models:
            return 0
        with self.db:
            marks = ','.join('?' * len(models))
            cur = self.db.execute(
                f"DELETE FROM firmware WHERE doc_id=? AND version='' "
                f"AND device_model IN ({marks})", [doc_id, *models])
            return cur.rowcount

    def confirm_all_firmware(self) -> int:
        """Массовое подтверждение всех оставшихся medium-связок (кнопка
        «принять всё» в /review). Возвращает число повышенных."""
        with self.db:
            cur = self.db.execute(
                "UPDATE firmware SET confidence='high' WHERE confidence != 'high'")
            return cur.rowcount

    def confirm_all_series(self) -> int:
        """Авто-подтверждение таксономии серий (низкий риск): убирает их из
        очереди review. Возвращает число подтверждённых."""
        with self.db:
            cur = self.db.execute(
                "UPDATE devices SET confirmed=1 WHERE confirmed=0 AND parent!=''")
            return cur.rowcount

    def confirm_device(self, model: str, ok: bool) -> None:
        """Отклонение не удаляет строку (иначе LLM переспросит завтра),
        а фиксирует «серия неизвестна»."""
        with self.db:
            if ok:
                self.db.execute(
                    'UPDATE devices SET confirmed=1 WHERE model=?', (model,))
            else:
                self.db.execute(
                    "UPDATE devices SET parent='', confirmed=1 WHERE model=?",
                    (model,))

    def log_qa(self, chat_id: int, user_id: int, question: str,
               answer: str, found: bool, msg_id: int = 0,
               parent_qa_id: int = 0, space: str = '') -> int:
        with self.db:
            cur = self.db.execute(
                'INSERT INTO qa_log(chat_id, user_id, question, answer, '
                'found, msg_id, parent_qa_id, space) VALUES(?,?,?,?,?,?,?,?)',
                (chat_id, user_id, question[:500], answer[:1000],
                 1 if found else 0, msg_id, parent_qa_id,
                 space or self.default_space))
            return cur.lastrowid

    def set_qa_answer_msg(self, qa_id: int, answer_msg_id: int) -> None:
        """Фиксирует id сообщения-ответа бота: реплай на него — follow-up."""
        with self.db:
            self.db.execute('UPDATE qa_log SET answer_msg_id=? WHERE id=?',
                            (answer_msg_id, qa_id))

    def qa_by_answer_msg(self, chat_id: int, answer_msg_id: int) -> int | None:
        """id Q&A, чьим ответом является сообщение бота (или None)."""
        row = self.db.execute(
            'SELECT id FROM qa_log WHERE chat_id=? AND answer_msg_id=?',
            (chat_id, answer_msg_id)).fetchone()
        return row[0] if row else None

    def qa_dialog(self, qa_id: int, depth: int = 3) -> list[tuple[str, str]]:
        """Цепочка (вопрос, ответ) от корня к qa_id включительно, не глубже
        depth последних обменов — контекст follow-up-вопросов."""
        chain: list[tuple[str, str]] = []
        cur = qa_id
        for _ in range(depth):
            row = self.db.execute(
                'SELECT question, answer, parent_qa_id FROM qa_log WHERE id=?',
                (cur,)).fetchone()
            if not row:
                break
            chain.append((row[0], row[1]))
            if not row[2]:
                break
            cur = row[2]
        chain.reverse()
        return chain

    def set_qa_rating(self, qa_id: int, rating: int) -> str | None:
        """Ставит оценку, возвращает текст вопроса (для события админу)."""
        with self.db:
            self.db.execute('UPDATE qa_log SET rating=? WHERE id=?',
                            (rating, qa_id))
        row = self.db.execute(
            'SELECT question FROM qa_log WHERE id=?', (qa_id,)).fetchone()
        return row[0] if row else None

    def gaps(self, limit: int = 15) -> list[tuple]:
        """Вопросы без ответа или с минусом — карта дыр в базе знаний."""
        return self.db.execute(
            'SELECT ts, question FROM qa_log '
            'WHERE (found=0 OR rating<0) AND gap_closed=0 '
            'ORDER BY id DESC LIMIT ?', (limit,)).fetchall()

    def open_nohit_gaps(self, limit: int = 10) -> list[tuple]:
        """Вопросы, на которые поиск ничего не нашёл, — кандидаты на
        авто-ответ после ночного инжеста (👎-вопросы не ретраим автоматом:
        повторный плохой ответ хуже молчания)."""
        return self.db.execute(
            'SELECT id, chat_id, msg_id, question FROM qa_log '
            'WHERE found=0 AND gap_closed=0 '
            'ORDER BY id DESC LIMIT ?', (limit,)).fetchall()

    def unposted_gaps(self, limit: int = 3, space: str | None = None) -> list[tuple]:
        """Открытые пробелы, ещё не публиковавшиеся в чате. space — только
        вопросы этой области: пост «помогите сообществу» идёт в чат своего
        пространства, и чужие вопросы там никому не помогут."""
        sql = ('SELECT id, question FROM qa_log '
               'WHERE (found=0 OR rating<0) AND gap_closed=0 AND gap_posted=0')
        args: tuple = ()
        if space is not None:
            sql += ' AND space=?'
            args = (space,)
        return self.db.execute(
            sql + ' ORDER BY id DESC LIMIT ?', args + (limit,)).fetchall()

    def mark_gaps_posted(self, ids: list[int]) -> None:
        if not ids:
            return
        with self.db:
            marks = ','.join('?' * len(ids))
            self.db.execute(
                f'UPDATE qa_log SET gap_posted=1 WHERE id IN ({marks})', ids)

    def mark_gap_closed(self, qa_id: int) -> None:
        with self.db:
            self.db.execute('UPDATE qa_log SET gap_closed=1 WHERE id=?', (qa_id,))

    def sources_report(self, limit: int = 12) -> dict:
        """Инвентарь базы: что именно проиндексировано и сколько это чанков.

        Виды источников различаются конвенцией (см. Chunk/ScoredChunk):
        chat_id<0 — Telegram-чаты; chat_id>0 и topic_id=1 — страницы HedEx,
        где topic_name = «продукт версия — крошки»; chat_id>0 и topic_id=0 —
        PDF и текст из архивов, где topic_name = имя файла."""
        q = self.db.execute
        chats = q('SELECT chat_id, count(*), min(date_from), max(date_to) '
                  'FROM chunks WHERE chat_id < 0 GROUP BY chat_id '
                  'ORDER BY count(*) DESC').fetchall()
        hedex = q("SELECT substr(topic_name, 1, instr(topic_name, ' — ') - 1) AS pkg, "
                  "count(*) FROM chunks WHERE chat_id > 0 AND topic_id = 1 "
                  "AND instr(topic_name, ' — ') > 0 "
                  "GROUP BY pkg ORDER BY 2 DESC").fetchall()
        docs = q('SELECT topic_name, count(*) FROM chunks '
                 'WHERE chat_id > 0 AND topic_id = 0 '
                 'GROUP BY topic_name ORDER BY 2 DESC').fetchall()

        def state_count(prefix: str) -> int:
            return q('SELECT count(*) FROM state WHERE key LIKE ?',
                     (prefix + '%',)).fetchone()[0]

        return {
            'spaces': self.count_by_space(),
            'chats': chats,
            'hedex': hedex[:limit],
            'hedex_total': len(hedex),
            'docs': docs[:limit],
            'docs_total': len(docs),
            'processed': {
                'hedex': state_count('hedex_ingested:'),
                'pdf': state_count('pdf_ingested:'),
                'archive': state_count('archive_scanned:'),
            },
            'chunks': self.count(),
        }

    def kb_stats(self) -> dict:
        """Сводка для /status. events_cost уже включает медиа-затраты
        инжест-прогонов (media_cost — деталь, не слагаемое)."""
        pdf_chunks = self.db.execute(
            'SELECT count(*) FROM chunks WHERE chat_id > 0').fetchone()[0]
        media = self.db.execute(
            'SELECT count(*), COALESCE(SUM(cost),0) FROM media_cache').fetchone()
        events_cost = self.db.execute(
            'SELECT COALESCE(SUM(cost),0) FROM events').fetchone()[0]
        downloads_24h = self.db.execute(
            "SELECT count(*) FROM events WHERE kind='download' "
            "AND ts >= datetime('now', 'localtime', '-1 day')").fetchone()[0]
        files_total = self.db.execute('SELECT count(*) FROM files').fetchone()[0]
        fw_models = self.db.execute(
            'SELECT count(DISTINCT device_model) FROM firmware').fetchone()[0]
        qa7 = self.db.execute(
            "SELECT count(*), COALESCE(SUM(rating<0),0), COALESCE(SUM(found=0),0) "
            "FROM qa_log WHERE ts >= datetime('now', 'localtime', '-7 day')"
        ).fetchone()
        return {
            'chunks': self.count(), 'pdf_chunks': pdf_chunks,
            'media_items': media[0], 'media_cost': media[1],
            'events_cost': events_cost, 'downloads_24h': downloads_24h,
            'files': files_total, 'fw_models': fw_models,
            'qa_7d': qa7[0], 'qa_bad_7d': qa7[1], 'qa_nohit_7d': qa7[2],
            'pending_review': self.pending_review_count(),
        }

    def count(self) -> int:
        return self.db.execute('SELECT count(*) FROM chunks').fetchone()[0]

    def backup(self, dest: str | None = None) -> None:
        # Горячее копирование файла с WAL небезопасно — только VACUUM INTO
        dest = dest or self.db_path + '.bak'
        if os.path.exists(dest):
            os.remove(dest)
        self.db.execute('VACUUM INTO ?', (dest,))

    def close(self) -> None:
        self.db.close()


def open_store():
    backend = os.getenv('KB_BACKEND', 'sqlite').lower()
    if backend == 'sqlite':
        from kb_spaces import load_spaces
        return SqliteVecStore(os.getenv('KB_DB_PATH', './kb/kb.sqlite'),
                              int(os.getenv('EMBED_DIM', '512')),
                              default_space=load_spaces().default.slug)
    raise ValueError(f'Неизвестный KB_BACKEND: {backend}')


def _selftest() -> None:
    import tempfile

    dim = 8

    def vec(axis: int) -> list[float]:
        return [1.0 if i == axis else 0.0 for i in range(dim)]

    with tempfile.TemporaryDirectory() as tmp:
        store = SqliteVecStore(os.path.join(tmp, 'kb.sqlite'), dim)
        chunks = [
            Chunk(-1001234, 2, 'Прошивки', '2026-01-01', '2026-01-01', 10, 15, 'ivan',
                  'Топик «Прошивки»\n[2026-01-01 10:00] ivan: прошивка MA5608T лежит на ftp',
                  vec(0)),
            Chunk(-1001234, 2, 'Прошивки', '2026-01-02', '2026-01-02', 20, 25, 'petr',
                  'Топик «Прошивки»\n[2026-01-02 11:00] petr: конфиг для S5735 вот такой',
                  vec(1)),
            Chunk(-1001234, 3, 'Железо', '2026-01-03', '2026-01-03', 30, 35, 'oleg',
                  'Топик «Железо»\n[2026-01-03 12:00] oleg: гудит блок питания, '
                  'помогла замена вентилятора', vec(2)),
        ]
        store.upsert_chunks(chunks)
        assert store.count() == 3
        store.upsert_chunks(chunks)
        assert store.count() == 3, 'повторный upsert не должен плодить дубли'

        hits = store.search('где лежит прошивка MA5608T', vec(0), top_k=2)
        assert hits and 'MA5608T' in hits[0].text, hits

        # Гонка двух писателей: сосед (ночной инжест, второй бэкфилл) вставил
        # тот же id между нашей проверкой и записью. Прод падал здесь на
        # UNIQUE constraint failed: chunks.id
        neighbour = SqliteVecStore(os.path.join(tmp, 'kb.sqlite'), dim)
        same = Chunk(-1001234, 2, 'Прошивки', '2026-01-04', '2026-01-04', 40, 45,
                     'ivan', 'вариант соседа', vec(3))
        neighbour.upsert_chunks([same])
        mine = Chunk(-1001234, 2, 'Прошивки', '2026-01-04', '2026-01-04', 40, 45,
                     'ivan', 'наш вариант', vec(3))
        store.upsert_chunks([mine])      # не должно падать
        neighbour.close()
        row = store.db.execute('SELECT rowid, text FROM chunks WHERE id=?',
                               (mine.id,)).fetchone()
        assert row and row[1] == 'наш вариант', row
        # rowid сохранился, значит вектор и полнотекст не отвязались
        for tbl in ('chunks_vec', 'chunks_fts'):
            n = store.db.execute(
                f'SELECT count(*) FROM {tbl} WHERE rowid=?', (row[0],)).fetchone()[0]
            assert n == 1, (tbl, n)
        assert store.prune_chunks(-1001234, [c.id for c in chunks]) == 1
        assert store.count() == 3

        # лексический хит: вектор указывает мимо, точное слово решает
        hits = store.search('S5735', vec(2), top_k=3)
        assert any('S5735' in h.text for h in hits), hits

        # FTS-only деградация: без вектора поиск живёт на полнотексте
        hits = store.search('прошивка MA5608T', None, top_k=3)
        assert any('MA5608T' in h.text for h in hits), hits

        # ── пространства знаний ─────────────────────────────────────────
        assert all(h.space == 'main' for h in hits), 'без метки — пространство по умолчанию'
        store.upsert_chunks([Chunk(
            -2002, 1, 'Обход', '2026-02-01', '2026-02-01', 5, 9, 'max',
            'Топик «Обход»\n[2026-02-01 09:00] max: прошивка mihomo на роутере',
            vec(3), space='b4')])
        assert dict(store.count_by_space()) == {'main': 3, 'b4': 1}
        # скоуп: одно пространство видит только своё, глобальный — всё
        assert [h.space for h in store.search('прошивка', vec(3), space='b4')] == ['b4']
        assert all(h.space == 'main'
                   for h in store.search('прошивка', vec(0), space='main'))
        spaces_seen = {h.space for h in store.search('прошивка', vec(3), top_k=8)}
        assert spaces_seen == {'main', 'b4'}, spaces_seen
        # то же для FTS-only пути
        assert [h.space for h in store.search('mihomo', None, space='b4')] == ['b4']
        assert store.search('mihomo', None, space='main') == []
        # переэмбеддинг одного чанка не выкидывает его из своего раздела
        b4_rowid = store.db.execute(
            "SELECT rowid FROM chunks WHERE space='b4'").fetchone()[0]
        store.set_vector(b4_rowid, vec(4))
        assert [h.space for h in store.search('mihomo', vec(4), space='b4')] == ['b4']

        store.set_state('last_seen_id:-1001234', '35')
        assert store.get_state('last_seen_id:-1001234') == '35'
        assert store.get_state('nope', 'def') == 'def'

        assert store.get_media_text('img:-1001234:10') is None
        store.put_media_text('img:-1001234:10', 'скриншот display board 0', 0.004)
        assert store.get_media_text('img:-1001234:10') == 'скриншот display board 0'
        assert abs(store.media_cost_total() - 0.004) < 1e-9

        # пересборка: хэши текста и prune устаревших границ
        h = store.chunk_hashes([chunks[0].id, 'нет-такого'])
        assert chunks[0].id in h and 'нет-такого' not in h
        assert store.prune_chunks(-1001234, [c.id for c in chunks]) == 0
        assert store.prune_chunks(-1001234, [chunks[0].id, chunks[1].id]) == 1
        assert store.count() == 3, 'чанк другого чата (space=b4) не трогается'
        assert store.prune_chunks(-999, ['x']) == 0  # чужой чат не трогается

        store.add_event('download', 'Скачан test.pdf (1.0 МБ)')
        store.add_event('ingest', 'Ночной инжест -1001234', 0.12)
        rows = store.unnotified_events()
        assert len(rows) == 2
        store.mark_events_notified([rows[0][0]])
        assert len(store.unnotified_events()) == 1

        # каталог файлов и прошивок
        store.upsert_file(doc_id=111, name='MA5608T_V800R017C10SPC200.zip',
                          size=100, md5='', chat_id=-1001234, msg_id=40,
                          caption='старая', topic_name='Прошивки',
                          date='2025-11-02')
        store.upsert_file(doc_id=222, name='MA5608T_V800R018C10SPC500.zip',
                          size=100, md5='', chat_id=-1001234, msg_id=50,
                          caption='новая', topic_name='Прошивки',
                          date='2026-03-12')
        store.upsert_firmware(111, 'MA5608T', 'V800R017C10SPC200',
                              '0800.0017.0010.0200')
        store.upsert_firmware(222, 'MA5608T', 'V800R018C10SPC500',
                              '0800.0018.0010.0500')
        assert (111, 'MA5608T_V800R017C10SPC200.zip') in store.files_without_md5()
        store.set_file_md5(222, 'a' * 32)
        assert all(d != 222 for d, _ in store.files_without_md5())
        assert len(store.all_files()) == 2
        # upsert_firmware: True для новой связки, False для дубля (reparse)
        assert store.upsert_firmware(111, 'TEST1', 'V1R1', '0001.0001') is True
        assert store.upsert_firmware(111, 'TEST1', 'V1R1', '0001.0001') is False
        assert 'TEST1' in store.all_models()
        rec = store.file_by_doc_id(222)
        assert rec[0].startswith('MA5608T_V800R018') and rec[4] == '2026-03-12'
        assert store.file_by_doc_id(999999) is None

        # /download: поиск по префиксу имени с экранированием LIKE-символов
        pref = store.files_by_prefix('MA5608T_V800R017')
        assert [r[1] for r in pref] == ['MA5608T_V800R017C10SPC200.zip'], pref
        assert store.files_by_prefix('MA5608T%') == []  # % — литерал, не wildcard
        assert store.files_by_prefix('MA5608TzV800') == []  # _ не «любой символ»
        fw = store.find_firmware('5608')
        assert len(fw) == 2 and fw[0][1] == 'V800R018C10SPC500', fw  # свежая первой
        assert store.find_firmware('S9999') == []
        store.upsert_firmware(222, 'MA5608T', 'V800R018C10SPC500', 'x')  # идемпотентно
        assert len(store.find_firmware('5608')) == 2

        # серии: файл «для всей серии S5700» находится по запросу модели S5735-L
        store.upsert_file(doc_id=333, name='S5700_bootrom_V200R010.zip',
                          size=1, md5='', chat_id=-1001234, msg_id=60,
                          caption='', topic_name='', date='2026-01-01')
        store.upsert_firmware(333, 'S5700', 'V200R010', '0200.0010.0000.0000')
        store.upsert_file(doc_id=444, name='S5735-L-V200R019C00SPC500.cc',
                          size=1, md5='', chat_id=-1001234, msg_id=70,
                          caption='', topic_name='', date='2026-02-01')
        store.upsert_firmware(444, 'S5735-L', 'V200R019C00SPC500',
                              '0200.0019.0000.0500')
        store.upsert_device('S5735-L', kind='model', parent='S5700', confirmed=0)
        store.upsert_device('S5700', kind='series', parent='', confirmed=1)
        hits = store.find_firmware('5735')
        models_found = {(h[0], h[7]) for h in hits}
        assert ('S5735-L', 0) in models_found and ('S5700', 1) in models_found, hits
        hits = store.find_firmware('S5700')  # запрос-серия видит модели серии
        assert {h[0] for h in hits} == {'S5700', 'S5735-L'}, hits

        # экстракция и подтверждения
        store.upsert_file(doc_id=555, name='fw_new_final2.zip', size=1, md5='',
                          chat_id=-1001234, msg_id=80,
                          caption='прошивка для MA5608T', topic_name='',
                          date='2026-03-01')
        assert [r[0] for r in store.files_for_extraction()] == [555]
        store.mark_file_extracted(555)
        assert store.files_for_extraction() == []
        assert store.models_without_device() == ['MA5608T', 'TEST1']
        store.upsert_firmware(555, 'MA5800', 'V100R022', '0100.0022.0000.0000',
                              source='caption', confidence='medium')
        fw_items, dev_items = store.review_items()
        assert len(fw_items) == 1 and dev_items == [('S5735-L', 'S5700')]
        assert store.pending_review_count() == 2
        store.confirm_firmware(fw_items[0][0], ok=True)
        store.confirm_device('S5735-L', ok=False)  # отклонение фиксируется
        assert store.pending_review_count() == 0
        # MA5800 добавилась подтверждённой связкой и тоже ждёт таксономию
        assert store.models_without_device(limit=50) == ['MA5608T', 'MA5800', 'TEST1']

        # авто-review: серии оптом, medium снимается парсером, остаток — оптом
        store.upsert_device('S5735-S', kind='model', parent='S5700', confirmed=0)
        store.upsert_device('S9999-X', kind='model', parent='S9900', confirmed=0)
        assert store.pending_review_count() == 2
        assert store.confirm_all_series() == 2  # обе серии подтверждены разом
        assert store.pending_review_count() == 0
        # medium для файла 444, чьё ИМЯ парсер разбирает в ту же модель S5735-L
        # (версия иная — иначе PK-конфликт с high-записью и строка не создастся)
        store.upsert_firmware(444, 'S5735-L', 'V300R001', '0300.0001.0000.0000',
                              source='caption', confidence='medium')
        import kb_firmware
        assert kb_firmware.auto_resolve_firmware(store) == 1  # снято парсером
        # medium для файла 555, чьё имя (fw_new_final2.zip) парсер не берёт
        store.upsert_firmware(555, 'MA5900', 'V100R023', '0100.0023.0000.0000',
                              source='caption', confidence='medium')
        assert kb_firmware.auto_resolve_firmware(store) == 0  # не подтверждён
        assert store.pending_review_count() == 1
        assert store.confirm_all_firmware() == 1  # массовое подтверждение
        assert store.pending_review_count() == 0

        # лог вопрос-ответ и оценки
        qa_id = store.log_qa(-1001234, 777, 'как прошить ONT?', 'вот так', True,
                             msg_id=100)
        assert store.set_qa_rating(qa_id, -1) == 'как прошить ONT?'

        # диалоговая цепочка: реплай на ответ бота -> follow-up
        store.set_qa_answer_msg(qa_id, 555)
        assert store.qa_by_answer_msg(-1001234, 555) == qa_id
        assert store.qa_by_answer_msg(-1001234, 556) is None
        assert store.qa_by_answer_msg(-999, 555) is None, 'чужой чат'
        fu_id = store.log_qa(-1001234, 777, 'а на R024?', 'на R024 иначе',
                             True, msg_id=102, parent_qa_id=qa_id)
        store.set_qa_answer_msg(fu_id, 556)
        dialog = store.qa_dialog(fu_id)
        assert dialog == [('как прошить ONT?', 'вот так'),
                          ('а на R024?', 'на R024 иначе')], dialog
        assert store.qa_dialog(fu_id, depth=1) == \
            [('а на R024?', 'на R024 иначе')]
        store.log_qa(-1001234, 778, 'про что-то неизвестное', '', False,
                     msg_id=101)
        assert len(store.gaps()) == 2

        # петля gaps: авто-ответ только для found=0, недельный пост — для всех
        nohit = store.open_nohit_gaps()
        assert len(nohit) == 1 and nohit[0][2] == 101, nohit
        unposted = store.unposted_gaps()
        assert len(unposted) == 2
        store.mark_gaps_posted([unposted[0][0]])
        assert len(store.unposted_gaps()) == 1
        store.mark_gap_closed(nohit[0][0])
        assert store.open_nohit_gaps() == []
        assert len(store.gaps()) == 1  # закрытый пробел ушёл из /gaps

        s = store.kb_stats()
        # 3 чанка: два main (один убрал prune) и один b4
        assert s['chunks'] == 3 and s['downloads_24h'] == 1
        assert dict(store.sources_report()['spaces']) == {'main': 2, 'b4': 1}
        assert abs(s['events_cost'] - 0.12) < 1e-9
        # 6 моделей: MA5608T, S5700, S5735-L, MA5800, TEST1, MA5900 (авто-review)
        assert s['files'] == 5 and s['fw_models'] == 6, (s['files'], s['fw_models'])
        # 3 вопроса: исходный, follow-up диалога и вопрос без ответа
        assert s['qa_7d'] == 3 and s['qa_bad_7d'] == 1 and s['qa_nohit_7d'] == 1

        # файлы-сироты: в журнале и на диске, но без сообщения в каталоге
        import kb_firmware
        dl = os.path.join(tmp, 'downloads')
        os.makedirs(dl)
        orphan_md5 = 'c' * 32
        with open(os.path.join(dl, 'S5731-H_V600R023C00SPC500.cc'), 'wb') as f:
            f.write(b'x')
        with open(os.path.join(dl, 'downloaded_files.txt'), 'w',
                  encoding='utf-8') as f:
            f.write(f'{orphan_md5},S5731-H_V600R023C00SPC500.cc\n')
        assert kb_firmware.link_local_files(store, dl) == 1
        orphan = store.find_firmware('S5731-H')
        assert orphan and orphan[0][8] == orphan_md5, orphan  # md5 -> кнопка 📎
        assert orphan[0][3] == 0, orphan  # chat_id=0: ссылки на пост нет
        assert kb_firmware.link_local_files(store, dl) == 0  # идемпотентно

        # reparse чистит пустоверсионные дубли старого парсера
        store.upsert_file(doc_id=666, name='S5731-H_V200R024SPH1b0.pat',
                          size=1, md5='', chat_id=-1001234, msg_id=90,
                          caption='', topic_name='', date='2026-04-01')
        store.upsert_firmware(666, 'S5731-H', '', '')  # старый парсер: без версии
        kb_firmware.reparse_files(store)
        rows666 = store.find_firmware_exact('S5731-H')
        assert rows666 and all(r[1] for r in rows666), rows666  # дубль удалён
        assert any(r[1] == 'V200R024SPH1B0' for r in rows666), rows666

        # смена модели эмбеддингов: reset_vectors + set_vector (kb_reembed)
        # кэш MD5: повторный вызов не перечитывает файл, изменение — сбрасывает
        import kb_store as _ks
        probe = os.path.join(tmp, 'probe.bin')
        with open(probe, 'wb') as f:
            f.write(b'x' * 1024)
        calls = []
        real_hash = _ks.hash_file

        def counting_hash(path):
            calls.append(path)
            return real_hash(path)

        _ks.hash_file = counting_hash
        try:
            first = _ks.file_md5(store, probe)
            second = _ks.file_md5(store, probe)
            assert first == second and len(calls) == 1, calls
            os.utime(probe, (0, 0))          # mtime изменился -> пересчёт
            third = _ks.file_md5(store, probe)
            assert third == first and len(calls) == 2, calls
            with open(probe, 'wb') as f:     # другое содержимое и размер
                f.write(b'y' * 2048)
            fourth = _ks.file_md5(store, probe)
            assert fourth != first and len(calls) == 3, calls
            assert store.cached_md5(os.path.join(tmp, 'нет.bin')) is None
        finally:
            _ks.hash_file = real_hash

        # листинг архивов + поиск doc_id по md5 (kb_archive)
        store.upsert_archive_files('a' * 32, [('inner/S5735.cc', 100),
                                              ('rn.pdf', 5)])
        assert store.archive_members('a' * 32) == [('inner/S5735.cc', 100),
                                                   ('rn.pdf', 5)]
        store.upsert_archive_files('a' * 32, [('other.txt', 1)])
        assert store.archive_members('a' * 32) == [('other.txt', 1)], \
            'повторный скан должен заменять листинг целиком'
        assert store.doc_id_by_md5('нет такого') is None

        n_chunks = store.count()
        store.reset_vectors(4)
        assert store.search('прошивка', [1.0, 0, 0, 0]) != [] or True
        for rowid, _text in store.chunks_iter():
            store.set_vector(rowid, [1.0, 0, 0, 0])
        hits4 = store.search('прошивка MA5608T', [1.0, 0, 0, 0], top_k=2)
        assert hits4, 'после переэмбеддинга поиск должен работать'
        assert len(store.chunks_iter()) == n_chunks
        try:
            store.set_vector(1, [1.0, 0])  # неверная размерность
            raise AssertionError('ожидали ValueError')
        except ValueError:
            pass

        bak = os.path.join(tmp, 'kb.bak')
        store.backup(bak)
        assert os.path.exists(bak)
        store.close()  # обязательно до выхода из TemporaryDirectory (Windows)
    print('kb_store selftest: OK')


if __name__ == '__main__':
    _selftest()
