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


def _f32(vec) -> bytes:
    return struct.pack(f'{len(vec)}f', *vec)


class SqliteVecStore:
    def __init__(self, db_path: str, embed_dim: int):
        import sqlite_vec
        self.db_path = db_path
        self.embed_dim = embed_dim
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.db = sqlite3.connect(db_path)
        self.db.enable_load_extension(True)
        sqlite_vec.load(self.db)
        self.db.enable_load_extension(False)
        # Базу одновременно пишет качалка (ночной ingest) и читает kb-bot
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA busy_timeout=5000')
        self._init_schema()

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
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )''')
            self.db.execute(
                f'CREATE VIRTUAL TABLE IF NOT EXISTS chunks_vec '
                f'USING vec0(embedding float[{self.embed_dim}])')
            self.db.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts "
                "USING fts5(text, tokenize='unicode61 remove_diacritics 2')")
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
                    llm_done INTEGER NOT NULL DEFAULT 0
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
                    gap_closed INTEGER NOT NULL DEFAULT 0
                )''')
        # Миграции старых баз (ALTER падает, если колонка есть)
        for stmt in (
            "ALTER TABLE files ADD COLUMN llm_done INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE qa_log ADD COLUMN msg_id INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE qa_log ADD COLUMN gap_posted INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE qa_log ADD COLUMN gap_closed INTEGER NOT NULL DEFAULT 0",
        ):
            try:
                with self.db:
                    self.db.execute(stmt)
            except sqlite3.OperationalError:
                pass

    def upsert_chunks(self, chunks: list[Chunk]) -> None:
        with self.db:
            for c in chunks:
                if c.embedding is None or len(c.embedding) != self.embed_dim:
                    raise ValueError(
                        f'chunk {c.id}: embedding отсутствует или неверной размерности')
                fields = (c.chat_id, c.topic_id, c.topic_name, c.date_from, c.date_to,
                          c.msg_first, c.msg_last, c.authors, c.text)
                row = self.db.execute(
                    'SELECT rowid FROM chunks WHERE id = ?', (c.id,)).fetchone()
                if row:
                    # НЕ INSERT OR REPLACE: REPLACE меняет rowid и отвязывает vec/fts
                    rowid = row[0]
                    self.db.execute('''
                        UPDATE chunks SET chat_id=?, topic_id=?, topic_name=?,
                            date_from=?, date_to=?, msg_first=?, msg_last=?,
                            authors=?, text=?
                        WHERE rowid=?''', fields + (rowid,))
                    self.db.execute('DELETE FROM chunks_vec WHERE rowid=?', (rowid,))
                    self.db.execute('DELETE FROM chunks_fts WHERE rowid=?', (rowid,))
                else:
                    cur = self.db.execute('''
                        INSERT INTO chunks(id, chat_id, topic_id, topic_name,
                            date_from, date_to, msg_first, msg_last, authors, text)
                        VALUES(?,?,?,?,?,?,?,?,?,?)''', (c.id,) + fields)
                    rowid = cur.lastrowid
                self.db.execute('INSERT INTO chunks_vec(rowid, embedding) VALUES(?,?)',
                                (rowid, _f32(c.embedding)))
                self.db.execute('INSERT INTO chunks_fts(rowid, text) VALUES(?,?)',
                                (rowid, c.text))

    def existing_ids(self, ids: list[str]) -> set[str]:
        out: set[str] = set()
        for i in range(0, len(ids), 500):  # лимит числа параметров sqlite
            part = ids[i:i + 500]
            marks = ','.join('?' * len(part))
            rows = self.db.execute(f'SELECT id FROM chunks WHERE id IN ({marks})', part)
            out.update(r[0] for r in rows)
        return out

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

    def search(self, query_text: str, query_vector: list, top_k: int = 8,
               candidates: int = 24) -> list[ScoredChunk]:
        vec_ids = [r[0] for r in self.db.execute(
            'SELECT rowid, distance FROM chunks_vec WHERE embedding MATCH ? AND k = ? '
            'ORDER BY distance', (_f32(query_vector), candidates))]
        # Пользовательский текст нельзя отдавать в MATCH сырым: синтаксис FTS5
        # падает на кавычках/минусах, поэтому только слова, каждое в кавычках.
        fts_ids: list[int] = []
        tokens = re.findall(r'\w+', query_text.lower())[:12]
        if tokens:
            fts_query = ' OR '.join(f'"{t}"' for t in tokens)
            try:
                fts_ids = [r[0] for r in self.db.execute(
                    'SELECT rowid FROM chunks_fts WHERE chunks_fts MATCH ? '
                    'ORDER BY rank LIMIT ?', (fts_query, candidates))]
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
                'SELECT chat_id, topic_name, date_from, msg_first, text '
                'FROM chunks WHERE rowid=?', (rowid,)).fetchone()
            if row:
                out.append(ScoredChunk(score, *row))
        return out

    def get_state(self, key: str, default: str | None = None) -> str | None:
        row = self.db.execute('SELECT value FROM state WHERE key=?', (key,)).fetchone()
        return row[0] if row else default

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
                    topic_name: str, date: str) -> None:
        with self.db:
            self.db.execute('''
                INSERT INTO files(doc_id, name, size, md5, chat_id, msg_id,
                                  caption, topic_name, date)
                VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(doc_id) DO UPDATE SET
                    md5 = CASE WHEN excluded.md5 != ''
                               THEN excluded.md5 ELSE files.md5 END,
                    caption = CASE WHEN excluded.caption != ''
                                   THEN excluded.caption ELSE files.caption END,
                    topic_name = CASE WHEN excluded.topic_name != ''
                                      THEN excluded.topic_name ELSE files.topic_name END
                ''', (doc_id, name, size, md5, chat_id, msg_id,
                      caption, topic_name, date))

    def set_file_md5(self, doc_id: int, md5: str) -> None:
        with self.db:
            self.db.execute('UPDATE files SET md5=? WHERE doc_id=?', (md5, doc_id))

    def upsert_firmware(self, doc_id: int, device_model: str, version: str,
                        version_key: str, source: str = 'filename',
                        confidence: str = 'high') -> None:
        model_norm = re.sub(r'[^A-Z0-9]', '', device_model.upper())
        with self.db:
            self.db.execute('''
                INSERT INTO firmware(doc_id, device_model, model_norm,
                                     version, version_key, source, confidence)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(doc_id, device_model, version) DO NOTHING
                ''', (doc_id, device_model, model_norm, version, version_key,
                      source, confidence))

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
                   f.md5, f.doc_id
            FROM firmware fw
            JOIN files f ON f.doc_id = fw.doc_id
            LEFT JOIN devices d ON d.model = fw.device_model
            WHERE fw.model_norm LIKE :like
               OR fw.device_model IN (
                    SELECT parent FROM devices
                    WHERE model_norm LIKE :like AND parent != '')
               OR fw.device_model IN (
                    SELECT model FROM devices WHERE parent IN (
                        SELECT model FROM devices
                        WHERE kind = 'series' AND model_norm LIKE :like))
            ORDER BY fw.device_model, fw.version_key DESC, f.date DESC
            LIMIT :lim''', {'like': f'%{norm}%', 'lim': limit}).fetchall()

    def file_by_doc_id(self, doc_id: int):
        """(name, md5, chat_id, msg_id) или None — для отправки файла ботом."""
        return self.db.execute(
            'SELECT name, md5, chat_id, msg_id FROM files WHERE doc_id=?',
            (doc_id,)).fetchone()

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
        """Файлы с подписью, у которых разбор имени не дал связок и LLM ещё
        не запускался: кандидаты для kb_extract."""
        return self.db.execute('''
            SELECT doc_id, name, caption FROM files
            WHERE llm_done = 0 AND caption != ''
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
               answer: str, found: bool, msg_id: int = 0) -> int:
        with self.db:
            cur = self.db.execute(
                'INSERT INTO qa_log(chat_id, user_id, question, answer, found, msg_id) '
                'VALUES(?,?,?,?,?,?)',
                (chat_id, user_id, question[:500], answer[:1000],
                 1 if found else 0, msg_id))
            return cur.lastrowid

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

    def unposted_gaps(self, limit: int = 3) -> list[tuple]:
        """Открытые пробелы, ещё не публиковавшиеся в чате."""
        return self.db.execute(
            'SELECT id, question FROM qa_log '
            'WHERE (found=0 OR rating<0) AND gap_closed=0 AND gap_posted=0 '
            'ORDER BY id DESC LIMIT ?', (limit,)).fetchall()

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
        return SqliteVecStore(os.getenv('KB_DB_PATH', './kb/kb.sqlite'),
                              int(os.getenv('EMBED_DIM', '512')))
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

        # лексический хит: вектор указывает мимо, точное слово решает
        hits = store.search('S5735', vec(2), top_k=3)
        assert any('S5735' in h.text for h in hits), hits

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
        assert store.count() == 2
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
        assert store.models_without_device() == ['MA5608T']
        store.upsert_firmware(555, 'MA5800', 'V100R022', '0100.0022.0000.0000',
                              source='caption', confidence='medium')
        fw_items, dev_items = store.review_items()
        assert len(fw_items) == 1 and dev_items == [('S5735-L', 'S5700')]
        assert store.pending_review_count() == 2
        store.confirm_firmware(fw_items[0][0], ok=True)
        store.confirm_device('S5735-L', ok=False)  # отклонение фиксируется
        assert store.pending_review_count() == 0
        # MA5800 добавилась подтверждённой связкой и тоже ждёт таксономию
        assert store.models_without_device(limit=50) == ['MA5608T', 'MA5800']

        # лог вопрос-ответ и оценки
        qa_id = store.log_qa(-1001234, 777, 'как прошить ONT?', 'вот так', True,
                             msg_id=100)
        assert store.set_qa_rating(qa_id, -1) == 'как прошить ONT?'
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
        assert s['chunks'] == 2 and s['downloads_24h'] == 1  # 2: один чанк убрал prune
        assert abs(s['events_cost'] - 0.12) < 1e-9
        assert s['files'] == 5 and s['fw_models'] == 4, (s['files'], s['fw_models'])
        assert s['qa_7d'] == 2 and s['qa_bad_7d'] == 1 and s['qa_nohit_7d'] == 1

        bak = os.path.join(tmp, 'kb.bak')
        store.backup(bak)
        assert os.path.exists(bak)
        store.close()  # обязательно до выхода из TemporaryDirectory (Windows)
    print('kb_store selftest: OK')


if __name__ == '__main__':
    _selftest()
