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
        return {
            'chunks': self.count(), 'pdf_chunks': pdf_chunks,
            'media_items': media[0], 'media_cost': media[1],
            'events_cost': events_cost, 'downloads_24h': downloads_24h,
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

        store.add_event('download', 'Скачан test.pdf (1.0 МБ)')
        store.add_event('ingest', 'Ночной инжест -1001234', 0.12)
        rows = store.unnotified_events()
        assert len(rows) == 2
        store.mark_events_notified([rows[0][0]])
        assert len(store.unnotified_events()) == 1
        s = store.kb_stats()
        assert s['chunks'] == 3 and s['downloads_24h'] == 1
        assert abs(s['events_cost'] - 0.12) < 1e-9

        bak = os.path.join(tmp, 'kb.bak')
        store.backup(bak)
        assert os.path.exists(bak)
        store.close()  # обязательно до выхода из TemporaryDirectory (Windows)
    print('kb_store selftest: OK')


if __name__ == '__main__':
    _selftest()
