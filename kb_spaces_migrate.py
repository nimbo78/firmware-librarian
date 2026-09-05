"""Разовая миграция базы под пространства знаний (SPACES_PLAN.md, фаза 1).

Что делает: проставляет всем существующим чанкам/файлам/вопросам пространство
по умолчанию и пересобирает chunks_vec (partition key по пространству) и
chunks_fts (колонка space) — старые таблицы такой схемы не имеют, а поиск по
пространству на них молча стал бы глобальным.

Векторы НЕ переэмбеддятся: они читаются из старой таблицы и перекладываются
в новую как есть. Telegram и OpenAI не нужны, денег не стоит.

    docker compose stop librarian kb-bot
    docker compose run --rm librarian python kb_spaces_migrate.py
    docker compose start librarian kb-bot

Идемпотентно: повторный запуск на мигрированной базе ничего не делает.
Перед пересборкой снимается бэкап (VACUUM INTO), как и в ночном конвейере.

Селфтест (создаёт базу старой схемы во временной папке и мигрирует её):
    python kb_spaces_migrate.py --selftest
"""
from __future__ import annotations

import os
import sqlite3
import sys
import time

from kb_spaces import load_spaces
from kb_store import SCHEMA_SPACES, fts_ddl, vec_ddl

BATCH = 2000


def _connect(db_path: str) -> sqlite3.Connection:
    import sqlite_vec
    db = sqlite3.connect(db_path)
    db.enable_load_extension(True)
    sqlite_vec.load(db)
    db.enable_load_extension(False)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA busy_timeout=5000')
    return db


def _has_column(db: sqlite3.Connection, table: str, column: str) -> bool:
    return any(r[1] == column
               for r in db.execute(f'PRAGMA table_info({table})'))


def _vec_dim(db: sqlite3.Connection) -> int:
    """Размерность из длины хранимого вектора: не полагаемся на EMBED_DIM —
    env мог уехать вперёд базы (тогда это работа kb_reembed, не наша)."""
    row = db.execute('SELECT embedding FROM chunks_vec LIMIT 1').fetchone()
    if row is None:
        return int(os.getenv('EMBED_DIM', '512'))
    return len(row[0]) // 4


def _rebuild_indexes(db: sqlite3.Connection, dim: int, n: int, log) -> None:
    """Пересобирает chunks_vec (partition key) и chunks_fts из chunks.

    Векторы читаются из существующей таблицы и перекладываются как есть —
    переэмбеддинга нет. Используется и первичной миграцией, и
    переименованием области (partition key в vec0 не обновляется UPDATE'ом)."""
    t0 = time.monotonic()
    with db:
        db.execute('DROP TABLE IF EXISTS chunks_vec_new')
        db.execute(vec_ddl(dim, 'chunks_vec_new'))
    moved, last = 0, 0
    while True:
        rows = db.execute(
            'SELECT v.rowid, c.space, v.embedding FROM chunks_vec v '
            'JOIN chunks c ON c.rowid = v.rowid '
            'WHERE v.rowid > ? ORDER BY v.rowid LIMIT ?',
            (last, BATCH)).fetchall()
        if not rows:
            break
        with db:
            db.executemany(
                'INSERT INTO chunks_vec_new(rowid, space, embedding) '
                'VALUES(?,?,?)', rows)
        last, moved = rows[-1][0], moved + len(rows)
        log(f'  вектора: {moved}/{n}')
    with db:
        db.execute('DROP TABLE chunks_vec')
    _rename_vec(db, 'chunks_vec_new', 'chunks_vec', dim)
    log(f'  вектора перенесены за {time.monotonic() - t0:.0f} с')

    t0 = time.monotonic()
    with db:
        db.execute('DROP TABLE IF EXISTS chunks_fts')
        db.execute(fts_ddl())
    indexed, last = 0, 0
    while True:
        rows = db.execute(
            'SELECT rowid, text, space FROM chunks WHERE rowid > ? '
            'ORDER BY rowid LIMIT ?', (last, BATCH)).fetchall()
        if not rows:
            break
        with db:
            db.executemany(
                'INSERT INTO chunks_fts(rowid, text, space) VALUES(?,?,?)', rows)
        last, indexed = rows[-1][0], indexed + len(rows)
        log(f'  полнотекст: {indexed}/{n}')
    log(f'  FTS пересобран за {time.monotonic() - t0:.0f} с')


def rename_space(db_path: str, old: str, new: str, log=print) -> int:
    """Переименовать область во ВСЕЙ базе: chunks/files/qa_log, ключи state
    обработанных документов и векторные разделы.

    Нужно, когда spaces.toml заводят после миграции и первую секцию
    называют не так, как область по умолчанию в базе (её имя — в state
    spaces_default). Без этого поиск по «новой» области нашёл бы пустоту.
    Запускать при остановленных сервисах."""
    db = _connect(db_path)
    try:
        moved = db.execute('SELECT count(*) FROM chunks WHERE space=?',
                           (old,)).fetchone()[0]
        if not moved and db.execute(
                'SELECT count(*) FROM files WHERE space=?', (old,)).fetchone()[0] == 0:
            log(f'Строк области «{old}» в базе нет — нечего переименовывать.')
            return 0
        default = db.execute(
            "SELECT value FROM state WHERE key='spaces_default'").fetchone()
        default = default[0] if default else old
        log(f'Переименование «{old}» -> «{new}»: чанков {moved}')
        with db:
            for table in ('chunks', 'files', 'qa_log'):
                cur = db.execute(f'UPDATE {table} SET space=? WHERE space=?',
                                 (new, old))
                log(f'  {table}: {cur.rowcount} строк')
        # ключи state документов: у области по умолчанию — исторический вид
        # без имени, у остальных — с именем. Смена «кто по умолчанию» их
        # переименовывает (см. SqliteVecStore.doc_key)
        new_default = new if default == old else default
        renamed = 0
        for prefix in ('pdf_ingested', 'hedex_ingested', 'archive_scanned'):
            for key, value in db.execute(
                    'SELECT key, value FROM state WHERE key LIKE ?',
                    (prefix + ':%',)).fetchall():
                rest = key[len(prefix) + 1:]
                head, sep, tail = rest.partition(':')
                key_space, md5 = (head, tail) if sep else (default, rest)
                if key_space != old:
                    continue
                fresh = (f'{prefix}:{md5}' if new == new_default
                         else f'{prefix}:{new}:{md5}')
                if fresh == key:
                    continue
                with db:
                    db.execute('DELETE FROM state WHERE key=?', (key,))
                    db.execute('INSERT OR REPLACE INTO state(key, value) '
                               'VALUES(?,?)', (fresh, value))
                renamed += 1
        if renamed:
            log(f'  ключей обработанных документов: {renamed}')
        _rebuild_indexes(db, _vec_dim(db), moved, log)
        with db:
            db.execute('INSERT OR REPLACE INTO state(key, value) VALUES(?,?)',
                       ('spaces_default', new_default))
        log('Готово. Запускай сервисы обратно.')
        return moved
    finally:
        db.close()


def migrate(db_path: str, space: str, log=print) -> bool:
    """True — база мигрирована сейчас, False — уже была в новой схеме."""
    db = _connect(db_path)
    try:
        done = db.execute(
            "SELECT value FROM state WHERE key='schema_spaces'").fetchone()
        if done and done[0] == SCHEMA_SPACES:
            log('База уже в схеме с пространствами — делать нечего.')
            return False

        n = db.execute('SELECT count(*) FROM chunks').fetchone()[0]
        dim = _vec_dim(db)
        log(f'Чанков: {n}, размерность векторов: {dim}, '
            f'пространство по умолчанию: «{space}»')

        if n:
            bak = db_path + '.pre-spaces'
            if os.path.exists(bak):
                os.remove(bak)
            log(f'Бэкап -> {bak} (столько же места, сколько сама база)…')
            db.execute('VACUUM INTO ?', (bak,))

        # 1. Колонки space (в свежей базе их создаёт _init_schema, в старой — нет)
        for table in ('chunks', 'files', 'qa_log'):
            if not _has_column(db, table, 'space'):
                with db:
                    db.execute(f"ALTER TABLE {table} ADD COLUMN space TEXT "
                               f"NOT NULL DEFAULT ''")
        with db:
            for table in ('chunks', 'files', 'qa_log'):
                cur = db.execute(f"UPDATE {table} SET space=? WHERE space=''",
                                 (space,))
                log(f'  {table}: помечено {cur.rowcount} строк')

        # 2. Векторная таблица заново (partition key) + 3. FTS с колонкой space
        _rebuild_indexes(db, dim, n, log)

        with db:
            db.execute('INSERT OR REPLACE INTO state(key, value) VALUES(?,?)',
                       ('schema_spaces', SCHEMA_SPACES))
            # чьё имя носят строки без явной метки: startup-guard хранилища
            # сверяет его с конфигом, чтобы поиск не ушёл в пустую область
            db.execute('INSERT OR REPLACE INTO state(key, value) VALUES(?,?)',
                       ('spaces_default', space))
        log('Готово. Запускай сервисы обратно.')
        return True
    finally:
        db.close()


def _rename_vec(db: sqlite3.Connection, src: str, dst: str, dim: int) -> None:
    """vec0 не переживает ALTER TABLE ... RENAME (теневые таблицы остаются со
    старым именем — «no such table: main.<dst>_rowids»), поэтому создаём
    целевую таблицу и переливаем в неё содержимое временной."""
    with db:
        db.execute(vec_ddl(dim, dst))
        db.execute(f'INSERT INTO {dst}(rowid, space, embedding) '
                   f'SELECT rowid, space, embedding FROM {src}')
        db.execute(f'DROP TABLE {src}')


def main() -> None:
    db_path = os.getenv('KB_DB_PATH', './kb/kb.sqlite')
    if not os.path.exists(db_path):
        raise SystemExit(f'Базы нет: {db_path}')
    if '--rename-space' in sys.argv:
        i = sys.argv.index('--rename-space')
        try:
            old, new = sys.argv[i + 1], sys.argv[i + 2]
        except IndexError:
            raise SystemExit('Использование: kb_spaces_migrate.py '
                             '--rename-space <старое> <новое>')
        rename_space(db_path, old, new)
        return
    space = load_spaces().default.slug
    migrate(db_path, space)


def _selftest() -> None:
    """Строит базу СТАРОЙ схемы (vec0 без partition key, fts без space),
    мигрирует и проверяет, что поиск по пространству работает, а векторы
    остались прежними."""
    import struct
    import tempfile

    from kb_store import Chunk, SqliteVecStore

    dim = 8

    def vec(axis: int) -> list[float]:
        return [1.0 if i == axis else 0.0 for i in range(dim)]

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'kb.sqlite')
        db = _connect(path)
        db.executescript('''
            CREATE TABLE chunks(
                id TEXT UNIQUE NOT NULL, chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL DEFAULT 0,
                topic_name TEXT NOT NULL DEFAULT '',
                date_from TEXT NOT NULL, date_to TEXT NOT NULL,
                msg_first INTEGER NOT NULL, msg_last INTEGER NOT NULL,
                authors TEXT NOT NULL DEFAULT '', text TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')));
            CREATE TABLE state(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE files(doc_id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE qa_log(id INTEGER PRIMARY KEY AUTOINCREMENT,
                question TEXT NOT NULL);
        ''')
        db.execute(f'CREATE VIRTUAL TABLE chunks_vec USING vec0(embedding float[{dim}])')
        db.execute("CREATE VIRTUAL TABLE chunks_fts USING fts5(text, "
                   "tokenize='unicode61 remove_diacritics 2')")
        texts = [f'чанк про прошивку номер {i}' for i in range(5)]
        with db:
            for i, text in enumerate(texts, start=1):
                db.execute(
                    'INSERT INTO chunks(id, chat_id, date_from, date_to, '
                    'msg_first, msg_last, text) VALUES(?,?,?,?,?,?,?)',
                    (f'id{i}', -1001, '2026-01-01', '2026-01-01', i, i, text))
                db.execute('INSERT INTO chunks_vec(rowid, embedding) VALUES(?,?)',
                           (i, struct.pack(f'{dim}f', *vec(i % dim))))
                db.execute('INSERT INTO chunks_fts(rowid, text) VALUES(?,?)',
                           (i, text))
            db.execute("INSERT INTO files(doc_id, name) VALUES(1, 'a.bin')")
            db.execute("INSERT INTO qa_log(question) VALUES('вопрос')")
        before = db.execute(
            'SELECT rowid, embedding FROM chunks_vec ORDER BY rowid').fetchall()
        db.close()

        quiet = []
        assert migrate(path, 'huawei', log=quiet.append) is True
        assert migrate(path, 'huawei', log=quiet.append) is False, 'идемпотентность'

        # старое хранилище открывается без миграции и знает про пространства
        store = SqliteVecStore(path, dim, default_space='huawei')
        try:
            assert store.count() == 5
            assert dict(store.count_by_space()) == {'huawei': 5}
            after = store.db.execute(
                'SELECT rowid, embedding FROM chunks_vec ORDER BY rowid').fetchall()
            assert after == before, 'векторы обязаны остаться прежними'
            hits = store.search('прошивку', vec(1), space='huawei')
            assert hits and all(h.space == 'huawei' for h in hits), hits
            assert store.search('прошивку', vec(1), space='b4') == []
            assert store.search('прошивку', None, space='huawei'), 'FTS по space'
            assert store.db.execute(
                "SELECT space FROM files").fetchone()[0] == 'huawei'
            assert store.db.execute(
                "SELECT space FROM qa_log").fetchone()[0] == 'huawei'
            # новые чанки ложатся рядом со старыми
            store.upsert_chunks([Chunk(-1001, 0, '', '2026-02-02', '2026-02-02',
                                       9, 9, '', 'новый чанк', vec(2))])
            assert dict(store.count_by_space()) == {'huawei': 6}
            store.set_state('pdf_ingested:' + 'a' * 32, 'manual.pdf')
        finally:
            store.close()

        # конфиг переименовали, а база помечена по-старому — не поднимаемся
        try:
            SqliteVecStore(path, dim, default_space='b4')
            raise AssertionError('ожидали отказ при расхождении областей')
        except RuntimeError as e:
            assert 'rename-space' in str(e), e

        # ...и чиним это переименованием: строки, ключи state и разделы vec0
        assert rename_space(path, 'huawei', 'b4', log=quiet.append) == 6
        store = SqliteVecStore(path, dim, default_space='b4')
        try:
            assert dict(store.count_by_space()) == {'b4': 6}
            hits = store.search('прошивку', vec(1), space='b4')
            assert hits and all(h.space == 'b4' for h in hits), hits
            assert store.search('прошивку', vec(1), space='huawei') == []
            # область по умолчанию сменилась -> ключ документа снова без имени
            assert store.get_state('pdf_ingested:' + 'a' * 32) == 'manual.pdf'
            assert store.get_state('spaces_default') == 'b4'
        finally:
            store.close()
    print('kb_spaces_migrate selftest: OK')


if __name__ == '__main__':
    if '--selftest' in sys.argv:
        _selftest()
    else:
        main()
