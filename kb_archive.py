"""Просмотр архивов из папки загрузок (флаг KB_ARCHIVE=1): листинг, каталог
по внутренностям, инжест текстового содержимого в RAG.

Три контура обработки одного архива (zip/rar/7z/tar/tar.gz/gz):

1. **Листинг** — состав пишется в `archive_files` (замена целиком).
   Для rar листинг — чистый Python (rarfile парсит заголовки сам).
2. **Каталог по внутренностям**: имена членов прогоняются через
   `parse_firmware_name` — архив «Сборка_для_апгрейда.rar» с
   `S5735…SPH121.pat` внутри получает связки в firmware (source='archive')
   и находится через /fw, хотя его собственное имя не парсится.
3. **Текст в RAG**: pdf/docx/xlsx/txt/html извлекаются ПОТОКОВО В ПАМЯТЬ
   (по одному члену, на диск не пишутся) → чанки с синтетическим
   положительным chat_id из MD5 архива (конвенция PDF: бот показывает
   «файл "архив → член", стр. N»). Бинарники (.cc/.pat/...) не читаются.

Исключение — контейнеры-источники: `.hdx` внутри архива распаковывается
(потоково, RAM ~КБ) в подпапку hedex_extracted/ и дальше обрабатывается
штатным kb_hedex (рекурсивный скан её видит; шаг архивов в пайплайнах
стоит ПЕРЕД шагом HedEx — извлечённый пакет инжестится той же ночью).

Идемпотентность: state `archive_scanned:<md5>`; md5 берётся из журнала
качалки (downloaded_files.txt), для положенных руками — считается.
Лимиты против zip-бомб: член 50 МБ (документы) / 5 МБ (плоский текст) /
2 ГБ (.hdx), суммарно текста с архива — 500 МБ. Пароленные и битые члены
пропускаются с warning. Вложенные архивы: листинг/каталог — да (глубина 1),
текст из них не извлекается.

Selftest (без Telegram и OpenAI): python kb_archive.py
"""
from __future__ import annotations

import hashlib
import html as html_mod
import io
import logging
import os
import re
import zipfile

from kb_firmware import _load_md5_journal, classify_name, parse_firmware_name
from kb_hedex import _split_body, page_text
from kb_store import Chunk

logger = logging.getLogger(__name__)

ARCHIVE_EXTS = ('.zip', '.rar', '.7z', '.tar', '.gz', '.tgz')
TEXT_DOC_EXTS = ('.pdf', '.docx', '.xlsx')
TEXT_PLAIN_EXTS = ('.txt', '.log', '.htm', '.html')

DOC_MEMBER_LIMIT = 50 * 1024 * 1024      # pdf/docx/xlsx
PLAIN_MEMBER_LIMIT = 5 * 1024 * 1024     # txt/html
CONTAINER_LIMIT = 2 * 1024 * 1024 * 1024  # .hdx
ARCHIVE_TEXT_TOTAL = 500 * 1024 * 1024   # суммарно текста с одного архива
XLSX_MAX_ROWS = 5000                     # на лист — защита от простыней
HEDEX_SUBDIR = 'hedex_extracted'

# конвенция чанков-файлов (как kb_pdf): chat_id>0, topic_id=0
CHUNK_CHARS = 4000


def archive_enabled() -> bool:
    return os.getenv('KB_ARCHIVE', '0') == '1'


def is_archive(name: str) -> bool:
    return name.lower().endswith(ARCHIVE_EXTS)


def _file_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 16), b''):
            h.update(block)
    return h.hexdigest()


def _fix_zip_name(info: zipfile.ZipInfo) -> str:
    """Zip без UTF-8-флага декодируется как cp437 — у китайских имён
    (частый случай для Huawei) выходит мусор; пробуем GBK."""
    name = info.filename
    if info.flag_bits & 0x800 or all(ord(c) < 128 for c in name):
        return name
    try:
        return name.encode('cp437').decode('gbk')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return name


class _ZipReader:
    def __init__(self, path: str):
        self.z = zipfile.ZipFile(path)
        self._names = {}  # отображаемое имя -> исходное имя члена
        for i in self.z.infolist():
            if not i.is_dir():
                self._names[_fix_zip_name(i)] = i.filename

    def members(self) -> list[tuple[str, int]]:
        sizes = {i.filename: i.file_size for i in self.z.infolist()}
        return [(shown, sizes[orig]) for shown, orig in self._names.items()]

    def read(self, member: str, limit: int) -> bytes | None:
        info = self.z.getinfo(self._names[member])
        if info.file_size > limit:
            return None
        with self.z.open(info) as f:
            return f.read(limit + 1)

    def extract_to(self, member: str, dest: str, limit: int) -> bool:
        info = self.z.getinfo(self._names[member])
        if info.file_size > limit:
            return False
        with self.z.open(info) as src, open(dest, 'wb') as out:
            while True:
                block = src.read(1 << 20)
                if not block:
                    break
                out.write(block)
        return True

    def close(self):
        self.z.close()


class _TarReader:
    def __init__(self, path: str):
        import tarfile
        self.t = tarfile.open(path, 'r:*')

    def members(self) -> list[tuple[str, int]]:
        return [(m.name, m.size) for m in self.t.getmembers() if m.isfile()]

    def read(self, member: str, limit: int) -> bytes | None:
        m = self.t.getmember(member)
        if m.size > limit:
            return None
        f = self.t.extractfile(m)
        return f.read(limit + 1) if f else None

    def extract_to(self, member: str, dest: str, limit: int) -> bool:
        m = self.t.getmember(member)
        if m.size > limit:
            return False
        src = self.t.extractfile(m)
        if src is None:
            return False
        with open(dest, 'wb') as out:
            while True:
                block = src.read(1 << 20)
                if not block:
                    break
                out.write(block)
        return True

    def close(self):
        self.t.close()


class _GzReader:
    """Одиночный .gz (не tar): один член с именем без .gz."""

    def __init__(self, path: str):
        self.path = path
        self.name = os.path.basename(path)[:-3] or 'content'

    def members(self) -> list[tuple[str, int]]:
        return [(self.name, os.path.getsize(self.path))]  # размер сжатого

    def read(self, member: str, limit: int) -> bytes | None:
        import gzip
        with gzip.open(self.path, 'rb') as f:
            data = f.read(limit + 1)
        return None if len(data) > limit else data

    def extract_to(self, member: str, dest: str, limit: int) -> bool:
        import gzip
        written = 0
        with gzip.open(self.path, 'rb') as src, open(dest, 'wb') as out:
            while True:
                block = src.read(1 << 20)
                if not block:
                    break
                written += len(block)
                if written > limit:
                    out.close()
                    os.unlink(dest)
                    return False
                out.write(block)
        return True

    def close(self):
        pass


class _SevenZipReader:
    def __init__(self, path: str):
        import py7zr
        self.z = py7zr.SevenZipFile(path)

    def members(self) -> list[tuple[str, int]]:
        return [(i.filename, i.uncompressed) for i in self.z.list()
                if not i.is_directory]

    def read(self, member: str, limit: int) -> bytes | None:
        sizes = dict(self.members())
        if sizes.get(member, limit + 1) > limit:
            return None
        self.z.reset()
        got = self.z.read(targets=[member])
        bio = got.get(member)
        return bio.read() if bio else None

    def extract_to(self, member: str, dest: str, limit: int) -> bool:
        data = self.read(member, limit)
        if data is None:
            return False
        with open(dest, 'wb') as out:
            out.write(data)
        return True

    def close(self):
        self.z.close()


class _RarReader:
    """Листинг — чистый Python; извлечение требует бэкенд (unar в образе).
    Без бэкенда read/extract вернут None/False с warning — листинг и
    каталог по внутренностям работают всегда."""

    def __init__(self, path: str):
        import rarfile
        self.r = rarfile.RarFile(path)

    def members(self) -> list[tuple[str, int]]:
        return [(i.filename, i.file_size) for i in self.r.infolist()
                if not i.is_dir()]

    def read(self, member: str, limit: int) -> bytes | None:
        info = self.r.getinfo(member)
        if info.file_size > limit:
            return None
        try:
            with self.r.open(member) as f:
                return f.read(limit + 1)
        except Exception as e:  # нет unrar/unar-бэкенда или битый член
            logger.warning('rar read failed (%s): %s', member, e)
            return None

    def extract_to(self, member: str, dest: str, limit: int) -> bool:
        data = self.read(member, limit)
        if data is None:
            return False
        with open(dest, 'wb') as out:
            out.write(data)
        return True

    def close(self):
        self.r.close()


def open_archive(path: str):
    """Формат определяется по СИГНАТУРЕ, не по расширению: в реальной папке
    встречаются rar/7z, названные .zip (Huawei), — расширение лишь фолбэк."""
    with open(path, 'rb') as f:
        head = f.read(8)
    if head.startswith(b'PK'):
        return _ZipReader(path)
    if head.startswith(b'Rar!'):
        return _RarReader(path)
    if head.startswith(b'7z\xbc\xaf'):
        return _SevenZipReader(path)
    import tarfile
    if head.startswith(b'\x1f\x8b'):  # gzip: tar.gz или одиночный .gz
        if tarfile.is_tarfile(path):
            return _TarReader(path)
        return _GzReader(path)
    if tarfile.is_tarfile(path):  # ustar-магия лежит на смещении 257
        return _TarReader(path)
    raise ValueError(f'неизвестный формат (первые байты: {head!r})')


# --- извлечение текста из членов ---

_DOCX_P_RE = re.compile(r'</w:p>')
_DOCX_TAG_RE = re.compile(r'<[^>]+>')


def docx_text(data: bytes) -> str:
    """docx = zip; текст лежит в word/document.xml."""
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        xml = z.read('word/document.xml').decode('utf-8', 'replace')
    text = _DOCX_TAG_RE.sub(' ', _DOCX_P_RE.sub('\n', xml))
    text = html_mod.unescape(text)
    lines = [re.sub(r'\s+', ' ', ln).strip() for ln in text.split('\n')]
    return '\n'.join(ln for ln in lines if ln)


def xlsx_text(data: bytes) -> str:
    """Листы -> строки текстом; openpyxl read-only (потоковый)."""
    import warnings
    from openpyxl import load_workbook
    with warnings.catch_warnings():
        # хуавеевские xlsx массово без default style — шум в журнале NAS
        warnings.simplefilter('ignore')
        wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    parts: list[str] = []
    for ws in wb.worksheets:
        parts.append(f'== Лист: {ws.title} ==')
        for n, row in enumerate(ws.iter_rows(values_only=True)):
            if n >= XLSX_MAX_ROWS:
                parts.append('…(лист обрезан)')
                break
            cells = [str(c).strip() for c in row if c is not None]
            if cells:
                parts.append(' | '.join(cells))
    wb.close()
    return '\n'.join(parts)


def pdf_pages(data: bytes) -> list[str]:
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(data))
    out = []
    for page in reader.pages:
        try:
            out.append((page.extract_text() or '').strip())
        except Exception:
            out.append('')
    return out


def member_kind(name: str) -> str:
    """'doc'/'plain'/'hdx'/'archive'/'' — что делать с членом архива."""
    low = name.lower()
    if low.endswith('.hdx'):
        return 'hdx'
    if low.endswith(TEXT_DOC_EXTS):
        return 'doc'
    if low.endswith(TEXT_PLAIN_EXTS):
        return 'plain'
    if low.endswith(ARCHIVE_EXTS):
        return 'archive'
    return ''


def inner_firmware(members: list[tuple[str, int]]) -> list[tuple[str, str, str]]:
    """(model, version, version_key) из имён членов; подписи и мусор мимо."""
    out: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for path, _size in members:
        base = os.path.basename(path)
        if classify_name(base) == 'signature':
            continue
        models, version, key = parse_firmware_name(base)
        for m in models:
            if (m, version) not in seen:
                seen.add((m, version))
                out.append((m, version, key))
    return out


def archive_text_chunks(path: str, md5: str, arc_name: str,
                        reader=None) -> list[Chunk]:
    """Чанки RAG из текстовых членов архива (потоково, по одному члену)."""
    close = reader is None
    reader = reader or open_archive(path)
    synthetic_chat = int(md5[:12], 16)
    chunks: list[Chunk] = []
    seq = 0
    budget = ARCHIVE_TEXT_TOTAL

    def add(label: str, text_parts: list[tuple[str, str]]):
        """text_parts: [(подзаголовок, текст куска)]"""
        nonlocal seq
        for sub, part in text_parts:
            seq += 1
            chunks.append(Chunk(
                chat_id=synthetic_chat, topic_id=0,
                topic_name=label[:200], date_from='', date_to='',
                msg_first=seq, msg_last=seq, authors='',
                text=f'Файл «{label}»{sub}\n{part}'))

    try:
        for member, size in sorted(reader.members()):
            kind = member_kind(member)
            if kind not in ('doc', 'plain') or size > budget:
                continue
            limit = DOC_MEMBER_LIMIT if kind == 'doc' else PLAIN_MEMBER_LIMIT
            try:
                data = reader.read(member, limit)
            except Exception as e:
                logger.warning('archive member read failed %s -> %s: %s',
                               arc_name, member, e)
                continue
            if data is None:  # больше лимита или нет rar-бэкенда
                continue
            budget -= len(data)
            label = f'{arc_name} → {os.path.basename(member)}'
            low = member.lower()
            try:
                if low.endswith('.pdf'):
                    pages = pdf_pages(data)
                    buf: list[str] = []
                    first = 0
                    size_c = 0
                    parts: list[tuple[str, str]] = []
                    for i, ptext in enumerate(pages, 1):
                        if not ptext:
                            continue
                        if buf and size_c + len(ptext) > CHUNK_CHARS:
                            parts.append((f', стр. {first}-{i - 1}',
                                          '\n'.join(buf)))
                            buf, size_c = [], 0
                        if not buf:
                            first = i
                        buf.append(ptext)
                        size_c += len(ptext)
                    if buf:
                        parts.append((f', стр. {first}-{len(pages)}',
                                      '\n'.join(buf)))
                    add(label, parts)
                else:
                    if low.endswith('.docx'):
                        text = docx_text(data)
                    elif low.endswith('.xlsx'):
                        text = xlsx_text(data)
                    elif low.endswith(('.htm', '.html')):
                        text = page_text(data.decode('utf-8', 'replace'))
                    else:
                        text = data.decode('utf-8', 'replace')
                    text = text.strip()
                    if len(text) < 80:  # пустышки не инжестим
                        continue
                    body_parts = _split_body(text)
                    add(label, [(f' (часть {n})' if len(body_parts) > 1 else '',
                                 p) for n, p in enumerate(body_parts, 1)])
            except Exception as e:
                logger.warning('archive member parse failed %s -> %s: %s',
                               arc_name, member, e)
    finally:
        if close:
            reader.close()
    return chunks


def extract_containers(path: str, arc_name: str, dest_dir: str,
                       reader=None) -> int:
    """Извлекает .hdx из архива в dest_dir (потоково). Возвращает число."""
    close = reader is None
    reader = reader or open_archive(path)
    os.makedirs(dest_dir, exist_ok=True)
    n = 0
    try:
        for member, size in reader.members():
            if member_kind(member) != 'hdx' or size > CONTAINER_LIMIT:
                continue
            base = os.path.basename(member)
            if not base or base.startswith('.'):
                continue
            dest = os.path.join(dest_dir, base)
            if os.path.exists(dest) and os.path.getsize(dest) == size:
                continue  # уже извлечён
            try:
                if reader.extract_to(member, dest, CONTAINER_LIMIT):
                    logger.warning('HedEx-пакет извлечён из %s: %s (%d МБ)',
                                   arc_name, base, size >> 20)
                    n += 1
            except Exception as e:
                logger.warning('container extract failed %s -> %s: %s',
                               arc_name, member, e)
                if os.path.exists(dest):
                    os.unlink(dest)
    finally:
        if close:
            reader.close()
    return n


def list_archives(folder: str) -> list[str]:
    """Относительные пути всех архивов в папке, рекурсивно.
    hedex_extracted/ пропускается (там наши же контейнеры)."""
    out: list[str] = []
    for root, dirs, files in os.walk(folder):
        dirs[:] = [d for d in dirs
                   if not d.startswith('.') and d != HEDEX_SUBDIR]
        for name in files:
            if is_archive(name):
                out.append(os.path.relpath(os.path.join(root, name), folder))
    return sorted(out)


def scan_archives(store, folder: str) -> tuple[int, int]:
    """Для dry-run: (новых архивов, оценка символов текста). Быстрый путь:
    zip/rar листаются (central directory), tar/7z оцениваются в 5% размера."""
    journal = _load_md5_journal(folder)
    files = 0
    chars = 0
    for rel in list_archives(folder):
        path = os.path.join(folder, rel)
        md5 = journal.get(os.path.basename(rel)) or _file_md5(path)
        if store.get_state(f'archive_scanned:{md5}'):
            continue
        files += 1
        low = rel.lower()
        try:
            if low.endswith(('.zip', '.rar')):
                reader = open_archive(path)
                text_bytes = sum(
                    min(s, DOC_MEMBER_LIMIT) for m, s in reader.members()
                    if member_kind(m) in ('doc', 'plain'))
                reader.close()
                # pdf/docx: полезного текста ~10% от байтов файла
                chars += int(text_bytes * 0.1)
            else:
                chars += int(os.path.getsize(path) * 0.05)
        except Exception as e:
            logger.warning('archive scan failed %s: %s', rel, e)
    return files, chars


def _process_archive_sync(path: str, arc_name: str, md5: str,
                          folder: str) -> tuple[list, list, int]:
    """Тяжёлая синхронная часть одного архива (листинг, извлечение .hdx,
    парсинг текста). Вызывается через asyncio.to_thread: часы распаковки
    в event loop качалки глушат Telethon — сокет не читается, и после
    возврата сыплется «Server sent a very old message» + Security error."""
    reader = open_archive(path)
    try:
        members = reader.members()
        extracted = extract_containers(path, arc_name,
                                       os.path.join(folder, HEDEX_SUBDIR),
                                       reader=reader)
        chunks = archive_text_chunks(path, md5, arc_name, reader=reader)
    finally:
        reader.close()
    return members, chunks, extracted


async def process_archives(store, folder: str, progress=None,
                           max_cost: float | None = None
                           ) -> tuple[int, int, float]:
    """Полный проход: листинг + каталог + текст в RAG + извлечение .hdx.
    Возвращает (архивов, чанков, стоимость $). Работа с store — только
    из основного потока (sqlite-коннект не потокобезопасен)."""
    import asyncio

    from kb_ingest import BudgetExceeded, EMBED_BATCH, embed_cost, embed_texts

    journal = _load_md5_journal(folder)
    done = 0
    chunks_total = 0
    cost = 0.0
    for rel in list_archives(folder):
        path = os.path.join(folder, rel)
        arc_name = os.path.basename(rel)
        md5 = journal.get(arc_name)
        if not md5:
            md5 = await asyncio.to_thread(_file_md5, path)
        if store.get_state(f'archive_scanned:{md5}'):
            continue
        try:
            members, chunks, _ = await asyncio.to_thread(
                _process_archive_sync, path, arc_name, md5, folder)
        except Exception as e:
            logger.warning('archive processing failed %s: %s', rel, e)
            continue
        store.upsert_archive_files(md5, members)
        doc_id = store.doc_id_by_md5(md5)
        if doc_id is not None:
            for model, version, key in inner_firmware(members):
                store.upsert_firmware(doc_id, model, version, key,
                                      source='archive')

        known = store.existing_ids([c.id for c in chunks])
        new_chunks = [c for c in chunks if c.id not in known]
        for i in range(0, len(new_chunks), EMBED_BATCH):
            part = new_chunks[i:i + EMBED_BATCH]
            vectors = await embed_texts([c.text for c in part])
            for c, v in zip(part, vectors):
                c.embedding = v
            store.upsert_chunks(part)
            cost += embed_cost([c.text for c in part])
            if max_cost is not None and cost >= max_cost:
                raise BudgetExceeded(cost)
        store.set_state(f'archive_scanned:{md5}', arc_name)
        done += 1
        chunks_total += len(new_chunks)
        if progress:
            progress('archive', done, chunks_total, cost)
    return done, chunks_total, cost


def _selftest() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        # --- zip с текстом, docx, вложенным hdx и прошивками внутри ---
        arc = os.path.join(tmp, 'Сборка_апгрейда.zip')
        docx = io.BytesIO()
        with zipfile.ZipFile(docx, 'w') as z:
            z.writestr('word/document.xml',
                       '<w:document><w:p><w:r><w:t>Дельта фич: WPA3 '
                       'обязателен &amp; PMF включён.</w:t></w:r></w:p>'
                       '<w:p><w:r><w:t>Вторая строка про DFS-каналы и '
                       'ограничения радиопланирования апгрейда.</w:t></w:r>'
                       '</w:p></w:document>')
        inner_hdx = io.BytesIO()
        with zipfile.ZipFile(inner_hdx, 'w') as z:
            z.writestr('profile.xml', '<profile/>')
        with zipfile.ZipFile(arc, 'w') as z:
            z.writestr('S5735-S-V2_V200R024SPH121.pat', b'\x00' * 64)
            z.writestr('inner/AC6805_V200R023C00SPC100.cc', b'\x00' * 64)
            z.writestr('inner/AC6805_V200R023C00SPC100.cc.asc', b'sig')
            z.writestr('Release_Notes.txt',
                       'Исправлено: падение CAPWAP-туннеля при роуминге '
                       'между AP разных серий; новое: поддержка WPA3-SAE '
                       'на радиомодулях 6 ГГц, ограничения смотри таблицу.')
            z.writestr('доки/delta.docx', docx.getvalue())
            z.writestr('WLAN_V200R024C00_en.hdx', inner_hdx.getvalue())
            z.writestr('image.bin', b'\x00' * 128)

        reader = open_archive(arc)
        members = reader.members()
        assert ('image.bin' in dict(members)) and len(members) == 7, members

        fw = inner_firmware(members)
        assert ('S5735-S-V2', 'V200R024SPH121') in [(m, v) for m, v, _ in fw], fw
        assert ('AC6805', 'V200R023C00SPC100') in [(m, v) for m, v, _ in fw], fw
        # подпись .asc не плодит дубль — dedup по (model, version)
        assert len([1 for m, v, _ in fw if m == 'AC6805']) == 1, fw

        md5 = _file_md5(arc)
        chunks = archive_text_chunks(arc, md5, 'Сборка_апгрейда.zip')
        texts = {c.topic_name: c.text for c in chunks}
        assert any('Release_Notes.txt' in n for n in texts), texts.keys()
        assert any('delta.docx' in n for n in texts), texts.keys()
        rn = next(t for n, t in texts.items() if 'Release_Notes' in n)
        assert 'CAPWAP' in rn and rn.startswith('Файл «Сборка_апгрейда.zip → ')
        dx = next(t for n, t in texts.items() if 'delta.docx' in n)
        assert 'WPA3' in dx and 'PMF' in dx and '&' in dx, dx
        assert all(c.chat_id > 0 and c.topic_id == 0 for c in chunks)

        # контейнер: hdx извлечён потоково, повторно — нет
        dest = os.path.join(tmp, HEDEX_SUBDIR)
        assert extract_containers(arc, 'a.zip', dest) == 1
        assert os.listdir(dest) == ['WLAN_V200R024C00_en.hdx']
        assert extract_containers(arc, 'a.zip', dest) == 0, 'идемпотентность'
        reader.close()

        # --- рекурсивный листинг папки, hedex_extracted исключён ---
        os.makedirs(os.path.join(tmp, 'sub'), exist_ok=True)
        os.replace(arc, os.path.join(tmp, 'sub', 'Сборка_апгрейда.zip'))
        with zipfile.ZipFile(os.path.join(dest, 'fake.zip'), 'w') as z:
            z.writestr('x', 'y')
        rels = list_archives(tmp)
        assert rels == [os.path.join('sub', 'Сборка_апгрейда.zip')], rels

        # --- одиночный .gz ---
        import gzip
        gz = os.path.join(tmp, 'notes.txt.gz')
        with gzip.open(gz, 'wt', encoding='utf-8') as f:
            f.write('Список исправлений в этой ветке достаточно длинный, '
                    'чтобы пройти фильтр пустышек: CAPWAP, DFS, роуминг.')
        r = open_archive(gz)
        assert r.members()[0][0] == 'notes.txt'
        assert b'CAPWAP' in r.read('notes.txt', 1 << 20)
        r.close()

        # --- сигнатура важнее расширения: zip под именем .rar (реальный
        # кейс папки: rar/7z, названные .zip) ---
        fake = os.path.join(tmp, 'на_самом_деле.rar')
        with zipfile.ZipFile(fake, 'w') as z:
            z.writestr('inner.txt', 'x' * 100)
        r = open_archive(fake)
        assert isinstance(r, _ZipReader), type(r)
        r.close()
        try:
            open_archive(__file__)  # обычный python-файл — не архив
            raise SystemExit('FAIL: ожидали ValueError')
        except ValueError as e:
            assert 'первые байты' in str(e), e

        # --- xlsx (если openpyxl доступен) ---
        try:
            from openpyxl import Workbook
        except ImportError:
            print('kb_archive selftest: OK (xlsx пропущен — нет openpyxl)')
            return
        wb = Workbook()
        ws = wb.active
        ws.title = 'Delta Features'
        ws.append(['Фича', 'R023', 'R025'])
        ws.append(['WPA3-SAE', 'нет', 'да'])
        buf = io.BytesIO()
        wb.save(buf)
        text = xlsx_text(buf.getvalue())
        assert 'Delta Features' in text and 'WPA3-SAE | нет | да' in text, text

    print('kb_archive selftest: OK')


if __name__ == '__main__':
    _selftest()
