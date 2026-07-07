"""Инжест PDF-документации из папки загрузок в базу знаний (флаг KB_PDF=1).

Берётся только текстовый слой (pypdf) — сканы без текстового слоя пропускаются
(OCR сознательно не делаем, дорого). Файл учитывается по MD5 в state
(pdf_ingested:<md5>): повторные прогоны уже проиндексированное не трогают.

Чанки PDF получают синтетический ПОЛОЖИТЕЛЬНЫЙ chat_id (из MD5 файла) —
у Telegram-чатов id отрицательные, поэтому kb_bot отличает PDF-источники
и показывает «файл, стр. N» вместо ссылки t.me.
"""
from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime

from kb_ingest import (BudgetExceeded, EMBED_BATCH, embed_cost, embed_texts)
from kb_store import Chunk

logger = logging.getLogger(__name__)

PDF_CHUNK_CHARS = 4000


def _file_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 16), b''):
            h.update(block)
    return h.hexdigest()


def _list_pdfs(folder: str) -> list[str]:
    if not os.path.isdir(folder):
        return []
    return sorted(n for n in os.listdir(folder) if n.lower().endswith('.pdf'))


def scan_pdfs(folder: str) -> tuple[int, int]:
    """Для dry-run: (файлов, страниц). Текст не извлекается — быстро."""
    from pypdf import PdfReader
    files = 0
    pages = 0
    for name in _list_pdfs(folder):
        try:
            pages += len(PdfReader(os.path.join(folder, name)).pages)
            files += 1
        except Exception as e:
            logger.warning('PDF scan failed for %s: %s', name, e)
    return files, pages


def _pdf_chunks(path: str, name: str, md5: str) -> list[Chunk]:
    from pypdf import PdfReader
    synthetic_chat = int(md5[:12], 16)  # >0, см. docstring модуля
    mdate = datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d')
    reader = PdfReader(path)

    chunks: list[Chunk] = []

    def make(buf: list[str], first: int, last: int) -> Chunk:
        return Chunk(
            chat_id=synthetic_chat, topic_id=0, topic_name=name,
            date_from=mdate, date_to=mdate,
            msg_first=first, msg_last=last, authors='',
            text=f'Документ «{name}», стр. {first}-{last}\n' + '\n'.join(buf))

    buf: list[str] = []
    buf_first = 0
    buf_last = 0
    size = 0
    for i, page in enumerate(reader.pages, 1):
        try:
            text = (page.extract_text() or '').strip()
        except Exception:
            text = ''
        if not text:
            continue
        if buf and size + len(text) > PDF_CHUNK_CHARS:
            chunks.append(make(buf, buf_first, buf_last))
            buf = []
            size = 0
        if not buf:
            buf_first = i
        buf.append(text)
        buf_last = i
        size += len(text)
    if buf:
        chunks.append(make(buf, buf_first, buf_last))
    return chunks


async def ingest_pdfs(store, folder: str, progress=None,
                      max_cost: float | None = None) -> tuple[int, int, float]:
    """Инжест новых PDF из folder. Возвращает (файлов, чанков, стоимость $)."""
    files = 0
    chunks_total = 0
    cost = 0.0
    for name in _list_pdfs(folder):
        path = os.path.join(folder, name)
        md5 = _file_md5(path)
        if store.get_state(f'pdf_ingested:{md5}'):
            continue
        try:
            chunks = _pdf_chunks(path, name, md5)
        except Exception as e:
            logger.warning('PDF parse failed for %s: %s', name, e)
            continue
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
        store.set_state(f'pdf_ingested:{md5}', name)
        files += 1
        chunks_total += len(new_chunks)
        if progress:
            progress('pdf', files, 0, cost)
    return files, chunks_total, cost
