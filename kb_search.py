"""Отладочный поиск по базе знаний без LLM.

Использование: python kb_search.py "как прошить MA5608T"
Нужны OPENAI_API_KEY (эмбеддинг запроса) и доступ к KB_DB_PATH.
"""
from __future__ import annotations

import asyncio
import sys

from kb_ingest import embed_texts
from kb_store import open_store


async def main() -> None:
    query = ' '.join(sys.argv[1:]).strip()
    if not query:
        raise SystemExit('usage: python kb_search.py "вопрос"')
    store = open_store()
    vector = (await embed_texts([query]))[0]
    hits = store.search(query, vector, top_k=8)
    if not hits:
        print('Ничего не найдено')
        return
    for i, h in enumerate(hits, 1):
        preview = h.text[:200].replace('\n', ' | ')
        print(f'{i}. score={h.score:.4f} топик «{h.topic_name}» '
              f'{h.date_from} msg={h.msg_first}')
        print(f'   {preview}')


if __name__ == '__main__':
    asyncio.run(main())
