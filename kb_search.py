"""Отладочный поиск по базе знаний без LLM.

    python kb_search.py "как прошить MA5608T"
    python kb_search.py --space b4 "стратегия для youtube"

Нужны ключ эмбеддингов и доступ к KB_DB_PATH. Без --space ищет по всем
областям — так же, как бот в личке админа.
"""
from __future__ import annotations

import asyncio
import sys

from kb_ingest import embed_texts
from kb_store import open_store


async def main() -> None:
    args = sys.argv[1:]
    space = None
    if len(args) > 1 and args[0] == '--space':
        space, args = args[1], args[2:]
    query = ' '.join(args).strip()
    if not query:
        raise SystemExit('usage: python kb_search.py [--space SLUG] "вопрос"')
    store = open_store()
    vector = (await embed_texts([query]))[0]
    hits = store.search(query, vector, top_k=8, space=space)
    if not hits:
        print('Ничего не найдено')
        return
    for i, h in enumerate(hits, 1):
        preview = h.text[:200].replace('\n', ' | ')
        print(f'{i}. score={h.score:.4f} [{h.space or "—"}] топик «{h.topic_name}» '
              f'{h.date_from} msg={h.msg_first}')
        print(f'   {preview}')


if __name__ == '__main__':
    asyncio.run(main())
