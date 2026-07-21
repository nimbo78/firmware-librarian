"""Переэмбеддинг всей базы знаний при смене модели/размерности эмбеддингов.

Зачем: вектора разных моделей несравнимы (даже при одинаковой размерности),
поэтому смена EMBED_MODEL/EMBED_DIM требует пересоздания chunks_vec и
прогона всех текстов чанков через новую модель. Telegram НЕ нужен —
тексты уже в базе. Процедура (оба сервиса остановить, чтобы поиск и инжест
не работали по полупустой таблице):

    # в .env выставить новые EMBED_MODEL / EMBED_DIM (для стороннего
    # OpenAI-совместимого провайдера — ещё EMBED_API_BASE / EMBED_API_KEY,
    # например DeepInfra: https://api.deepinfra.com/v1/openai), затем:
    docker compose stop librarian kb-bot
    docker compose run --rm librarian python kb_reembed.py
    docker compose start librarian kb-bot

Стоимость печатается до старта (у text-embedding-3-large — $0.13/1M токенов).
Прерванный прогон безопасно перезапускать: он просто начнёт заново
(таблица пересоздаётся), двойной оплаты нет смысла бояться — это один
дешёвый проход. state `embed_cfg` обновляется только после успеха, до тех
пор качалка и бот отказываются работать с несогласованной базой (guard).
"""
from __future__ import annotations

import asyncio
import os

from kb_ingest import EMBED_BATCH, EMBED_PRICE_PER_MTOK, embed_cfg, embed_texts
from kb_store import open_store


async def main() -> None:
    if os.getenv('EMBED_API_BASE', '').strip():
        if not os.getenv('EMBED_API_KEY', '').strip():
            raise SystemExit('EMBED_API_BASE задан, а EMBED_API_KEY нет')
    elif not os.getenv('OPENAI_API_KEY'):
        raise SystemExit('OPENAI_API_KEY не задан')
    model = os.getenv('EMBED_MODEL', 'text-embedding-3-small')
    dim = int(os.getenv('EMBED_DIM', '512'))
    store = open_store()
    chunks = store.chunks_iter()
    if not chunks:
        print('База пуста — переэмбеддировать нечего.')
        store.set_state('embed_cfg', embed_cfg())
        return
    tokens = sum(len(t) for _, t in chunks) // 3
    price = EMBED_PRICE_PER_MTOK
    print(f'Чанков: {len(chunks)}, ~{tokens} токенов')
    print(f'Модель: {model}, размерность: {dim}')
    print(f'Оценка стоимости: ~${tokens / 1e6 * price:.2f}')
    old = store.get_state('embed_cfg', '<не зафиксирована>')
    print(f'Старая конфигурация: {old} -> новая: {embed_cfg()}')

    print('Пересоздаю векторную таблицу...', flush=True)
    store.reset_vectors(dim)
    done = 0
    for i in range(0, len(chunks), EMBED_BATCH):
        part = chunks[i:i + EMBED_BATCH]
        vectors = await embed_texts([t for _, t in part])
        for (rowid, _), vec in zip(part, vectors):
            store.set_vector(rowid, vec)
        done += len(part)
        print(f'  эмбеддинги: {done}/{len(chunks)}', flush=True)
    store.set_state('embed_cfg', embed_cfg())
    store.backup()
    store.add_event('reembed',
                    f'Переэмбеддинг завершён: {len(chunks)} чанков, {embed_cfg()}')
    print(f'Готово: {done} чанков на {embed_cfg()}. Запускай сервисы обратно.')


if __name__ == '__main__':
    asyncio.run(main())
