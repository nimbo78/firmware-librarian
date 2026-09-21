"""Проверка MCP-сервера без сети: инструменты через встроенный клиент
и HTTP-слой (токен, /health) через тестовый ASGI-клиент.

Отдельный файл, как и остальные *_check: kb_mcp на импорте читает
окружение и открывает базу, поэтому оно задаётся ДО импорта. Эмбеддер,
расширение запроса и OpenAI подменены — проверяется проводка: область
доходит до поиска, чужие фрагменты не примешиваются, указатель «#b4»
работает, неизвестная область даёт понятную ошибку, токен закрывает /mcp,
но не /health.

Запуск: python kb_mcp_check.py
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
import types

_TMP = tempfile.mkdtemp()
_SPACES = os.path.join(_TMP, 'spaces.toml')
with open(_SPACES, 'w', encoding='utf-8') as _f:
    _f.write('''
[huawei]
title = "Huawei"
persona = "инженеров по оборудованию Huawei"
chats = [-1001]

[b4]
title = "B4 и обход DPI"
persona = "администраторов B4"
hints = "nfqueue, стратегии, geodata"
chats = [-2001]
answer = []
''')
os.environ.update(
    KB_DB_PATH=os.path.join(_TMP, 'kb.sqlite'), EMBED_DIM='4',
    KB_SPACES_FILE=_SPACES, KB_MCP_TOKEN='secret-token',
    OPENAI_API_KEY='sk-test')

import kb_answer  # noqa: E402
import kb_mcp as M  # noqa: E402
from kb_store import Chunk  # noqa: E402

VEC = [1.0, 0.0, 0.0, 0.0]


def _chunk(space: str, chat: int, first: int, topic: str, text: str) -> Chunk:
    return Chunk(chat, 1, topic, '2026-09-01', '2026-09-02', first, first + 3,
                 'ivan', f'Топик «{topic}»\n{text}', VEC, space=space)


def _seed() -> None:
    M.store.upsert_chunks([
        _chunk('b4', -1002001, 10, 'Настройка',
               '[2026-09-01 10:00] ivan: стратегию для youtube выбирает автоподбор, '
               'смотри логи nfqueue'),
        _chunk('b4', -1002001, 20, 'Роутер',
               '[2026-09-02 11:00] max: на openwrt ставится пакетом, geodata '
               'обновляется сама'),
        _chunk('huawei', -1001001, 30, 'Прошивки',
               '[2026-09-01 12:00] igor: S5735 до R025 прошивается через bootrom, '
               'youtube тут ни при чём'),
    ])


def _stub_network() -> None:
    """Эмбеддер и LLM подменены: вектора одинаковые, различает полнотекст."""
    async def fake_embed(texts):
        return [VEC for _ in texts]

    async def no_expand(question, hints=''):
        return []

    class _Resp:
        def __init__(self, text: str):
            self.choices = [types.SimpleNamespace(
                message=types.SimpleNamespace(content=text))]

    class FakeOA:
        class chat:
            class completions:
                @staticmethod
                async def create(**kwargs):
                    return _Resp('Коротко: стратегию подбирает автоподбор [1].')

    kb_answer.embed_texts = fake_embed
    kb_answer.expand_query = no_expand
    kb_answer.openai_client = lambda: FakeOA()


async def _tools() -> None:
    from mcp.client import Client

    async with Client(M.mcp) as c:
        names = {t.name for t in (await c.list_tools()).tools}
        assert names == {'kb_search', 'kb_answer', 'kb_spaces'}, names

        # схема плоская: параметры — прямо в arguments, без обёртки params
        tool = next(t for t in (await c.list_tools()).tools if t.name == 'kb_search')
        assert 'query' in tool.input_schema['properties'], tool.input_schema
        assert 'youtube' in tool.input_schema['properties']['query']['description']

        # область параметром: только b4, чужой huawei-фрагмент не примешан
        r = await c.call_tool('kb_search', {'query': 'youtube', 'space': 'b4'})
        text = r.content[0].text
        assert 'автоподбор' in text and 'S5735' not in text, text
        assert '#b4' in text and 'https://t.me/c/2001/10' in text, text
        assert 'область: B4' in text, text

        # указатель в тексте работает так же, как в чате
        r = await c.call_tool('kb_search', {'query': '#huawei прошивка bootrom'})
        text = r.content[0].text
        assert 'S5735' in text and 'автоподбор' not in text, text

        # без области — по всем: находится и то, и другое
        r = await c.call_tool('kb_search', {'query': 'youtube', 'expand': False})
        text = r.content[0].text
        assert 'автоподбор' in text and 'S5735' in text, text

        # json для обработки
        r = await c.call_tool('kb_search', {'query': 'openwrt', 'space': 'b4',
                                            'response_format': 'json'})
        data = json.loads(r.content[0].text)
        assert data['count'] >= 1 and data['fragments'][0]['space'] == 'b4', data
        assert data['fragments'][0]['link'].startswith('https://t.me/c/'), data

        # обрезка длинных фрагментов
        r = await c.call_tool('kb_search', {'query': 'openwrt', 'space': 'b4',
                                            'max_chars': 200})
        assert '[обрезано]' not in r.content[0].text  # короче лимита — целиком

        # неизвестная область — понятная ошибка со списком
        r = await c.call_tool('kb_search', {'query': 'что угодно', 'space': 'nope'})
        text = r.content[0].text
        assert 'Нет области' in text and '#b4' in text and '#huawei' in text, text

        # готовый ответ бота с пометкой области
        r = await c.call_tool('kb_answer', {'question': 'как выбрать стратегию',
                                            'space': 'b4'})
        text = r.content[0].text
        assert 'автоподбор' in text and 'Область: B4' in text, text

        # инвентарь
        r = await c.call_tool('kb_spaces', {})
        text = r.content[0].text
        assert '#b4 — B4 и обход DPI (2 фрагментов)' in text, text
        assert 'чат -1002001: 2 фрагм.' in text and '#huawei' in text, text
        assert 'терминология: nfqueue' in text, text
    print('  инструменты: поиск по области, указатель, json, ошибки, ответ — OK')


async def _http() -> None:
    """HTTP-слой in-process, в ТОМ ЖЕ потоке, что и база: TestClient
    Starlette гоняет приложение в отдельном потоке, а SQLite-соединение
    однопоточное. В проде uvicorn крутит loop в потоке, где открыта база."""
    import httpx

    rpc = {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list', 'params': {}}
    hdr = {'Accept': 'application/json, text/event-stream',
           'Content-Type': 'application/json'}
    app = M.build_app()
    assert isinstance(app, M._BearerAuth), 'с токеном /mcp обязан быть закрыт'
    inner = app.app
    # lifespan запускает менеджер сессий Streamable HTTP — без него /mcp
    # отвечает 500; ASGITransport lifespan сам не гоняет
    async with inner.router.lifespan_context(inner):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url='http://kb-mcp') as tc:
            h = await tc.get('/health')
            assert h.status_code == 200 and h.json()['spaces'] == ['huawei', 'b4'], h.text

            r = await tc.post('/mcp', json=rpc, headers=hdr)
            assert r.status_code == 401, (r.status_code, r.text)
            r = await tc.post('/mcp', json=rpc,
                              headers={**hdr, 'Authorization': 'Bearer wrong'})
            assert r.status_code == 401, r.status_code

            r = await tc.post('/mcp', json=rpc,
                              headers={**hdr, 'Authorization': 'Bearer secret-token'})
            assert r.status_code == 200, (r.status_code, r.text[:300])
            assert 'kb_search' in r.text, r.text[:300]
    print('  HTTP: /health открыт, /mcp закрыт токеном, tools/list отвечает — OK')


def _selftest() -> None:
    import logging
    # библиотека mcp на INFO печатает каждый запрос rich-таблицами — это
    # шум, за которым не видно результата проверки
    for name in ('mcp', 'httpx', 'kb_bot', 'kb_mcp'):
        logging.getLogger(name).setLevel(logging.WARNING)
    _seed()
    _stub_network()
    asyncio.run(_tools())
    asyncio.run(_http())
    M.store.close()
    print('kb_mcp_check: OK')


if __name__ == '__main__':
    _selftest()
