"""MCP-сервер базы знаний: знания из чатов — прямо в Claude Code.

Третий сервис compose (kb-mcp): Streamable HTTP на порту 8765 внутри
контейнера (наружу — KB_MCP_PORT). Ни Telegram, ни сессий: только база
(read-only) и те же ключи эмбеддингов и OpenAI, что у бота. Подключение
на ПК, где работает Claude Code:

    claude mcp add --transport http librarian http://<nas>:8765/mcp \\
        --header "Authorization: Bearer <KB_MCP_TOKEN>"

Инструменты:
  kb_spaces — какие области есть и что в них лежит (бесплатно, без сети);
  kb_search — фрагменты чатов и документации по вопросу. Главный: Claude
              сильнее gpt-5-mini и из сырых фрагментов с датами и ссылками
              соберёт ответ точнее, чем готовый; стоит один эмбеддинг
              запроса (+ дешёвое расширение, отключаемое);
  kb_answer — готовый ответ бота с источниками, как /ask в Telegram
              (~$0.01: расширение запроса + генерация).

Защита: bearer-токен KB_MCP_TOKEN (пусто = без авторизации, только для
доверенной сети — об этом предупреждение на старте) и, по желанию,
KB_MCP_ALLOWED_HOSTS от DNS-rebinding — список АДРЕСОВ ЭТОГО СЕРВЕРА,
какими их пишет клиент в URL (заголовок Host), а не адресов клиентов.

Проверка без сети: python kb_mcp_check.py
"""
from __future__ import annotations

import hmac
import json
import logging
import os
from typing import Annotated, Literal

from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from pydantic import Field
from starlette.responses import JSONResponse

from kb_answer import answer_question, search_expanded
from kb_render import msg_link
from kb_spaces import Scope, load_spaces
from kb_store import open_store

logger = logging.getLogger('kb_mcp')

LISTEN_PORT = 8765                       # внутри контейнера; наружу — KB_MCP_PORT
TOKEN = os.getenv('KB_MCP_TOKEN', '').strip()
ALLOWED_HOSTS = [h.strip() for h in os.getenv('KB_MCP_ALLOWED_HOSTS', '').split(',')
                 if h.strip()]
MAX_CHARS_DEFAULT = 2000

SPACES = load_spaces()
store = open_store()

_READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False,
                             idempotent_hint=True, open_world_hint=False)


def _instructions() -> str:
    """Подсказка клиенту при подключении: что за база и как ей пользоваться.
    Список областей — живой, из конфига, чтобы Claude знал slug'и."""
    areas = ', '.join(f'#{s.slug} — {s.label}' for s in SPACES.all)
    return (
        'База знаний из истории Telegram-чатов и документации '
        f'(firmware-librarian). Области: {areas}. '
        'Начинай с kb_search: он возвращает сырые фрагменты обсуждений с '
        'датами и ссылками — из них ответ собирается точнее, чем готовый. '
        'kb_answer — когда нужен ответ в том виде, в каком его даёт '
        'телеграм-бот. Область задаётся параметром space или указателем '
        '«#slug» в начале текста; без неё поиск идёт по всем областям.')


mcp = MCPServer('librarian_kb', title='Firmware Librarian — база знаний',
                instructions=_instructions())


def _scope(space: str | None, text: str) -> tuple[Scope | None, str, str]:
    """(область поиска, текст без указателя, текст ошибки).

    Явный параметр важнее указателя в тексте. Без обоих — resolve для
    «чата 0»: он ни к чему не привязан, значит поиск по всем областям (как
    в личке админа), а указатель «#b4» в тексте всё равно учитывается."""
    if space:
        sp = SPACES.get(space)
        if sp is None:
            return None, text, (f'Нет области «{space}». Доступны: '
                                + ', '.join(f'#{s}' for s in SPACES.slugs)
                                + ' — список с описанием даёт kb_spaces.')
        return Scope(sp, explicit=True, multi=len(SPACES.all) > 1), text, ''
    scope, rest = SPACES.resolve(0, text)
    return scope, rest, ''


def _kind(hit) -> str:
    """Конвенция chat_id/topic_id (см. ScoredChunk): чат <0, документация
    HedEx >0 и topic_id=1, PDF/архивы >0 и topic_id=0."""
    if hit.chat_id < 0:
        return 'чат'
    return 'документация' if hit.topic_id == 1 else 'файл'


def _hit_dict(i: int, hit, max_chars: int) -> dict:
    text = hit.text
    if len(text) > max_chars:
        text = text[:max_chars] + ' …[обрезано]'
    return {
        'n': i, 'space': hit.space or None, 'kind': _kind(hit),
        'topic': hit.topic_name or None, 'date': hit.date_from,
        'link': msg_link(hit.chat_id, hit.msg_first),
        'chat_id': hit.chat_id, 'msg_id': hit.msg_first, 'text': text,
    }


def _render_hits(hits, scope: Scope, fell_back: bool, max_chars: int,
                 fmt: str, expanded: bool) -> str:
    items = [_hit_dict(i, h, max_chars) for i, h in enumerate(hits, 1)]
    if fmt == 'json':
        return json.dumps({'scope': scope.label, 'fell_back': fell_back,
                           'expanded': expanded, 'count': len(items),
                           'fragments': items}, ensure_ascii=False, indent=1)
    head = f'Найдено фрагментов: {len(items)} · область: {scope.label}'
    if fell_back:
        head += ' (в запрошенной пусто — показаны все области)'
    head += ' · расширение запроса: ' + ('да' if expanded else 'нет')
    lines = [head, '']
    for it in items:
        where = f'{it["kind"]}: {it["topic"]}' if it['topic'] else it['kind']
        tag = f'#{it["space"]} · ' if it['space'] else ''
        link = f' · {it["link"]}' if it['link'] else ''
        lines.append(f'### [{it["n"]}] {tag}{where} · {it["date"]}{link}')
        lines.append(it['text'])
        lines.append('')
    return '\n'.join(lines).rstrip()


@mcp.tool(name='kb_search', title='Поиск фрагментов в базе знаний',
          annotations=_READ_ONLY)
async def kb_search(
    query: Annotated[str, Field(
        min_length=2, max_length=500,
        description='Вопрос или ключевые слова. Область можно задать указателем '
                    'в начале: «#b4 стратегия для youtube».')],
    space: Annotated[str | None, Field(
        description='Slug области (см. kb_spaces). Пусто — искать по всем.')] = None,
    limit: Annotated[int, Field(ge=1, le=20, description='Сколько фрагментов вернуть')] = 8,
    expand: Annotated[bool, Field(
        description='Расширять запрос через LLM (разворот сленга в терминологию '
                    'области; дешёвый вызов). Выключи для точного поиска по '
                    'конкретному термину или имени файла.')] = True,
    max_chars: Annotated[int, Field(
        ge=200, le=6000,
        description='Обрезать каждый фрагмент до N символов')] = MAX_CHARS_DEFAULT,
    response_format: Annotated[Literal['markdown', 'json'], Field(
        description='markdown — для чтения, json — для обработки')] = 'markdown',
) -> str:
    """Гибридный поиск (эмбеддинги + полнотекст, слияние RRF) по истории
    Telegram-чатов и документации. Возвращает фрагменты бесед: область,
    топик, дата, ссылка на сообщение и сам текст — с них и стоит начинать
    любой вопрос про B4, Telemt, Huawei и остальные области из kb_spaces.

    Фрагмент — не одно сообщение, а кусок беседы (до паузы >30 минут или
    ~4000 символов): контекст обсуждения важнее отдельной реплики. Ссылка
    ведёт на первое сообщение фрагмента. Для документации (HedEx, PDF)
    ссылки нет — вместо неё название документа и раздел.

    Ничего не найдено в своей области — поиск сам расширяется на все
    (в ответе это помечено). Ошибка эмбеддинг-провайдера не роняет поиск:
    он деградирует до полнотекстового.
    """
    scope, text, err = _scope(space, query)
    if err:
        return err
    hits, fell_back = await search_expanded(store, text, scope=scope, expand=expand)
    hits = hits[:limit]
    if not hits:
        return (f'По запросу «{text}» ничего не нашлось (область: {scope.label}). '
                'Попробуй другие слова, expand=true или другую область из kb_spaces.')
    return _render_hits(hits, scope, fell_back, max_chars, response_format, expand)


@mcp.tool(name='kb_answer', title='Готовый ответ бота с источниками',
          annotations=_READ_ONLY)
async def kb_answer(
    question: Annotated[str, Field(
        min_length=3, max_length=1000,
        description='Вопрос как для телеграм-бота. Область — параметром или '
                    'указателем «#slug» в начале.')],
    space: Annotated[str | None, Field(
        description='Slug области (см. kb_spaces). Пусто — по всем.')] = None,
) -> str:
    """Тот же ответ, что даёт бот на /ask: поиск с расширением запроса,
    затем генерация ответа моделью ANSWER_MODEL со списком источников
    (ссылки на сообщения). Стоит ~$0.01 за вопрос.

    Когда использовать: нужен готовый ответ в стиле бота или проверка того,
    что увидят люди в чате. Для собственного анализа выгоднее kb_search:
    дешевле и даёт сырые фрагменты целиком.
    """
    scope, text, err = _scope(space, question)
    if err:
        return err
    answer, found = await answer_question(store, text, scope=scope)
    if not found:
        answer += ('\n\n(В базе не нашлось — вопрос попадёт в /gaps бота; '
                   'можно попробовать kb_search с другими словами.)')
    return answer


@mcp.tool(name='kb_spaces', title='Области базы знаний и их наполнение',
          annotations=_READ_ONLY)
async def kb_spaces() -> str:
    """Список областей знаний (slug, название, аудитория, терминология) и
    что в каждой лежит: чаты и документы с числом фрагментов и охватом
    дат. Бесплатно и без сети — вызывай первым, чтобы выбрать space для
    kb_search/kb_answer и понять, есть ли вообще данные по теме.
    """
    counts = dict(store.count_by_space())
    overview = store.space_overview()
    lines = [f'Областей: {len(SPACES.all)}, фрагментов всего: {store.count()}', '']
    for s in SPACES.all:
        lines.append(f'## #{s.slug} — {s.label} ({counts.get(s.slug, 0)} фрагментов)')
        if s.persona:
            lines.append(f'аудитория: {s.persona}')
        if s.hints:
            lines.append(f'терминология: {s.hints}')
        rows = overview.get(s.slug, [])
        for chat_id, n, d1, d2 in rows[:12]:
            kind = 'чат' if chat_id < 0 else 'документы'
            lines.append(f'- {kind} {chat_id}: {n} фрагм., {d1} — {d2}')
        if len(rows) > 12:
            lines.append(f'- …и ещё источников: {len(rows) - 12}')
        if not rows:
            lines.append('- пусто: инжест ещё не доехал')
        lines.append('')
    return '\n'.join(lines).rstrip()


class _BearerAuth:
    """ASGI-обёртка: один общий токен в Authorization: Bearer.

    OAuth здесь избыточен: сервер живёт в домашней сети, клиент один.
    Сравнение constant-time; /health открыт — по нему docker и человек
    проверяют, что сервис жив, без токена."""

    def __init__(self, app, token: str, exempt: tuple[str, ...] = ('/health',)):
        self.app, self.exempt = app, exempt
        self.expected = f'Bearer {token}'.encode()

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http' or scope.get('path') in self.exempt:
            return await self.app(scope, receive, send)
        got = b''
        for key, value in scope.get('headers', ()):
            if key == b'authorization':
                got = value
                break
        if not hmac.compare_digest(got, self.expected):
            resp = JSONResponse({'error': 'unauthorized: нужен заголовок '
                                          'Authorization: Bearer <KB_MCP_TOKEN>'},
                                status_code=401)
            return await resp(scope, receive, send)
        await self.app(scope, receive, send)


async def _health(request):
    return JSONResponse({'status': 'ok', 'chunks': store.count(),
                         'spaces': list(SPACES.slugs)})


def build_app():
    """ASGI-приложение: /mcp (Streamable HTTP, stateless JSON) + /health.

    Защита от DNS-rebinding включается только с явным списком хостов:
    с пустым списком библиотека отвергала бы КАЖДЫЙ запрос (421), а в
    домашней сети за bearer-токеном угроза rebinding и так мала.

    В списке — адреса, по которым обращаются К НАМ (заголовок Host), а не
    адреса обращающихся; источник запроса не проверяется. Адрес не из
    списка получает 421 — включая localhost, если его туда не внесли."""
    if ALLOWED_HOSTS:
        security = TransportSecuritySettings(enable_dns_rebinding_protection=True,
                                             allowed_hosts=ALLOWED_HOSTS)
    else:
        security = TransportSecuritySettings(enable_dns_rebinding_protection=False)
    app = mcp.streamable_http_app(json_response=True, stateless_http=True,
                                  transport_security=security)
    app.add_route('/health', _health, methods=['GET'])
    return _BearerAuth(app, TOKEN) if TOKEN else app


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    if not TOKEN:
        logger.warning('KB_MCP_TOKEN не задан — MCP доступен без авторизации '
                       '(допустимо только в доверенной сети)')
    logger.info('KB MCP: %d областей (%s), чанков в базе: %d, порт %d',
                len(SPACES.all), ', '.join(SPACES.slugs), store.count(), LISTEN_PORT)
    uvicorn.run(build_app(), host='0.0.0.0', port=LISTEN_PORT, log_level='info')


if __name__ == '__main__':
    main()
