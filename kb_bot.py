"""Answer-бот базы знаний: /ask и @упоминание в чатах KB_ANSWER_CHAT_IDS.

Отдельный контейнер (сервис kb-bot в docker-compose.yml). Логин по бот-токену
не требует интерактива; сессия хранится на томе /app/kb, чтобы не создавать
новую при каждом рестарте.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta

from telethon import Button, TelegramClient, events

from kb_firmware import (OS_NAMES, PRODUCT_CATEGORIES, product_category,
                         split_query, version_branch_label)
from kb_ingest import embed_texts, openai_client
from kb_store import open_store
from tg_conn import proxy_kwargs

API_ID = int(os.environ['TELEGRAM_API_ID'])
API_HASH = os.environ['TELEGRAM_API_HASH']
BOT_TOKEN = os.getenv('KB_BOT_TOKEN', '')
ANSWER_CHAT_IDS = {int(x) for x in os.getenv('KB_ANSWER_CHAT_IDS', '').split(',')
                   if x.strip()}
# Whitelist админов: telegram user id через запятую. Только им доступны
# команды в личке и уведомления. Бот не может написать первым — админ
# должен один раз нажать Start.
ADMIN_IDS = {int(x) for x in os.getenv('KB_ADMIN_IDS', '').split(',') if x.strip()}
ANSWER_MODEL = os.getenv('ANSWER_MODEL', 'gpt-5-mini')
# KB_WEB=1: ответы могут дополняться веб-поиском (Responses API + web_search);
# контекст чата приоритетен, веб-источники — отдельным блоком 🌐
KB_WEB = os.getenv('KB_WEB', '0') == '1'
SESSION = os.getenv('KB_BOT_SESSION', 'kb_bot')
# Том загрузок качалки (read-only в compose): отсюда бот шлёт файлы по кнопке 📎
DOWNLOAD_FOLDER = os.getenv('DOWNLOAD_FOLDER', './downloads')
COOLDOWN_SECONDS = 30
TOP_K = 8
NOTIFY_POLL_SECONDS = 60

# Петля «вопросы без ответа»: авто-ответ через час после ночного инжеста
# (свежие знания уже в базе) и еженедельный пост «помогите сообществу».
GAP_CHECK_HOUR = int(os.getenv('INGEST_HOUR', '5')) + 1
GAPS_CHAT_ID = int(os.getenv('KB_GAPS_CHAT_ID', '0') or 0)   # 0 = пост выключен
GAPS_TOPIC_ID = int(os.getenv('KB_GAPS_TOPIC_ID', '0') or 0)  # топик форума
GAPS_POST_WEEKDAY = 0   # понедельник
GAPS_POST_HOUR = 10

# Каталожное сообщение удаляется после N успешных отправок файлов по 📎:
# файлы уже в чате, простыня со списком больше не нужна (решение владельца)
MSG_CLICKS_TO_DELETE = 3
_msg_clicks: dict[tuple[int, int], int] = {}


def _user_help() -> str:
    mention = f'@{_bot_username}' if _bot_username else '@<имя бота>'
    return (
        '🤖 Хранитель знаний чата. Что умею:\n'
        '\n'
        '❓ Вопросы по базе знаний (история чата + документация):\n'
        f'• /ask <вопрос> — или просто упомяни меня: {mention} <вопрос>\n'
        '• Ответь реплаем на мой ответ — продолжу диалог с учётом контекста\n'
        '  (можно уточнять: «а на R024?», «подробнее про DFS»).\n'
        '• Под ответом кнопки 👍/👎 — оценки делают базу лучше.\n'
        '• Если ответа не нашлось — вопрос запоминается; как только в чате\n'
        '  появится обсуждение, я сам отвечу реплаем.\n'
        '\n'
        '📦 Каталог прошивок и документации:\n'
        '• /fw — навигация по разделам, как на support.huawei.com\n'
        '• /fw <модель> [версия] — поиск: /fw S5735-S R024, /fw 5735\n'
        '• /sw <модель> — сводка по веткам софта\n'
        '• /download <начало имени> — все файлы с этим префиксом подряд\n'
        '  (включая .asc/.p7s для проверки подписи и многотомники)\n'
        '• В выдаче: софт по веткам от новых к старым (💿 образ, 🩹 патчи),\n'
        '  документация и прочее — блоком 📖 в конце.\n'
        '• Кнопка 📎 присылает файл прямо в чат (до 2 ГБ).\n'
        '  После трёх отправок с одного списка он удаляется — не мусорим.'
    )


ADMIN_HELP_EXTRA = (
    '\n\n🔧 Команды администратора (только в личке):\n'
    '/status — база, стоимость, курсоры инжеста (+кнопка инжеста)\n'
    '/events — последние 20 событий\n'
    '/gaps — вопросы без ответа или с 👎\n'
    '/review — подтвердить связки каталога (есть «принять все»)\n'
    '/ingest — внеплановый инжест сейчас (не ждать ночи)\n'
    '/notify on|off — уведомления о событиях в личку\n'
    'Любой другой текст в личке — вопрос к базе знаний.'
)

SYSTEM_PROMPT = (
    'Ты — ассистент telegram-чата инженеров по оборудованию Huawei. '
    'Отвечай по-русски, кратко и по делу, опираясь только на приведённый '
    'контекст из истории чата и документации. Ссылайся на фрагменты '
    'номерами в квадратных скобках, например [1]. Если ответа в контексте '
    'нет — прямо скажи об этом, не выдумывай.\n\n'
    'Формат — сообщение в Telegram, а не статья: короткие абзацы или '
    'списки вместо простыни, **жирным** — ключевые выводы и номера версий, '
    '`моноширинным` — команды CLI и имена файлов. Заголовков и таблиц не '
    'делай — Telegram их не рендерит. Пара уместных эмодзи приветствуется '
    '(⚠️ грабли, ✅ рабочее решение, 🔧 команда), но без ёлки. Тон — свой '
    'инженер в чате: живой, разговорный, сленг и лёгкий вайб ок, но '
    'техническая точность всегда важнее прикола.'
)

# %(name)s подписывает источник: telethon.network.* — сетевой слой Telegram,
# kb_bot/kb_* — наши модули (иначе непонятно, чей варнинг)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('kb_bot')
logging.getLogger('telethon').setLevel(logging.WARNING)

client = TelegramClient(SESSION, API_ID, API_HASH,
                        connection_retries=-1, retry_delay=300,
                        auto_reconnect=True, request_retries=5, timeout=30,
                        **proxy_kwargs())
store = open_store()
_last_ask: dict[int, float] = {}
_bot_username = ''


def _msg_link(chat_id: int, msg_id: int) -> str | None:
    s = str(chat_id)
    if s.startswith('-100'):
        return f'https://t.me/c/{s[4:]}/{msg_id}'
    return None


def _extract_question(text: str) -> str | None:
    m = re.match(r'^/ask(@\w+)?\s*(.*)$', text, re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(2).strip()
    if _bot_username:
        mention = f'@{_bot_username}'
        if mention.lower() in text.lower():
            return re.sub(re.escape(mention), '', text, flags=re.IGNORECASE).strip()
    return None


async def _expand_query(question: str) -> list[str]:
    """Разворот сленга в термины: «зеркалка на 5735» → «port mirroring
    S5735», «SPAN настройка зеркалирования». Пара альтернативных
    формулировок ловит жаргон лучше любой смены модели эмбеддингов
    (качество приоритетнее стоимости — решение владельца)."""
    try:
        oa = openai_client()
        resp = await oa.chat.completions.create(
            model=ANSWER_MODEL,
            response_format={'type': 'json_object'},
            messages=[{'role': 'user', 'content':
                'Вопрос из чата про оборудование Huawei:\n' + question[:300] +
                '\n\nСгенерируй 2 альтернативные поисковые формулировки: '
                'разверни сленг/жаргон в официальные термины и добавь '
                'англоязычный вариант с терминологией Huawei. '
                'Верни JSON {"queries": ["...", "..."]}.'}])
        data = json.loads(resp.choices[0].message.content or '{}')
        out = [str(q).strip() for q in data.get('queries', [])
               if str(q).strip()]
        return out[:2]
    except Exception as e:
        logger.warning('query expansion failed: %s', e)
        return []


async def _search_expanded(question: str, extra: list[str] = ()) -> list:
    """Поиск по вопросу + расширенным формулировкам, слияние через RRF
    (тот же приём, что внутри store.search для вектора+FTS).
    extra — доп. варианты (вопросы из диалога: follow-up «а на R024?» сам
    по себе не несёт сущностей, их держит предыдущий вопрос)."""
    variants = [question] + list(extra) + await _expand_query(question)
    if len(variants) > 1:
        logger.info('Query expansion: %s', ' | '.join(variants[1:]))
    try:
        vectors = await embed_texts(variants)
    except Exception as e:
        # эмбеддинг-провайдер лёг — деградируем до FTS-only, а не падаем:
        # фолбэк на другую модель невозможен (вектора несравнимы)
        logger.warning('embedding failed, FTS-only search: %s', e)
        vectors = [None] * len(variants)
    scores: dict[tuple, float] = {}
    by_key: dict[tuple, object] = {}
    for variant, vec in zip(variants, vectors):
        for rank, h in enumerate(store.search(variant, vec, top_k=TOP_K)):
            key = (h.chat_id, h.msg_first)
            by_key.setdefault(key, h)
            scores[key] = scores.get(key, 0.0) + 1.0 / (60 + rank)
    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    return [by_key[k] for k, _ in ranked[:TOP_K]]


async def _answer_with_web(system: str, user: str, oa) -> tuple[str, list[str]]:
    """Ответ через Responses API с веб-поиском. Возвращает (текст, веб-URL)."""
    resp = await oa.responses.create(
        model=ANSWER_MODEL,
        tools=[{'type': 'web_search'}],
        input=[{'role': 'system', 'content': system},
               {'role': 'user', 'content': user}])
    text = (getattr(resp, 'output_text', '') or '').strip()
    if not text:
        raise RuntimeError('empty web answer')
    urls: list[str] = []
    for item in getattr(resp, 'output', None) or []:
        for part in getattr(item, 'content', None) or []:
            for ann in getattr(part, 'annotations', None) or []:
                url = getattr(ann, 'url', None)
                if url and url not in urls:
                    urls.append(url)
    return text, urls[:5]


WEB_PROMPT_EXTRA = (
    ' Тебе доступен веб-поиск: используй его, чтобы дополнить или проверить '
    'ответ (официальная документация Huawei, release notes, CVE), но опыт '
    'из контекста чата приоритетен — он отражает реальную эксплуатацию.'
)


async def answer_question(question: str,
                          dialog: list[tuple[str, str]] = ()) -> tuple[str, bool]:
    """(текст ответа, нашлось ли что-то в базе) — found=False копится в /gaps.
    dialog — предыдущие обмены (вопрос, ответ) при follow-up реплаем."""
    oa = openai_client()
    hits = await _search_expanded(question, extra=[q for q, _ in dialog])
    if not hits and not KB_WEB:
        return 'В базе знаний пока ничего не нашлось по этому вопросу.', False
    ctx_parts = []
    src_lines = []  # выровнено с нумерацией контекста: src_lines[i-1] = [i]
    for i, h in enumerate(hits, 1):
        where = f'топик «{h.topic_name}», {h.date_from}' if h.topic_name else h.date_from
        ctx_parts.append(f'[{i}] ({where})\n{h.text}')
        link = _msg_link(h.chat_id, h.msg_first)
        if link:
            src_lines.append(f'[{i}] {link}')
        elif h.chat_id > 0 and h.topic_id == 1:
            # HedEx-чанк (kb_hedex.py): topic_name = «продукт версия — раздел»
            src_lines.append(f'[{i}] документация: {h.topic_name}')
        elif h.chat_id > 0:
            # PDF-чанк: синтетический положительный chat_id (см. kb_pdf.py)
            src_lines.append(f'[{i}] файл «{h.topic_name}», стр. {h.msg_first}')
        else:
            src_lines.append(f'[{i}] обсуждение в чате, {h.date_from}')
    context = ('Контекст:\n\n' + '\n\n'.join(ctx_parts)
               if ctx_parts else 'Контекст из чата пуст.')
    dialog_block = ''
    if dialog:
        turns = [f'Вопрос: {q}\nТвой ответ: {a[:800]}' for q, a in dialog]
        dialog_block = ('Предыдущий диалог (пользователь ответил на твоё '
                        'последнее сообщение — вопрос ниже продолжает его):\n'
                        + '\n\n'.join(turns) + '\n\n')
    user_msg = f'{dialog_block}{context}\n\nВопрос: {question}'
    answer = ''
    web_urls: list[str] = []
    if KB_WEB:
        try:
            answer, web_urls = await _answer_with_web(
                SYSTEM_PROMPT + WEB_PROMPT_EXTRA, user_msg, oa)
        except Exception as e:
            logger.warning('web answer failed, fallback to plain: %s', e)
    if not answer:
        if not hits:
            return ('В базе знаний пока ничего не нашлось по этому '
                    'вопросу.', False)
        # temperature/max_tokens не передаём: модели класса gpt-5 их не принимают
        resp = await oa.chat.completions.create(
            model=ANSWER_MODEL,
            messages=[{'role': 'system', 'content': SYSTEM_PROMPT},
                      {'role': 'user', 'content': user_msg}])
        answer = (resp.choices[0].message.content or '').strip()
    # В списке источников — только те, на которые LLM сослался в тексте:
    # иначе либо висячие [7] без ссылки, либо простыня из всех top-8
    cited = {int(n) for n in re.findall(r'\[(\d+)\]', answer)
             if 1 <= int(n) <= len(src_lines)}
    shown = ([src_lines[n - 1] for n in sorted(cited)]
             if cited else src_lines[:3])
    if shown:
        answer += '\n\nИсточники:\n' + '\n'.join(shown)
    if web_urls:
        answer += '\n\n🌐 Веб:\n' + '\n'.join(web_urls)
    return answer[:4000], bool(hits)  # лимит сообщения Telegram — 4096


async def _send_answer(event, question: str, parent_qa_id: int = 0) -> None:
    """Общий путь ответа (группа и личка): лог Q&A + кнопки оценки.
    parent_qa_id != 0 — follow-up: в промпт и поиск идёт цепочка диалога."""
    from kb_ingest import check_embed_cfg
    if check_embed_cfg(store):
        # база на другой модели эмбеддингов: поиск был бы мусорным
        await event.reply('База знаний переэмбеддируется (сменилась модель '
                          'эмбеддингов) — вопросы временно недоступны. '
                          'Админ: kb_reembed.py.')
        return
    dialog = store.qa_dialog(parent_qa_id) if parent_qa_id else []
    logger.info('Question from %s in %s%s: %s',
                event.sender_id, event.chat_id,
                ' (follow-up)' if dialog else '', question[:100])
    try:
        answer, found = await answer_question(question, dialog=dialog)
    except Exception as e:
        logger.warning('Answer failed: %s', e)
        await event.reply('Не получилось получить ответ, попробуй позже.')
        return
    qa_id = store.log_qa(event.chat_id or 0, event.sender_id or 0,
                         question, answer, found,
                         msg_id=event.message.id,  # для авто-ответа реплаем
                         parent_qa_id=parent_qa_id)
    buttons = [[Button.inline('👍', f'r:{qa_id}:1'.encode()),
                Button.inline('👎', f'r:{qa_id}:-1'.encode())]]
    sent = await event.reply(answer, link_preview=False, buttons=buttons)
    # реплай на это сообщение = продолжение диалога
    store.set_qa_answer_msg(qa_id, sent.id)


# порядок и подписи секций общего рендера каталога (используются также в /sw)
_SW_KIND_ORDER = {'software': 0, 'patch': 1, 'release_notes': 2, 'doc': 3,
                  'mib': 4, 'tool': 5, '': 6}
_SW_KIND_TITLES = {'software': '💿 Образ', 'patch': '🩹 Патчи',
                   'release_notes': '📃 Release notes',
                   'doc': '📖 Документация', 'mib': '🧾 MIB',
                   'tool': '🛠 Инструменты', '': '📁 Прочее'}
_RENDER_MAX_BRANCHES = 6


_RENDER_MAX_DOCS = 8
_SW_KINDS = ('software', 'patch')  # «софт-часть» ветки; остальное — в конец


def _render_grouped(rows: list, query: str, model_query: str = '',
                    max_models: int = 3) -> tuple[str, list, set]:
    """Единый рендер каталога для /fw, /sw и листа навигации.

    Порядок (решение владельца): сначала софт по веткам от новых к старым —
    🔀 R025 (💿 образ + 🩹 патчи), затем R024 и т.д., — а вся документация/
    RN/MIB/прочее одним блоком 📖 в конце модели с пометкой ветки.
    Запрошенная модель всегда первой — иначе серия (S5700) заливает лимит
    4096 и запрошенное отрезается. Возвращает (текст, кнопки 📎, seen_doc_ids)
    — seen нужен вызывающему, чтобы дополнять кнопки без дублей.
    """
    if not rows:
        return (f'По запросу «{query}» в каталоге пусто. Каталог наполняется '
                f'из имён файлов в чатах.', [], set())
    grouped: dict = {}
    is_series: dict = {}
    for r in rows:
        branch = version_branch_label(r[1])
        grouped.setdefault(r[0], {}).setdefault(branch, []).append(r)
        is_series[r[0]] = is_series.get(r[0], False) or bool(r[7])
    qnorm = re.sub(r'[^A-Z0-9]', '', (model_query or query).upper())

    def _model_rank(m: str) -> tuple:
        return (0 if qnorm and qnorm in re.sub(r'[^A-Z0-9]', '', m) else 1, m)

    out = [f'Каталог по запросу «{query}»:']
    buttons: list = []
    seen: set[int] = set()

    def _emit(r: tuple, extra: str = '') -> None:
        unconfirmed = ' · не подтверждено' if r[6] != 'high' else ''
        line = f'  • {r[2]} · {r[5]}{extra}{unconfirmed}'
        link = _msg_link(r[3], r[4])
        if link:
            line += f'\n    {link}'
        out.append(line)
        if r[8] and r[9] not in seen and len(buttons) < 10:
            seen.add(r[9])
            buttons.append([Button.inline(f'📎 {r[2][:40]}',
                                          f'g:{r[9]}'.encode())])

    for model in sorted(grouped, key=_model_rank)[:max_models]:
        suffix = (' (вся серия — проверь совместимость!)'
                  if is_series[model] else '')
        out.append(f'\n📦 {model}{suffix}')
        branches = sorted((b for b in grouped[model] if b != 'без версии'),
                          reverse=True)
        if 'без версии' in grouped[model]:
            branches.append('без версии')
        # софт-часть по веткам; документация копится в общий хвост модели
        sw_branches: list[tuple[str, dict]] = []
        docs_pool: list[tuple[str, tuple]] = []
        doc_names: set[str] = set()
        for branch in branches:
            seen_names: set[str] = set()
            by_kind: dict = {}
            for r in grouped[model][branch]:
                if r[2] in seen_names:
                    continue  # повторные посты того же файла не дублируем
                seen_names.add(r[2])
                if r[10] in _SW_KINDS:
                    by_kind.setdefault(r[10], []).append(r)
                elif r[2] not in doc_names:
                    doc_names.add(r[2])
                    docs_pool.append((branch, r))
            if by_kind:
                sw_branches.append((branch, by_kind))
        for branch, by_kind in sw_branches[:_RENDER_MAX_BRANCHES]:
            os_name = OS_NAMES.get(branch.split(' ')[0], '')
            os_mark = f' · {os_name}' if os_name else ''
            out.append(f'🔀 {branch}{os_mark}')
            for kind in sorted(by_kind, key=lambda k: _SW_KIND_ORDER.get(k, 6)):
                out.append(f'  {_SW_KIND_TITLES.get(kind, "📁 Прочее")}:')
                for r in by_kind[kind][:5]:
                    _emit(r)
                if len(by_kind[kind]) > 5:
                    out.append(f'    … и ещё {len(by_kind[kind]) - 5}')
        if len(sw_branches) > _RENDER_MAX_BRANCHES:
            out.append(f'  … и ещё веток с софтом: '
                       f'{len(sw_branches) - _RENDER_MAX_BRANCHES} '
                       f'(уточни версией: /fw {model} R0xx)')
        if docs_pool:
            out.append('📖 Документация и прочее:')
            for branch, r in docs_pool[:_RENDER_MAX_DOCS]:
                emoji = _SW_KIND_TITLES.get(r[10], '📁 Прочее').split()[0]
                branch_mark = f' {branch}' if branch != 'без версии' else ''
                _emit(r, extra=f' · {emoji}{branch_mark}')
            if len(docs_pool) > _RENDER_MAX_DOCS:
                out.append(f'    … и ещё {len(docs_pool) - _RENDER_MAX_DOCS} '
                           f'(все — в /fw через навигацию)')
    return '\n'.join(out), buttons, seen


async def _fw_llm_match(query: str) -> tuple[list[str], list[int]]:
    """Fallback для /fw: подстрочный поиск промахнулся — просим лёгкую модель
    сматчить запрос к известным моделям И к именам файлов каталога напрямую.
    Ловит новые схемы имён, серии и вольные формулировки без правки регулярок
    (качество матчинга важнее стоимости вызова — решение владельца).
    Возвращает (модели, doc_id подходящих файлов)."""
    if not os.getenv('OPENAI_API_KEY'):
        return [], []
    known = store.all_models()
    files = store.all_files(skip_signatures=True)
    if not known and not files:
        return [], []
    file_list = '\n'.join(f'{i}: {name}' for i, (_, name) in enumerate(files))
    oa = openai_client()
    resp = await oa.chat.completions.create(
        model=ANSWER_MODEL,
        response_format={'type': 'json_object'},
        messages=[{'role': 'user', 'content':
            'Запрос пользователя (софт/прошивка для оборудования Huawei): '
            + query[:200] + '\n\n'
            'Известные модели/серии: ' + (', '.join(known) or 'нет') + '\n\n'
            'Файлы каталога (номер: имя):\n' + (file_list or 'нет') + '\n\n'
            'Верни JSON {"models": [...], "file_numbers": [...]} — какие '
            'модели и какие файлы соответствуют запросу (учитывай серии, '
            'подсемейства, сокращения, опечатки). models — только значения '
            'из списка, максимум 5; file_numbers — номера из списка файлов, '
            'максимум 10. Ничего не подходит — пустые списки.'}])
    try:
        data = json.loads(resp.choices[0].message.content or '{}')
    except Exception:
        return [], []
    known_set = set(known)
    models = [str(m) for m in data.get('models', []) if str(m) in known_set][:5]
    doc_ids: list[int] = []
    for n in data.get('file_numbers', []):
        try:
            doc_ids.append(files[int(n)][0])
        except (ValueError, IndexError, TypeError):
            continue
    return models, doc_ids[:10]


# ── Навигация каталога кнопками: категория → модель → ветка R → файлы ──
NAV_PAGE_SIZE = 14


def _nav_tree() -> dict:
    """Категория -> {модель -> {rkey(9 симв. version_key) -> (счётчик, метка)}}.
    Метка ветки берётся из сырой версии (version_branch_label), не из ключа."""
    tree: dict = {}
    for model, version, vkey in store.fw_all():
        cat = product_category(model)
        rkey = (vkey or '')[:9] or '-'
        node = tree.setdefault(cat, {}).setdefault(model, {})
        if rkey in node:
            node[rkey] = (node[rkey][0] + 1, node[rkey][1])
        else:
            node[rkey] = (1, version_branch_label(version))
    return tree


def _branch_files(branch: dict) -> int:
    """Сумма файлов по всем веткам модели (значения — (счётчик, метка))."""
    return sum(cnt for cnt, _ in branch.values())


def _nav_root_view() -> tuple[str, list]:
    tree = _nav_tree()
    if not tree:
        return 'Каталог пока пуст — файлы появятся после инжеста.', []
    buttons = []
    for ci, cat in enumerate(PRODUCT_CATEGORIES):
        models = tree.get(cat)
        if models:
            n_files = sum(_branch_files(r) for r in models.values())
            buttons.append([Button.inline(
                f'{cat} · {len(models)} моделей · {n_files} файлов',
                f'n:c:{ci}:0'.encode())])
    return ('Каталог прошивок и документации. Выбери раздел '
            '(или сразу /fw <модель>):'), buttons


def _nav_category_view(ci: int, page: int) -> tuple[str, list]:
    cat = PRODUCT_CATEGORIES[ci]
    models = sorted(_nav_tree().get(cat, {}).items())
    if not models:
        return f'{cat}: пусто.', [[Button.inline('⬅️ Разделы', b'n:r')]]
    start = page * NAV_PAGE_SIZE
    chunk = models[start:start + NAV_PAGE_SIZE]
    buttons = []
    for i in range(0, len(chunk), 2):  # по две модели в ряд
        row = [Button.inline(f'{m} ({_branch_files(r)})', f'n:m:{m}'.encode())
               for m, r in chunk[i:i + 2]]
        buttons.append(row)
    nav_row = []
    if page > 0:
        nav_row.append(Button.inline('◀️', f'n:c:{ci}:{page - 1}'.encode()))
    nav_row.append(Button.inline('⬅️ Разделы', b'n:r'))
    if start + NAV_PAGE_SIZE < len(models):
        nav_row.append(Button.inline('▶️', f'n:c:{ci}:{page + 1}'.encode()))
    buttons.append(nav_row)
    pages = (len(models) - 1) // NAV_PAGE_SIZE + 1
    return f'{cat} — модели ({page + 1}/{pages}):', buttons


def _nav_model_view(model: str) -> tuple[str, list]:
    branches = {}
    for m, rkeys in _nav_tree().get(product_category(model), {}).items():
        if m == model:
            branches = rkeys
    if not branches:
        return f'{model}: файлов нет.', [[Button.inline('⬅️ Разделы', b'n:r')]]
    ci = PRODUCT_CATEGORIES.index(product_category(model))
    buttons = []
    for rkey in sorted(branches, reverse=True):
        count, label = branches[rkey]  # метка уже человекочитаемая
        buttons.append([Button.inline(f'{label} · {count} файл(ов)',
                                      f'n:v:{model}:{rkey}'.encode())])
    buttons.append([Button.inline('⬅️ Модели', f'n:c:{ci}:0'.encode())])
    return f'{model} — ветки версий:', buttons


def _nav_files_view(model: str, rkey: str) -> tuple[str, list]:
    rows = store.find_firmware_exact(model, '' if rkey == '-' else rkey)
    if rkey == '-':
        rows = [r for r in rows if not r[1]]
    text, buttons, _ = _render_grouped(rows, model, model, max_models=1)
    buttons.append([Button.inline('⬅️ Ветки версий', f'n:m:{model}'.encode())])
    return text[:4000], buttons


async def _handle_sw(event, text: str) -> None:
    """Семантическая сводка: продукт → ветка V+R (ОС) → образ/патчи рядом.

    Схема имени Huawei (от владельца): продукт _ Vxxx(ОС) Rxxx(версия
    системного софта) Cxx(codebase) SPCxxx/SPHxxx(билд софта/патча).
    Ветка V+R и есть «версия системного софта» — группируем по ней.
    Рендер общий с /fw (_render_grouped).
    """
    parts = text.split(maxsplit=1)
    arg = parts[1].strip() if len(parts) > 1 else ''
    if not arg:
        await event.reply('Укажи модель: /sw S5735-S (можно с веткой: '
                          '/sw S5735-S R024)')
        return
    model_query, version_tokens = split_query(arg)
    rows = store.find_firmware(model_query, limit=120)
    if version_tokens:
        rows = [r for r in rows
                if all(t in (r[1] or '').upper() for t in version_tokens)]
    if not rows:
        await event.reply(f'По «{arg}» в каталоге пусто. Попробуй /fw {arg} '
                          f'(там есть LLM-подбор) или /download <начало имени>.')
        return
    out, buttons, _ = _render_grouped(rows, arg, model_query)
    await event.reply(out[:4000], link_preview=False, buttons=buttons or None)


DOWNLOAD_BATCH_LIMIT = 12


async def _handle_download(event, text: str) -> None:
    """Фолбэк, когда парсер бессилен: все скачанные файлы, чьё имя начинается
    с префикса, шлются последовательно — включая подписи .asc/.p7s (они нужны
    для проверки PGP) и многотомные архивы (сортировка по имени)."""
    parts = text.split(maxsplit=1)
    prefix = parts[1].strip() if len(parts) > 1 else ''
    if len(prefix) < 8:
        await event.reply('Дай начало имени файла (минимум 8 символов): '
                          '/download iMasterNCEServerInstall_V100R022C00SPC908')
        return
    rows = store.files_by_prefix(prefix, limit=DOWNLOAD_BATCH_LIMIT + 1)
    on_disk = []
    for doc_id, name, md5 in rows:
        path = os.path.join(DOWNLOAD_FOLDER, os.path.basename(name))
        if md5 and os.path.exists(path):
            on_disk.append((name, path))
    if not on_disk:
        if rows:
            await event.reply('Файлы с таким именем есть в каталоге, но на '
                              'диске NAS их нет — качай по ссылкам из /fw.')
        else:
            await event.reply(f'Ничего не начинается с «{prefix[:60]}».')
        return
    truncated = len(on_disk) > DOWNLOAD_BATCH_LIMIT
    on_disk = on_disk[:DOWNLOAD_BATCH_LIMIT]
    note = (f' (первые {DOWNLOAD_BATCH_LIMIT}, уточни префикс для остальных)'
            if truncated else '')
    await event.reply(f'Отправляю {len(on_disk)} файл(ов){note} — большие '
                      f'идут долго…')
    logger.info('Download batch "%s": %d files to %s (asked by %s)',
                prefix[:60], len(on_disk), event.chat_id, event.sender_id)
    sent = 0
    for name, path in on_disk:
        try:
            await client.send_file(event.chat_id, path,
                                   reply_to=event.message.id,
                                   force_document=True)
            sent += 1
        except Exception as e:
            logger.warning('download batch send failed for %s: %s', name, e)
    if sent < len(on_disk):
        await event.reply(f'Отправлено {sent} из {len(on_disk)} — остальные '
                          f'не ушли, детали в логах.')


async def _handle_fw(event, text: str) -> None:
    parts = text.split(maxsplit=1)
    arg = parts[1].strip() if len(parts) > 1 else ''
    if not arg:
        nav_text, nav_buttons = _nav_root_view()
        await event.reply(nav_text, buttons=nav_buttons or None)
        return
    # 'S5735-S-V2 R025': версия отдельным словом — фильтр, а не часть модели
    model_query, version_tokens = split_query(arg)

    def _ver_ok(version: str) -> bool:
        return all(t in (version or '').upper() for t in version_tokens)

    rows = [r for r in store.find_firmware(model_query) if _ver_ok(r[1])]
    llm_note = ''
    extra_files: list[tuple] = []
    if not rows:
        try:
            models, doc_ids = await _fw_llm_match(arg)
            seen_rows = set()
            for model in models:
                for r in store.find_firmware(model):
                    key = (r[9], r[0], r[1])  # doc_id, model, version
                    if key not in seen_rows and _ver_ok(r[1]):
                        seen_rows.add(key)
                        rows.append(r)
            fw_docs = {r[9] for r in rows}
            for doc_id in doc_ids:
                rec = store.file_by_doc_id(doc_id)
                if rec and doc_id not in fw_docs:
                    extra_files.append((doc_id,) + tuple(rec))
            if rows or extra_files:
                llm_note = (f'Подобрано LLM по запросу «{arg}» — сверь модель '
                            'в имени файла.\n\n')
        except Exception as e:
            logger.warning('fw llm fallback failed: %s', e)
    if rows:
        text_out, buttons, seen = _render_grouped(rows, arg, model_query)
    elif extra_files:
        text_out = f'Точных связок «модель → прошивка» по «{arg}» нет.'
        buttons, seen = [], set()
    else:
        text_out, buttons, seen = _render_grouped(rows, arg, model_query)
    if extra_files:
        lines = ['', '🤖 Возможно подходящие файлы (LLM по именам):']
        for doc_id, name, md5, chat_id, msg_id, date in extra_files:
            line = f'  • {name} · {date}'
            link = _msg_link(chat_id, msg_id)
            if link:
                line += f'\n    {link}'
            lines.append(line)
            if md5 and doc_id not in seen and len(buttons) < 12:
                seen.add(doc_id)
                buttons.append(
                    [Button.inline(f'📎 {name[:40]}', f'g:{doc_id}'.encode())])
        text_out += '\n'.join(lines)
    # пометка LLM — в начале: хвост может обрезаться лимитом 4096
    await event.reply((llm_note + text_out)[:4000],
                      link_preview=False, buttons=buttons or None)


def _fmt_event(ts: str, kind: str, text: str, cost: float) -> str:
    line = f'{ts[5:16]} [{kind}] {text}'
    if cost:
        line += f' ~${cost:.2f}'
    return line


async def handle_admin(event) -> None:
    text = (event.raw_text or '').strip()
    low = text.lower()
    if low.startswith('/start') or low.startswith('/help'):
        await event.reply(_user_help() + ADMIN_HELP_EXTRA)
    elif low.startswith('/status'):
        s = store.kb_stats()
        notify = 'вкл' if store.get_state('admin_notify', '1') == '1' else 'выкл'
        lines = [
            f'Чанков в базе: {s["chunks"]} (из них PDF: {s["pdf_chunks"]})',
            f'Каталог: {s["files"]} файлов, {s["fw_models"]} моделей с прошивками',
            f'Медиа обработано: {s["media_items"]} (~${s["media_cost"]:.2f})',
            f'Потрачено суммарно: ~${s["events_cost"]:.2f}',
            f'Скачано файлов за 24 ч: {s["downloads_24h"]}',
            f'Вопросов за 7 дней: {s["qa_7d"]} (👎 {s["qa_bad_7d"]}, '
            f'без ответа {s["qa_nohit_7d"]})',
            f'На подтверждение (/review): {s["pending_review"]}',
            f'Уведомления: {notify}',
        ]
        # шаги архивов/HedEx идут часами — показываем, чем занят конвейер
        running = store.get_state('pipeline_status', '')
        if running:
            lines.insert(0, f'⏳ Сейчас идёт: {running}')
        cursors = store.state_items('last_seen_id:')
        if cursors:
            lines.append('Курсоры инжеста (chat: msg_id):')
            lines += [f'  {k.split(":", 1)[1]}: {v}' for k, v in cursors]
        await event.reply('\n'.join(lines),
                          buttons=[[Button.inline('▶️ Инжест сейчас', b'a:ingest')]])
    elif low.startswith('/events'):
        rows = store.recent_events(20)
        if not rows:
            await event.reply('Событий пока нет.')
        else:
            body = '\n'.join(_fmt_event(ts, kind, tx, cost)
                             for _, ts, kind, tx, cost in reversed(rows))
            await event.reply(body[:4000], link_preview=False)
    elif low.startswith('/notify'):
        parts = text.split(maxsplit=1)
        arg = parts[1].strip().lower() if len(parts) > 1 else ''
        if arg in ('on', 'off'):
            store.set_state('admin_notify', '1' if arg == 'on' else '0')
            await event.reply(
                'Уведомления включены.' if arg == 'on' else 'Уведомления выключены.')
        else:
            cur = 'on' if store.get_state('admin_notify', '1') == '1' else 'off'
            await event.reply(f'Сейчас: {cur}. Используй /notify on или /notify off.')
    elif low.startswith('/gaps'):
        rows = store.gaps(15)
        if not rows:
            await event.reply('Вопросов без ответа нет — база справляется.')
        else:
            body = 'Вопросы без ответа или с 👎:\n' + '\n'.join(
                f'• {ts[5:16]} {q}' for ts, q in rows)
            await event.reply(body[:4000])
    elif low.startswith('/ingest'):
        store.set_state('ingest_request', '1')
        await event.reply('Запросил внеплановый инжест — качалка запустит его '
                          'в течение минуты (чаты → PDF → экстракция → бэкап). '
                          'События придут сюда по мере выполнения.')
    elif low.startswith('/fw'):
        await _handle_fw(event, text)
    elif low.startswith('/sw'):
        await _handle_sw(event, text)
    elif low.startswith('/download'):
        await _handle_download(event, text)
    elif low.startswith('/review'):
        pending = store.pending_review_count()
        if not pending:
            await event.reply('Нечего подтверждать — каталог чист.')
            return
        fw_items, dev_items = store.review_items(5)
        for rowid, model, version, fname, source in fw_items:
            btns = [[Button.inline('✅ верно', f'c:f:{rowid}:1'.encode()),
                     Button.inline('❌ нет', f'c:f:{rowid}:0'.encode())]]
            await event.reply(
                f'{fname}\n→ {model} {version or "(без версии)"} '
                f'· источник: {source}', buttons=btns)
        for model, parent in dev_items:
            btns = [[Button.inline('✅ верно', f'c:d:{model}:1'.encode()),
                     Button.inline('❌ нет', f'c:d:{model}:0'.encode())]]
            await event.reply(f'Серия: {model} принадлежит {parent}?',
                              buttons=btns)
        # массовое подтверждение остатка — без тысячи кликов
        await event.reply(
            f'Всего на подтверждении: {pending}. Проверять поштучно не '
            f'обязательно — можно принять всё разом:',
            buttons=[[Button.inline(f'✅ Принять все {pending}', b'c:allfw')]])
    elif text.startswith('/') and not low.startswith('/ask'):
        await event.reply(_user_help() + ADMIN_HELP_EXTRA)
    else:
        question = _extract_question(text)
        if question is None:
            question = text  # в личке админа любой текст — вопрос к базе
        if not question:
            await event.reply(_user_help() + ADMIN_HELP_EXTRA)
            return
        parent_qa = 0
        reply_id = event.message.reply_to_msg_id
        if reply_id:
            parent_qa = store.qa_by_answer_msg(event.chat_id, reply_id) or 0
        await _send_answer(event, question, parent_qa_id=parent_qa)


async def notifier_loop() -> None:
    """Раз в минуту рассылает админам непрочитанные события из очереди.
    События помечаются доставленными только после успешной отправки —
    при выключенных уведомлениях копятся и видны через /events."""
    if not ADMIN_IDS:
        return
    while True:
        await asyncio.sleep(NOTIFY_POLL_SECONDS)
        try:
            if store.get_state('admin_notify', '1') != '1':
                continue
            if not client.is_connected():
                continue
            rows = store.unnotified_events(20)
            if not rows:
                continue
            msg = 'События:\n' + '\n'.join(
                _fmt_event(ts, kind, tx, cost) for _, ts, kind, tx, cost in rows)
            sent = False
            for admin_id in ADMIN_IDS:
                try:
                    await client.send_message(admin_id, msg[:4000],
                                              link_preview=False)
                    sent = True
                except Exception as e:
                    # обычно: админ ещё не нажал Start у бота
                    logger.warning('notify to %s failed: %s', admin_id, e)
            if sent:
                store.mark_events_notified([r[0] for r in rows])
        except Exception as e:
            logger.warning('notifier failed: %s', e)


async def auto_answer_gaps() -> int:
    """Повторно отвечает на вопросы, где раньше не было данных: ночной инжест
    мог принести ответ из обсуждения. Ответ уходит реплаем на исходный вопрос."""
    answered = 0
    for qa_id, chat_id, msg_id, question in store.open_nohit_gaps(10):
        if chat_id not in ANSWER_CHAT_IDS and chat_id not in ADMIN_IDS:
            store.mark_gap_closed(qa_id)  # чат больше не обслуживается
            continue
        try:
            answer, found = await answer_question(question)
        except Exception as e:
            logger.warning('gap re-answer failed for %s: %s', qa_id, e)
            continue
        if not found:
            continue  # знаний всё ещё нет — оставляем пробел открытым
        new_qa = store.log_qa(chat_id, 0, question, answer, True)
        buttons = [[Button.inline('👍', f'r:{new_qa}:1'.encode()),
                    Button.inline('👎', f'r:{new_qa}:-1'.encode())]]
        text = 'Появился ответ на вопрос выше:\n\n' + answer
        try:
            sent = await client.send_message(chat_id, text[:4000],
                                             reply_to=msg_id,
                                             buttons=buttons,
                                             link_preview=False)
        except Exception:
            # исходное сообщение могли удалить — отвечаем без реплая, с цитатой
            text = f'По вопросу «{question[:200]}»:\n\n{answer}'
            sent = await client.send_message(chat_id, text[:4000],
                                             buttons=buttons,
                                             link_preview=False)
        store.set_qa_answer_msg(new_qa, sent.id)  # реплай на него = follow-up
        store.mark_gap_closed(qa_id)
        answered += 1
    if answered:
        store.add_event('gaps', f'Авто-ответы на закрытые пробелы: {answered}')
    return answered


async def post_gaps() -> None:
    """Еженедельный пост «помогите сообществу» — топ вопросов без ответа.
    Обсуждение подберёт ночной инжест, авто-ответ закроет петлю."""
    rows = store.unposted_gaps(3)
    if not rows:
        return
    lines = [f'{i}. {q}' for i, (_, q) in enumerate(rows, 1)]
    text = ('Помогите сообществу! Я не смог ответить на эти вопросы:\n\n'
            + '\n'.join(lines)
            + '\n\nОбсудите в чате — ночью я прочитаю обсуждение и отвечу '
              'авторам вопросов.')
    await client.send_message(GAPS_CHAT_ID, text[:4000],
                              reply_to=GAPS_TOPIC_ID or None)
    store.mark_gaps_posted([gid for gid, _ in rows])
    store.add_event('gaps', f'Опубликовано вопросов без ответа: {len(rows)}')


def _seconds_to_next_hour() -> float:
    now = datetime.now()
    nxt = now.replace(minute=0, second=30, microsecond=0) + timedelta(hours=1)
    return (nxt - now).total_seconds()


async def gaps_loop() -> None:
    """Тик раз в час; сделанность фиксируется в state — рестарты не дублируют."""
    while True:
        await asyncio.sleep(_seconds_to_next_hour())
        if not client.is_connected():
            continue
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')
        week = '{}-{}'.format(*now.isocalendar()[:2])
        try:
            if (now.hour == GAP_CHECK_HOUR
                    and store.get_state('gaps_check_date') != today):
                await auto_answer_gaps()
                store.set_state('gaps_check_date', today)
            if (GAPS_CHAT_ID and now.weekday() == GAPS_POST_WEEKDAY
                    and now.hour == GAPS_POST_HOUR
                    and store.get_state('gaps_post_week') != week):
                await post_gaps()
                store.set_state('gaps_post_week', week)
        except Exception as e:
            logger.warning('gaps loop failed: %s', e)


@client.on(events.NewMessage)
async def handler(event):
    if event.is_private:
        if event.sender_id in ADMIN_IDS:
            await handle_admin(event)
        return  # личка не-админов игнорируется
    if event.chat_id not in ANSWER_CHAT_IDS:
        return
    text = (event.raw_text or '').strip()
    low = text.lower()
    if low.startswith(('/help', '/start')):
        await event.reply(_user_help())
        return
    if low.startswith('/fw'):
        await _handle_fw(event, text)  # без кулдауна: дёшево, без LLM
        return
    if low.startswith('/sw'):
        await _handle_sw(event, text)
        return
    if low.startswith('/download'):
        # кулдаун: пачка до 12 больших файлов — лёгкий вектор флуда в группе
        now = time.monotonic()
        if now - _last_ask.get(event.sender_id, 0.0) < COOLDOWN_SECONDS:
            await event.reply('Подожди немного перед следующей пачкой файлов.')
            return
        _last_ask[event.sender_id] = now
        await _handle_download(event, text)
        return
    # реплай на ответ бота = продолжение диалога: /ask и упоминание не нужны,
    # цепочка предыдущих Q&A уходит в промпт и в поиск
    parent_qa = 0
    reply_id = event.message.reply_to_msg_id
    if reply_id:
        parent_qa = store.qa_by_answer_msg(event.chat_id, reply_id) or 0
    question = _extract_question(text)
    if question is None:
        if not parent_qa or not text:
            return
        question = text  # follow-up без команды
    if not question:
        await event.reply('Напиши вопрос после команды: /ask как прошить ONT')
        return
    now = time.monotonic()
    if now - _last_ask.get(event.sender_id, 0.0) < COOLDOWN_SECONDS:
        await event.reply('Подожди немного перед следующим вопросом.')
        return
    _last_ask[event.sender_id] = now
    await _send_answer(event, question, parent_qa_id=parent_qa)


@client.on(events.CallbackQuery(pattern=rb'^n:'))
async def on_nav(event):
    """Кнопки навигации каталога (/fw без аргументов)."""
    if event.chat_id not in ANSWER_CHAT_IDS and event.sender_id not in ADMIN_IDS:
        await event.answer()
        return
    try:
        parts = event.data.decode().split(':')
        kind = parts[1]
        if kind == 'r':
            text, buttons = _nav_root_view()
        elif kind == 'c':
            text, buttons = _nav_category_view(int(parts[2]), int(parts[3]))
        elif kind == 'm':
            text, buttons = _nav_model_view(parts[2])
        elif kind == 'v':
            text, buttons = _nav_files_view(parts[2], parts[3])
        else:
            await event.answer()
            return
        await event.edit(text[:4000], buttons=buttons or None,
                         link_preview=False)
    except Exception as e:
        logger.warning('nav callback failed: %s', e)
        try:
            await event.answer()
        except Exception:
            pass


@client.on(events.CallbackQuery(pattern=rb'^a:'))
async def on_admin_action(event):
    """Админ-кнопки: сейчас только «▶️ Инжест сейчас» из /status."""
    if event.sender_id not in ADMIN_IDS:
        await event.answer()
        return
    try:
        if event.data == b'a:ingest':
            store.set_state('ingest_request', '1')
            await event.answer('Инжест запрошен — качалка запустит в течение минуты')
    except Exception as e:
        logger.warning('admin action failed: %s', e)
        try:
            await event.answer()
        except Exception:
            pass


_md5_cache: dict[str, tuple[float, int, str]] = {}  # path -> (mtime, size, md5)


async def _verify_md5(path: str, expected: str) -> bool:
    """Сверка файла на диске с каталогом перед отправкой: качалка могла
    перезаписать файл новым содержимым под тем же именем (дедуп по имени),
    и каталожный md5 устарел бы. Хэш большого файла считается в thread'е
    (Celeron: ~десятки секунд на гигабайты) и кэшируется по (mtime, size)."""
    st = os.stat(path)
    cached = _md5_cache.get(path)
    if cached and cached[0] == st.st_mtime and cached[1] == st.st_size:
        return cached[2] == expected

    def _calc() -> str:
        h = hashlib.md5()
        with open(path, 'rb') as f:
            for block in iter(lambda: f.read(1 << 20), b''):
                h.update(block)
        return h.hexdigest()

    md5 = await asyncio.to_thread(_calc)
    if len(_md5_cache) > 500:
        _md5_cache.clear()
    _md5_cache[path] = (st.st_mtime, st.st_size, md5)
    return md5 == expected


@client.on(events.CallbackQuery(pattern=rb'^g:'))
async def on_getfile(event):
    """Кнопка 📎 в /fw: отправка файла с тома NAS прямо в чат.
    MTProto-боты умеют до 2 ГБ (лимит 50 МБ — только у Bot HTTP API)."""
    if event.chat_id not in ANSWER_CHAT_IDS and event.sender_id not in ADMIN_IDS:
        await event.answer()
        return
    try:
        doc_id = int(event.data.decode().split(':', 1)[1])
        rec = store.file_by_doc_id(doc_id)
        if rec is None:
            await event.answer('Файл не найден в каталоге', alert=True)
            return
        name, md5 = rec[0], rec[1]
        path = os.path.join(DOWNLOAD_FOLDER, os.path.basename(name))
        if not md5 or not os.path.exists(path):
            await event.answer('Файла нет на диске NAS — качай по ссылке на пост',
                               alert=True)
            return
        await event.answer('Отправляю файл, большие идут долго…')
        if not await _verify_md5(path, md5):
            logger.warning('md5 mismatch for %s: disk differs from catalog',
                           name)
            store.add_event('error',
                            f'md5 не совпал для «{name}» — файл на диске '
                            f'изменился после каталогизации, 📎 не отправлен')
            await event.reply(f'⚠️ «{name}» на диске не совпадает с каталогом '
                              f'— не отправляю, админ уведомлён.')
            return
        logger.info('Sending file %s to %s (asked by %s)',
                    name, event.chat_id, event.sender_id)
        await client.send_file(event.chat_id, path,
                               reply_to=event.message_id, force_document=True)
        # после N успешных отправок каталожный список удаляется из чата:
        # файлы уже присланы, простыня больше не нужна
        key = (event.chat_id, event.message_id)
        _msg_clicks[key] = _msg_clicks.get(key, 0) + 1
        if _msg_clicks[key] >= MSG_CLICKS_TO_DELETE:
            _msg_clicks.pop(key, None)
            try:
                msg = await event.get_message()
                await msg.delete()
            except Exception as e:
                logger.warning('listing cleanup failed: %s', e)
        elif len(_msg_clicks) > 500:  # не копим счётчики вечно
            _msg_clicks.clear()
    except Exception as e:
        logger.warning('getfile failed: %s', e)
        try:
            await event.answer('Не получилось отправить файл', alert=True)
        except Exception:
            pass


@client.on(events.CallbackQuery(pattern=rb'^c:'))
async def on_confirm(event):
    """Кнопки /review: подтверждение связок каталога. Только админы."""
    if event.sender_id not in ADMIN_IDS:
        await event.answer()
        return
    try:
        parts = event.data.decode().split(':')
        if parts[1] == 'allfw':
            # шаг 1: превью того, что будет подтверждено, — массовое действие
            # не должно быть слепым (когда-то так чуть не приняли 649 связок
            # старого слабого парсера не глядя)
            rows = store.medium_firmware_with_names()
            lines = [f'• {name[:60]} → {model}'
                     for _, model, name in rows[:15]]
            more = f'\n…и ещё {len(rows) - 15}' if len(rows) > 15 else ''
            await event.edit(
                f'Будут подтверждены {len(rows)} связок:\n'
                + '\n'.join(lines) + more,
                buttons=[[Button.inline(f'✅ Подтверждаю все {len(rows)}',
                                        b'c:allfw2'),
                          Button.inline('✖️ Отмена', b'c:cancel')]])
            return
        if parts[1] == 'allfw2':  # шаг 2: подтверждение после превью
            n = store.confirm_all_firmware()
            await event.edit(f'✅ Подтверждено связок разом: {n}', buttons=None)
            return
        if parts[1] == 'cancel':
            await event.edit('Отменено — связки остались на /review.',
                             buttons=None)
            return
        kind, key, ok = parts[1], parts[2], parts[3] == '1'
        if kind == 'f':
            store.confirm_firmware(int(key), ok)
        elif kind == 'd':
            store.confirm_device(key, ok)
        msg = await event.get_message()
        verdict = '✅ принято' if ok else '❌ отклонено'
        await event.edit(f'{msg.raw_text}\n{verdict}', buttons=None)
    except Exception as e:
        logger.warning('confirm callback failed: %s', e)
        try:
            await event.answer()
        except Exception:
            pass


@client.on(events.CallbackQuery(pattern=rb'^r:'))
async def on_rating(event):
    """Кнопки 👍/👎 под ответами. 👎 уходит событием админу."""
    try:
        _, qa_id_s, val_s = event.data.decode().split(':')
        rating = 1 if int(val_s) > 0 else -1
        question = store.set_qa_rating(int(qa_id_s), rating)
        if rating < 0 and question:
            store.add_event('feedback', f'👎 на ответ: {question}')
        await event.answer('Учтено, спасибо!')
    except Exception as e:
        logger.warning('rating callback failed: %s', e)
        try:
            await event.answer()
        except Exception:
            pass


async def run() -> None:
    global _bot_username
    if not BOT_TOKEN or not ANSWER_CHAT_IDS:
        # Не падаем: crash-loop с restart: unless-stopped только шумит в логах
        logger.error('KB_BOT_TOKEN / KB_ANSWER_CHAT_IDS не заданы — бот в режиме '
                     'ожидания. Заполни .env и передеплой.')
        while True:
            await asyncio.sleep(3600)
    if not os.getenv('OPENAI_API_KEY'):
        logger.warning('OPENAI_API_KEY не задан — вопросы будут падать до появления ключа')
    initial_backoff = 300
    max_backoff = 1800
    backoff = initial_backoff
    notifier_task = asyncio.create_task(notifier_loop())  # живут поверх реконнектов
    gaps_task = asyncio.create_task(gaps_loop())
    while True:
        try:
            await client.start(bot_token=BOT_TOKEN)
            me = await client.get_me()
            _bot_username = me.username or ''
            logger.info('KB bot online as @%s, чанков в базе: %d',
                        _bot_username, store.count())
            backoff = initial_backoff
            await client.run_until_disconnected()
        except (ConnectionError, OSError, asyncio.TimeoutError) as e:
            logger.warning('Top-level reconnect in %ds after: %s', backoff, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
        else:
            logger.info('KB bot disconnected cleanly')
            return


if __name__ == '__main__':
    asyncio.run(run())
