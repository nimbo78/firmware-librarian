"""Ответы бота: гибридный поиск, расширение запроса, промпты, веб-поиск.

Вынесено из kb_bot по той же причине, что и kb_render: модуль
импортируется без переменных окружения Telegram, хранилище передаётся
параметром — значит, поведение поиска и сборки ответа можно проверять
отдельно от телеграм-обвязки.
"""
from __future__ import annotations

import json
import logging
import os
import re

from kb_ingest import embed_texts, openai_client
from kb_render import msg_link

logger = logging.getLogger('kb_bot')


ANSWER_MODEL = os.getenv('ANSWER_MODEL', 'gpt-5-mini')


KB_WEB = os.getenv('KB_WEB', '0') == '1'


TOP_K = 8


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


async def expand_query(question: str) -> list[str]:
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


async def search_expanded(store, question: str, extra: list[str] = ()) -> list:
    """Поиск по вопросу + расширенным формулировкам, слияние через RRF
    (тот же приём, что внутри store.search для вектора+FTS).
    extra — доп. варианты (вопросы из диалога: follow-up «а на R024?» сам
    по себе не несёт сущностей, их держит предыдущий вопрос)."""
    variants = [question] + list(extra) + await expand_query(question)
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


async def answer_with_web(system: str, user: str, oa) -> tuple[str, list[str]]:
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


async def answer_question(store, question: str,
                          dialog: list[tuple[str, str]] = ()) -> tuple[str, bool]:
    """(текст ответа, нашлось ли что-то в базе) — found=False копится в /gaps.
    dialog — предыдущие обмены (вопрос, ответ) при follow-up реплаем."""
    oa = openai_client()
    hits = await search_expanded(store, question, extra=[q for q, _ in dialog])
    if not hits and not KB_WEB:
        return 'В базе знаний пока ничего не нашлось по этому вопросу.', False
    ctx_parts = []
    src_lines = []  # выровнено с нумерацией контекста: src_lines[i-1] = [i]
    for i, h in enumerate(hits, 1):
        where = f'топик «{h.topic_name}», {h.date_from}' if h.topic_name else h.date_from
        ctx_parts.append(f'[{i}] ({where})\n{h.text}')
        link = msg_link(h.chat_id, h.msg_first)
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
            answer, web_urls = await answer_with_web(
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


async def fw_llm_match(store, query: str) -> tuple[list[str], list[int]]:
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
