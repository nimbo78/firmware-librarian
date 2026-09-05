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

from telethon import Button, TelegramClient, errors, events

from kb_answer import answer_question, fw_llm_match
from kb_firmware import split_query
from kb_ingest import fetch_topics, message_topic_id, openai_client
from kb_render import (ADMIN_HELP_EXTRA, fmt_event, msg_link,
                       nav_category_view, nav_files_view, nav_model_view,
                       nav_root_view, render_grouped, render_sources,
                       user_help)
from kb_store import open_store
from tg_conn import proxy_kwargs

API_ID = int(os.environ['TELEGRAM_API_ID'])
API_HASH = os.environ['TELEGRAM_API_HASH']
BOT_TOKEN = os.getenv('KB_BOT_TOKEN', '')
def _parse_chat_topics(raw: str) -> dict[int, set[int]]:
    """'-100123:15,-100123:22,-100999' -> {-100123: {15, 22}, -100999: set()}.

    Пустой набор топиков = отвечаем в чате где угодно (обратная
    совместимость со старым форматом «просто список чатов»). В форуме
    иначе бот засоряет все топики подряд — а его ждут в одном-двух."""
    out: dict[int, set[int]] = {}
    for item in raw.split(','):
        item = item.strip()
        if not item:
            continue
        chat, _, topic = item.partition(':')
        topics = out.setdefault(int(chat), set())
        if topic.strip():
            topics.add(int(topic))
    return out


ANSWER_TOPICS = _parse_chat_topics(os.getenv('KB_ANSWER_CHAT_IDS', ''))
ANSWER_CHAT_IDS = set(ANSWER_TOPICS)
# Через сколько минут убирать за собой служебные сообщения (каталожные
# простыни, «подожди», подсказки). 0 = не убирать. Ответы на вопросы не
# трогаются никогда — это знание, ради которого всё затевалось.
CLEANUP_MINUTES = int(os.getenv('KB_CLEANUP_MINUTES', '0') or 0)
# Whitelist админов: telegram user id через запятую. Только им доступны
# команды в личке и уведомления. Бот не может написать первым — админ
# должен один раз нажать Start.
ADMIN_IDS = {int(x) for x in os.getenv('KB_ADMIN_IDS', '').split(',') if x.strip()}
# KB_WEB=1: ответы могут дополняться веб-поиском (Responses API + web_search);
# контекст чата приоритетен, веб-источники — отдельным блоком 🌐
SESSION = os.getenv('KB_BOT_SESSION', 'kb_bot')
# Том загрузок качалки (read-only в compose): отсюда бот шлёт файлы по кнопке 📎
DOWNLOAD_FOLDER = os.getenv('DOWNLOAD_FOLDER', './downloads')
COOLDOWN_SECONDS = 30
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
# (когда удалять, chat_id, msg_id) — очередь уборки за собой
_cleanup: list[tuple[float, int, int]] = []

# Закрытые топики форума: {chat_id: (когда протухнет, {закрытые топики})}.
# Спрашивать статусы на каждое сообщение нельзя — лишний RPC и повод для
# флуд-лимита; топики закрывают редко, устаревание на минуты безвредно.
TOPIC_CACHE_TTL = 600
_closed_topics: dict[int, tuple[float, set[int]]] = {}


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


# Ошибки «сюда писать нельзя»: ретраить бессмысленно, трейсбек ничего не
# лечит. TOPIC_CLOSED своего класса в telethon 1.44 не имеет — прилетает
# generic BadRequestError с message='TOPIC_CLOSED', поэтому проверяем и текст.
_MUTE_ERRORS = (errors.ChatWriteForbiddenError, errors.TopicDeletedError,
                errors.ChatAdminRequiredError, errors.UserBannedInChannelError)
_MUTE_MESSAGES = ('TOPIC_CLOSED', 'TOPIC_DELETED')


def _is_mute_error(e: BaseException) -> bool:
    """Отказ вида «топик закрыт / писать запрещено», а не сбой сети."""
    if isinstance(e, _MUTE_ERRORS):
        return True
    msg = getattr(e, 'message', '') or ''
    return isinstance(e, errors.RPCError) and any(m in msg for m in _MUTE_MESSAGES)


def _remember_closed(chat_id: int, topic_id: int) -> None:
    """Отказ отправки — свежайшее знание о топике: кладём в тот же кэш,
    чтобы следующие сообщения отсекались гейтом до дорогой работы."""
    until, closed = _closed_topics.get(
        chat_id, (time.monotonic() + TOPIC_CACHE_TTL, set()))
    _closed_topics[chat_id] = (until, closed | {topic_id})


async def _closed_in(chat_id: int) -> set[int]:
    """Закрытые топики чата; ответ Telegram кэшируется на TOPIC_CACHE_TTL.

    Обычная группа (и отказ Telegram в списке топиков — боту метод могут
    и не дать) выглядит как «закрытых нет»: гейт пропускает всё, отказ
    ловится уже на отправке в _reply. Пустой ответ тоже кэшируется —
    иначе неудачный запрос повторялся бы на каждое сообщение."""
    hit = _closed_topics.get(chat_id)
    if hit is not None and hit[0] > time.monotonic():
        return hit[1]
    try:
        topics = await fetch_topics(client, chat_id)
    except Exception as e:            # сеть отвалилась — не молчим из-за этого
        logger.debug('topics fetch failed for %s: %s', chat_id, e)
        return hit[1] if hit else set()
    closed = {tid for tid, (_, is_closed) in topics.items() if is_closed}
    _closed_topics[chat_id] = (time.monotonic() + TOPIC_CACHE_TTL, closed)
    return closed


async def _topic_allowed(event) -> bool:
    """Разрешён ли ответ в этом топике форума (см. _parse_chat_topics).

    Закрытый топик приравнен к чужому: писать в него всё равно не выйдет,
    а промолчать до генерации ответа дешевле, чем после."""
    topics = ANSWER_TOPICS.get(event.chat_id)
    if topics is None:
        return False
    topic_id = message_topic_id(event.message)
    if topics and topic_id not in topics:
        return False
    return topic_id not in await _closed_in(event.chat_id)


def _schedule_cleanup(msg) -> None:
    """Пометить сообщение к удалению через CLEANUP_MINUTES. Список живёт
    в памяти: после рестарта хвост не удалится — приемлемо, как и со
    счётчиками кликов."""
    # в личке админа чистить нечего — там простыни никому не мешают
    if (CLEANUP_MINUTES > 0 and msg is not None
            and not getattr(msg, 'is_private', False)):
        _cleanup.append((time.monotonic() + CLEANUP_MINUTES * 60,
                         msg.chat_id, msg.id))


async def _reply(event, *args, **kwargs):
    """event.reply, который молчит, если в топик писать нельзя.

    Закрытый топик — штатная ситуация (модератор закрыл обсуждение, а чат
    всё ещё в KB_ANSWER_CHAT_IDS), поэтому вместо трейсбека одна строка в
    лог и None вместо сообщения. Гейт узнаёт об этом из того же кэша."""
    try:
        return await event.reply(*args, **kwargs)
    except Exception as e:
        if not _is_mute_error(e):
            raise
        topic_id = message_topic_id(event.message)
        _remember_closed(event.chat_id, topic_id)
        logger.info('cannot write to %s/%s (%s) — reply skipped',
                    event.chat_id, topic_id,
                    getattr(e, 'message', '') or type(e).__name__)
        return None


async def _reply_temp(event, *args, **kwargs):
    """Служебный ответ: сам удалится, чтобы не копиться в топике.
    Заодно убирается и команда пользователя — если бот админ в группе."""
    msg = await _reply(event, *args, **kwargs)
    if msg is None:
        return None       # в закрытый топик не написали — убирать нечего
    _schedule_cleanup(msg)
    _schedule_cleanup(event.message)
    return msg


async def cleanup_loop() -> None:
    """Удаляет отслужившие служебные сообщения. Тик раз в 30 секунд."""
    if CLEANUP_MINUTES <= 0:
        return
    while True:
        await asyncio.sleep(30)
        now = time.monotonic()
        due = [x for x in _cleanup if x[0] <= now]
        if not due or not client.is_connected():
            continue
        _cleanup[:] = [x for x in _cleanup if x[0] > now]
        for _, chat_id, msg_id in due:
            try:
                await client.delete_messages(chat_id, msg_id)
            except Exception as e:  # нет прав или сообщение уже удалено
                logger.debug('cleanup skipped %s/%s: %s', chat_id, msg_id, e)


def _extract_question(text: str) -> str | None:
    m = re.match(r'^/ask(@\w+)?\s*(.*)$', text, re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(2).strip()
    if _bot_username:
        mention = f'@{_bot_username}'
        if mention.lower() in text.lower():
            return re.sub(re.escape(mention), '', text, flags=re.IGNORECASE).strip()
    return None


async def _send_answer(event, question: str, parent_qa_id: int = 0) -> None:
    """Общий путь ответа (группа и личка): лог Q&A + кнопки оценки.
    parent_qa_id != 0 — follow-up: в промпт и поиск идёт цепочка диалога."""
    from kb_ingest import check_embed_cfg
    if check_embed_cfg(store):
        # база на другой модели эмбеддингов: поиск был бы мусорным
        await _reply(event, 'База знаний переэмбеддируется (сменилась модель '
                     'эмбеддингов) — вопросы временно недоступны. '
                     'Админ: kb_reembed.py.')
        return
    dialog = store.qa_dialog(parent_qa_id) if parent_qa_id else []
    logger.info('Question from %s in %s%s: %s',
                event.sender_id, event.chat_id,
                ' (follow-up)' if dialog else '', question[:100])
    try:
        answer, found = await answer_question(store, question, dialog=dialog)
    except Exception as e:
        logger.warning('Answer failed: %s', e)
        await _reply(event, 'Не получилось получить ответ, попробуй позже.')
        return
    qa_id = store.log_qa(event.chat_id or 0, event.sender_id or 0,
                         question, answer, found,
                         msg_id=event.message.id,  # для авто-ответа реплаем
                         parent_qa_id=parent_qa_id)
    buttons = [[Button.inline('👍', f'r:{qa_id}:1'.encode()),
                Button.inline('👎', f'r:{qa_id}:-1'.encode())]]
    sent = await _reply(event, answer, link_preview=False, buttons=buttons)
    if sent is None:
        # qa_log пишется до отправки: запись останется без answer_msg_id.
        # Само по себе безвредно, но расход в /status должен быть объясним
        logger.warning('answer for qa_id=%s generated but not delivered '
                       '(topic closed): %s', qa_id, question[:80])
        return
    # реплай на это сообщение = продолжение диалога
    store.set_qa_answer_msg(qa_id, sent.id)


# порядок и подписи секций общего рендера каталога (используются также в /sw)


# ── Навигация каталога кнопками: категория → модель → ветка R → файлы ──


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
        await _reply_temp(event, 'Укажи модель: /sw S5735-S (можно с веткой: '
                          '/sw S5735-S R024)')
        return
    model_query, version_tokens = split_query(arg)
    rows = store.find_firmware(model_query, limit=120)
    if version_tokens:
        rows = [r for r in rows
                if all(t in (r[1] or '').upper() for t in version_tokens)]
    if not rows:
        await _reply_temp(event, f'По «{arg}» в каталоге пусто. Попробуй /fw {arg} '
                          f'(там есть LLM-подбор) или /download <начало имени>.')
        return
    out, buttons, _ = render_grouped(rows, arg, model_query)
    await _reply_temp(event, out[:4000], link_preview=False, buttons=buttons or None)


DOWNLOAD_BATCH_LIMIT = 12


async def _handle_download(event, text: str) -> None:
    """Фолбэк, когда парсер бессилен: все скачанные файлы, чьё имя начинается
    с префикса, шлются последовательно — включая подписи .asc/.p7s (они нужны
    для проверки PGP) и многотомные архивы (сортировка по имени)."""
    parts = text.split(maxsplit=1)
    prefix = parts[1].strip() if len(parts) > 1 else ''
    if len(prefix) < 8:
        await _reply_temp(event, 'Дай начало имени файла (минимум 8 символов): '
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
            await _reply_temp(event, 'Файлы с таким именем есть в каталоге, но на '
                              'диске NAS их нет — качай по ссылкам из /fw.')
        else:
            await _reply_temp(event, f'Ничего не начинается с «{prefix[:60]}».')
        return
    truncated = len(on_disk) > DOWNLOAD_BATCH_LIMIT
    on_disk = on_disk[:DOWNLOAD_BATCH_LIMIT]
    note = (f' (первые {DOWNLOAD_BATCH_LIMIT}, уточни префикс для остальных)'
            if truncated else '')
    await _reply_temp(event, f'Отправляю {len(on_disk)} файл(ов){note} — большие '
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
            if _is_mute_error(e):  # топик закрыли прямо во время пачки
                _remember_closed(event.chat_id, message_topic_id(event.message))
                logger.info('download batch stopped: cannot write to %s (%s)',
                            event.chat_id, getattr(e, 'message', '') or e)
                return
            logger.warning('download batch send failed for %s: %s', name, e)
    if sent < len(on_disk):
        await _reply_temp(event, f'Отправлено {sent} из {len(on_disk)} — остальные '
                          f'не ушли, детали в логах.')


async def _handle_fw(event, text: str) -> None:
    parts = text.split(maxsplit=1)
    arg = parts[1].strip() if len(parts) > 1 else ''
    if not arg:
        nav_text, nav_buttons = nav_root_view(store)
        await _reply_temp(event, nav_text, buttons=nav_buttons or None)
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
            models, doc_ids = await fw_llm_match(store, arg)
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
        text_out, buttons, seen = render_grouped(rows, arg, model_query)
    elif extra_files:
        text_out = f'Точных связок «модель → прошивка» по «{arg}» нет.'
        buttons, seen = [], set()
    else:
        text_out, buttons, seen = render_grouped(rows, arg, model_query)
    if extra_files:
        lines = ['', '🤖 Возможно подходящие файлы (LLM по именам):']
        for doc_id, name, md5, chat_id, msg_id, date in extra_files:
            line = f'  • {name} · {date}'
            link = msg_link(chat_id, msg_id)
            if link:
                line += f'\n    {link}'
            lines.append(line)
            if md5 and doc_id not in seen and len(buttons) < 12:
                seen.add(doc_id)
                buttons.append(
                    [Button.inline(f'📎 {name[:40]}', f'g:{doc_id}'.encode())])
        text_out += '\n'.join(lines)
    # пометка LLM — в начале: хвост может обрезаться лимитом 4096
    await _reply_temp(event, (llm_note + text_out)[:4000],
                      link_preview=False, buttons=buttons or None)


async def handle_admin(event) -> None:
    text = (event.raw_text or '').strip()
    low = text.lower()
    if low.startswith('/start') or low.startswith('/help'):
        await event.reply(user_help(_bot_username) + ADMIN_HELP_EXTRA)
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
            body = '\n'.join(fmt_event(ts, kind, tx, cost)
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
    elif low.startswith('/sources'):
        await event.reply(render_sources(store.sources_report(limit=25))[:4000],
                          link_preview=False)
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
        await event.reply(user_help(_bot_username) + ADMIN_HELP_EXTRA)
    else:
        question = _extract_question(text)
        if question is None:
            question = text  # в личке админа любой текст — вопрос к базе
        if not question:
            await event.reply(user_help(_bot_username) + ADMIN_HELP_EXTRA)
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
                fmt_event(ts, kind, tx, cost) for _, ts, kind, tx, cost in rows)
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
            answer, found = await answer_question(store, question)
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
        except Exception as e:
            if _is_mute_error(e):
                # топик закрыт: пробел закрываем, иначе каждую ночь платим
                # за генерацию ответа, который некуда доставить
                store.mark_gap_closed(qa_id)
                logger.info('gap %s not delivered — cannot write to %s (%s)',
                            qa_id, chat_id, getattr(e, 'message', '') or e)
                continue
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
    try:
        await client.send_message(GAPS_CHAT_ID, text[:4000],
                                  reply_to=GAPS_TOPIC_ID or None)
    except Exception as e:
        if not _is_mute_error(e):
            raise
        # это конфиг, а не случайность: чинится правкой KB_GAPS_* в .env
        logger.info('weekly gaps post skipped — cannot write to %s/%s',
                    GAPS_CHAT_ID, GAPS_TOPIC_ID)
        store.add_event('error', f'Пост «помогите сообществу» не ушёл: топик '
                                 f'{GAPS_CHAT_ID}/{GAPS_TOPIC_ID} закрыт — '
                                 f'поправь KB_GAPS_CHAT_ID/KB_GAPS_TOPIC_ID')
        return  # вопросы не помечаем опубликованными — уйдут в следующий раз
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
    if not await _topic_allowed(event):
        return          # чужой чат, закрытый топик или топик, где бота не ждут
    text = (event.raw_text or '').strip()
    low = text.lower()
    if low.startswith(('/help', '/start')):
        await _reply_temp(event, user_help(_bot_username))
        return
    if low.startswith('/fw'):
        await _handle_fw(event, text)  # без кулдауна: дёшево, без LLM
        return
    if low.startswith('/sw'):
        await _handle_sw(event, text)
        return
    if low.startswith('/sources'):
        await _reply_temp(event, render_sources(store.sources_report())[:4000],
                          link_preview=False)
        return
    if low.startswith('/download'):
        # кулдаун: пачка до 12 больших файлов — лёгкий вектор флуда в группе
        now = time.monotonic()
        if now - _last_ask.get(event.sender_id, 0.0) < COOLDOWN_SECONDS:
            await _reply_temp(event, 'Подожди немного перед следующей пачкой файлов.')
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
        await _reply_temp(event, 'Напиши вопрос после команды: /ask как прошить ONT')
        return
    now = time.monotonic()
    if now - _last_ask.get(event.sender_id, 0.0) < COOLDOWN_SECONDS:
        await _reply_temp(event, 'Подожди немного перед следующим вопросом.')
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
            text, buttons = nav_root_view(store)
        elif kind == 'c':
            text, buttons = nav_category_view(store, int(parts[2]), int(parts[3]))
        elif kind == 'm':
            text, buttons = nav_model_view(store, parts[2])
        elif kind == 'v':
            text, buttons = nav_files_view(store, parts[2], parts[3])
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
        note = 'Не получилось отправить файл'
        if _is_mute_error(e):
            # кнопки живут в старых сообщениях: топик мог закрыться после
            # публикации каталога — говорим об этом всплывашкой, без трейсбека.
            # Топик в кэш не кладём: у CallbackQuery нет message, а тянуть его
            # ради этого — лишний RPC; гейт узнает при обновлении кэша
            logger.info('attach button: cannot write to %s (%s)',
                        event.chat_id, getattr(e, 'message', '') or e)
            note = 'Топик закрыт — файл сюда не отправить'
        else:
            logger.warning('getfile failed: %s', e)
        try:
            await event.answer(note, alert=True)
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
    cleanup_task = asyncio.create_task(cleanup_loop())
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
