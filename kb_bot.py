"""Answer-бот базы знаний: /ask и @упоминание в чатах KB_ANSWER_CHAT_IDS.

Отдельный контейнер (сервис kb-bot в docker-compose.yml). Логин по бот-токену
не требует интерактива; сессия хранится на томе /app/kb, чтобы не создавать
новую при каждом рестарте.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from datetime import datetime, timedelta

from telethon import Button, TelegramClient, events

from kb_ingest import embed_texts, openai_client
from kb_store import open_store

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
SESSION = os.getenv('KB_BOT_SESSION', 'kb_bot')
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

ADMIN_HELP = (
    'Команды администратора:\n'
    '/status — база, стоимость, курсоры инжеста\n'
    '/events — последние 20 событий\n'
    '/gaps — вопросы без ответа или с 👎\n'
    '/fw <модель> — прошивки из каталога\n'
    '/review — подтвердить связки каталога (LLM-экстракция)\n'
    '/notify on|off — уведомления о событиях в личку\n'
    'Любой другой текст в личке — вопрос к базе знаний.'
)

SYSTEM_PROMPT = (
    'Ты — ассистент чата по оборудованию Huawei. Отвечай кратко и по-русски, '
    'опираясь только на приведённый контекст из истории чата. Ссылайся на '
    'фрагменты номерами в квадратных скобках, например [1]. Если ответа в '
    'контексте нет — прямо скажи об этом, не выдумывай.'
)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('telethon').setLevel(logging.WARNING)

client = TelegramClient(SESSION, API_ID, API_HASH,
                        connection_retries=-1, retry_delay=300,
                        auto_reconnect=True, request_retries=5, timeout=30)
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


async def answer_question(question: str) -> tuple[str, bool]:
    """(текст ответа, нашлось ли что-то в базе) — found=False копится в /gaps."""
    oa = openai_client()
    qvec = (await embed_texts([question], client=oa))[0]
    hits = store.search(question, qvec, top_k=TOP_K)
    if not hits:
        return 'В базе знаний пока ничего не нашлось по этому вопросу.', False
    ctx_parts = []
    links = []
    for i, h in enumerate(hits, 1):
        where = f'топик «{h.topic_name}», {h.date_from}' if h.topic_name else h.date_from
        ctx_parts.append(f'[{i}] ({where})\n{h.text}')
        link = _msg_link(h.chat_id, h.msg_first)
        if link:
            links.append(f'[{i}] {link}')
        elif h.chat_id > 0:
            # PDF-чанк: синтетический положительный chat_id (см. kb_pdf.py)
            links.append(f'[{i}] файл «{h.topic_name}», стр. {h.msg_first}')
    # temperature/max_tokens не передаём: модели класса gpt-5 их не принимают
    resp = await oa.chat.completions.create(
        model=ANSWER_MODEL,
        messages=[
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content':
                'Контекст:\n\n' + '\n\n'.join(ctx_parts) + f'\n\nВопрос: {question}'},
        ])
    answer = (resp.choices[0].message.content or '').strip()
    if links:
        answer += '\n\nИсточники:\n' + '\n'.join(links[:5])
    return answer[:4000], True  # лимит сообщения Telegram — 4096


async def _send_answer(event, question: str) -> None:
    """Общий путь ответа (группа и личка): лог Q&A + кнопки оценки."""
    logger.info('Question from %s in %s: %s',
                event.sender_id, event.chat_id, question[:100])
    try:
        answer, found = await answer_question(question)
    except Exception as e:
        logger.warning('Answer failed: %s', e)
        await event.reply('Не получилось получить ответ, попробуй позже.')
        return
    qa_id = store.log_qa(event.chat_id or 0, event.sender_id or 0,
                         question, answer, found,
                         msg_id=event.message.id)  # для авто-ответа реплаем
    buttons = [[Button.inline('👍', f'r:{qa_id}:1'.encode()),
                Button.inline('👎', f'r:{qa_id}:-1'.encode())]]
    await event.reply(answer, link_preview=False, buttons=buttons)


def _format_fw(rows: list, query: str) -> str:
    if not rows:
        return (f'Прошивок по запросу «{query}» в каталоге нет. '
                f'Каталог наполняется из имён файлов в чатах.')
    out = [f'Прошивки по запросу «{query}»:']
    current_model = None
    latest_marked = False
    for model, version, name, chat_id, msg_id, date, confidence, is_series in rows:
        if model != current_model:
            suffix = ' (вся серия — проверь совместимость!)' if is_series else ''
            out.append(f'\n{model}{suffix}:')
            current_model = model
            latest_marked = False
        mark = ''
        if version and not latest_marked:
            mark = ' — последняя'
            latest_marked = True
        if confidence != 'high':
            mark += ' · не подтверждено'
        ver = version or 'версия не распознана'
        line = f'• {ver}{mark} · {date} · {name}'
        link = _msg_link(chat_id, msg_id)
        if link:
            line += f'\n  {link}'
        out.append(line)
    return '\n'.join(out)[:4000]


async def _handle_fw(event, text: str) -> None:
    parts = text.split(maxsplit=1)
    arg = parts[1].strip() if len(parts) > 1 else ''
    if not arg:
        await event.reply('Укажи модель: /fw MA5608T (можно часть: /fw 5735)')
        return
    rows = store.find_firmware(arg)
    await event.reply(_format_fw(rows, arg), link_preview=False)


def _fmt_event(ts: str, kind: str, text: str, cost: float) -> str:
    line = f'{ts[5:16]} [{kind}] {text}'
    if cost:
        line += f' ~${cost:.2f}'
    return line


async def handle_admin(event) -> None:
    text = (event.raw_text or '').strip()
    low = text.lower()
    if low.startswith('/start') or low.startswith('/help'):
        await event.reply(ADMIN_HELP)
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
        cursors = store.state_items('last_seen_id:')
        if cursors:
            lines.append('Курсоры инжеста (chat: msg_id):')
            lines += [f'  {k.split(":", 1)[1]}: {v}' for k, v in cursors]
        await event.reply('\n'.join(lines))
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
    elif low.startswith('/fw'):
        await _handle_fw(event, text)
    elif low.startswith('/review'):
        fw_items, dev_items = store.review_items(5)
        if not fw_items and not dev_items:
            await event.reply('Нечего подтверждать.')
            return
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
    elif text.startswith('/') and not low.startswith('/ask'):
        await event.reply(ADMIN_HELP)
    else:
        question = _extract_question(text)
        if question is None:
            question = text  # в личке админа любой текст — вопрос к базе
        if not question:
            await event.reply(ADMIN_HELP)
            return
        await _send_answer(event, question)


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
            await client.send_message(chat_id, text[:4000], reply_to=msg_id,
                                      buttons=buttons, link_preview=False)
        except Exception:
            # исходное сообщение могли удалить — отвечаем без реплая, с цитатой
            text = f'По вопросу «{question[:200]}»:\n\n{answer}'
            await client.send_message(chat_id, text[:4000],
                                      buttons=buttons, link_preview=False)
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
    if text.lower().startswith('/fw'):
        await _handle_fw(event, text)  # без кулдауна: дёшево, без LLM
        return
    question = _extract_question(text)
    if question is None:
        return
    if not question:
        await event.reply('Напиши вопрос после команды: /ask как прошить ONT')
        return
    now = time.monotonic()
    if now - _last_ask.get(event.sender_id, 0.0) < COOLDOWN_SECONDS:
        await event.reply('Подожди немного перед следующим вопросом.')
        return
    _last_ask[event.sender_id] = now
    await _send_answer(event, question)


@client.on(events.CallbackQuery(pattern=rb'^c:'))
async def on_confirm(event):
    """Кнопки /review: подтверждение связок каталога. Только админы."""
    if event.sender_id not in ADMIN_IDS:
        await event.answer()
        return
    try:
        parts = event.data.decode().split(':')
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
