"""Прогон всех веток kb_backfill на подставных Telegram и OpenAI.

Отдельный файл, как и kb_bot_check: kb_backfill на импорте читает окружение,
поэтому оно задаётся ДО импорта.

Ловит класс поломок «ветка не выполнялась ни разу»: в проде бэкфилл шёл
часами, а падал на шаге, до которого доходил впервые. Так уехало затенение
переменной extra (словарь чатов для каталогизации затирался строкой лога) —
крэш случался только когда prune удалял хоть один чанк, то есть при
повторном полном прогоне. py_compile такое не видит.

Запуск: python kb_backfill_check.py
"""
from __future__ import annotations

import asyncio
import contextlib
import io
import os
import tempfile
from datetime import datetime

_TMP = tempfile.mkdtemp()
_SPACES = os.path.join(_TMP, 'spaces.toml')
with open(_SPACES, 'w', encoding='utf-8') as _f:
    _f.write('''
[huawei]
title = "Huawei"
chats = [-1001]
folder = "%s"
catalog = "huawei"
download = { chats = [-1001, -1009], extensions = ["pdf"] }

[b4]
title = "B4"
chats = [-2001]
answer = []
vision = false
voice = false
''' % _TMP.replace('\\', '/'))

os.environ.update(
    TELEGRAM_API_ID='1', TELEGRAM_API_HASH='x', OPENAI_API_KEY='sk-test',
    KB_DB_PATH=os.path.join(_TMP, 'kb.sqlite'), EMBED_DIM='4',
    KB_SPACES_FILE=_SPACES, KB_VISION='1', KB_VOICE='0')

import kb_backfill as B  # noqa: E402
import kb_firmware  # noqa: E402
import kb_ingest  # noqa: E402
import kb_pipeline  # noqa: E402
from kb_ingest import IngestStats  # noqa: E402
from kb_spaces import load_spaces  # noqa: E402


def _quiet(spaces, max_cost, force: bool = False) -> tuple[str, object]:
    """Прогон бэкфилла с перехватом вывода.

    Бэкфилл печатает прогресс и сообщение про исчерпанный бюджет — в тесте
    это сбивает с толку (выглядит как реальная проблема с балансом API),
    поэтому наружу вывод идёт только при провале проверки."""
    buf = io.StringIO()
    code = None
    with contextlib.redirect_stdout(buf):
        try:
            asyncio.run(B._backfill(FakeClient(), spaces, max_cost, force=force))
        except SystemExit as e:      # Ctrl+C выходит кодом, а не traceback'ом
            code = e.code
    return buf.getvalue(), code


class FakeMsg:
    def __init__(self, mid: int):
        self.id, self.document, self.reply_to = mid, object(), None


class FakeClient:
    """Telegram, которого нет: отдаёт по одному документу на чат."""

    TIP = 777        # id последнего сообщения: по нему помечается разобранное

    def iter_messages(self, chat_id, reverse=False, min_id=None):
        async def gen():
            yield FakeMsg(1)
        return gen()

    async def get_messages(self, chat_id, limit=1):
        return [FakeMsg(self.TIP)]


class _ScanMsg:
    """Сообщение без медиа и документов: нам важен только проход по истории."""

    def __init__(self, mid: int):
        self.id, self.sender_id = mid, 42
        self.document = self.file = self.sender = None
        self.photo = self.sticker = self.gif = self.voice = None
        self.reply_to = None
        self.raw_text = f'сообщение {mid}'
        self.date = datetime(2026, 1, 1, 12, 0)


class _ScanStore:
    def get_state(self, key, default=None): return default
    def set_state(self, key, value): pass
    def chunk_hashes(self, ids): return {}
    def upsert_chunks(self, chunks): pass
    def prune_chunks(self, chat_id, keep): return 0


class _ScanClient:
    def __init__(self, n): self.n = n

    async def get_messages(self, chat_id, limit=0):
        class Empty(list):
            total = 0
        out = Empty()
        out.total = self.n
        return out

    def iter_messages(self, chat_id, min_id=None, reverse=False):
        async def gen():
            for i in range(1, self.n + 1):
                yield _ScanMsg(i)
        return gen()


def _check_scan_progress() -> None:
    """Проход по истории обязан быть виден: раньше между «Подключился» и
    первыми эмбеддингами бэкфилл молчал минутами и выглядел зависшим."""
    seen: list[tuple] = []
    real_topics, real_embed = kb_ingest.fetch_topic_names, kb_ingest.embed_texts

    async def no_topics(client, chat_id): return {}

    async def fake_embed(texts): return [[1.0, 0.0, 0.0, 0.0] for _ in texts]

    kb_ingest.fetch_topic_names = no_topics
    kb_ingest.embed_texts = fake_embed
    try:
        asyncio.run(kb_ingest.ingest_chat(
            _ScanClient(1200), _ScanStore(), -1001, min_id=0,
            progress=lambda *a: seen.append(a), enrich_media=False))
    finally:
        kb_ingest.fetch_topic_names = real_topics
        kb_ingest.embed_texts = real_embed

    scans = [s for s in seen if s[0] == 'scan']
    assert len(scans) == 3, scans          # 500, 1000 и финальный тик
    assert scans[0][1] == 500 and scans[0][2] == 1200, scans[0]
    assert scans[-1][1] == 1200, scans[-1]  # 100% в конце прохода
    # и что это печатается человеку с процентами
    line = io.StringIO()
    with contextlib.redirect_stdout(line):
        B.Progress()('scan', 500, 1200, 0.0)
    assert 'сообщения: 500/1200 (41%)' in line.getvalue(), line.getvalue()


class _Tty(io.StringIO):
    """Поток, который считает себя терминалом."""

    def isatty(self) -> bool:
        return True


def _check_live_bar() -> None:
    """В терминале — перерисовка одной строки, в журнале — обычные строки.

    Контракт, который легко сломать: любой посторонний вывод обязан идти
    через say(), иначе он ляжет поверх наполовину нарисованной полосы."""
    tty = _Tty()
    with contextlib.redirect_stdout(tty):
        p = B.Progress()
        p('scan', 500, 1000, 0.0)
        p('scan', 1000, 1000, 0.0)
        p.say('итог этапа')
    raw = tty.getvalue()
    assert raw.count('\r') == 2, raw            # два тика — две перерисовки
    assert '█' in raw and '░' in raw, raw       # полоса нарисована
    # say обязана перевести строку ДО своего текста, иначе наедет на полосу
    assert raw.endswith('\nитог этапа\n'), repr(raw[-40:])
    assert raw.count('\n') == 2, repr(raw)      # всего две физические строки

    plain = io.StringIO()                       # не терминал: журнал прогона
    with contextlib.redirect_stdout(plain):
        p = B.Progress()
        p('scan', 500, 1000, 0.0)
        p.say('итог этапа')
    out = plain.getvalue()
    assert '\r' not in out and '█' not in out, repr(out)
    assert 'сообщения: 500/1000 (50%)' in out, out


def _selftest() -> None:
    calls: dict[str, list] = {'ingest': [], 'media': [], 'catalog': [],
                              'pipeline': []}

    async def fake_ingest(client, store, chat_id, min_id=None, progress=None,
                          max_cost=None, enrich_media=True, space='', media=None):
        calls['ingest'].append((chat_id, space, enrich_media))
        # pruned > 0 — тот самый случай, на котором прод падал: раньше строка
        # лога затирала словарь чатов для каталогизации.
        # media_seen — знаменатель прогресса этапа [2/3]
        return IngestStats(messages=10, new_chunks=3, pruned=2, cost=0.01,
                           media_seen=5)

    async def fake_media(client, store, chat_id, progress=None, max_cost=None,
                         media=None, concurrency=None, total=0):
        calls['media'].append((chat_id, media.vision, media.voice, total))
        if progress:                      # прогресс обязан пережить проценты и ETA
            progress('media', 2, total, 0.008)
        return 2, 0.008

    async def fake_pipeline(store, spaces, budget=None, progress=None,
                            report='events', say=print):
        # say — печать конвейера через объект прогресса: иначе его строки
        # лягут поверх перерисовываемой полосы
        calls['pipeline'].append((report, say))
        return 0.0, False

    def fake_record(store, msg, fname, topic_name, space=''):
        calls['catalog'].append((fname, space))

    async def fake_topics(client, chat_id):
        return {1: 'Общий'}

    B.ingest_chat = fake_ingest
    B.enrich_chat_media = fake_media
    kb_pipeline.run_post_ingest = fake_pipeline
    kb_firmware.record_file = fake_record
    kb_firmware.document_filename = lambda msg: 'S5735-L_V200R019.cc'
    kb_ingest.fetch_topic_names = fake_topics
    kb_ingest.message_topic_id = lambda msg: 1

    spaces = load_spaces()
    out, code = _quiet(spaces, None)
    try:
        assert code is None, code
        # Этапы 1 и 3 прошли по обоим чатам-источникам, с правильными областями
        assert (-1001, 'huawei', False) in calls['ingest'], calls['ingest']
        assert (-2001, 'b4', False) in calls['ingest'], calls['ingest']
        assert (-1001, 'huawei', True) in calls['ingest'], 'этап [3/3] не дошёл'
        # Этап 2: только там, где обогащение включено (у b4 vision/voice = false)
        # знаменатель посчитан на этапе [1/3] и доехал до этапа [2/3]
        assert calls['media'] == [(-1001, True, False, 5)], calls['media']
        assert 'всего медиа: 5' in out, out
        assert 'медиа: 2/5 (40%)' in out, out
        assert 'из кэша: 3' in out, out
        assert 'обогащение выключено' in out, out
        # Каталогизация чата качалки вне chats — ветка, в которой жил баг с
        # затенением extra: падала только при pruned > 0, то есть на повторном
        # полном прогоне, через час работы
        assert calls['catalog'] == [('S5735-L_V200R019.cc', 'huawei')], calls['catalog']
        assert calls['pipeline'] == [('print', B._progress.say)], calls['pipeline']
        print('  1/6 обычный прогон: этапы, каталогизация, политика медиа — OK')

        # Бюджет: BudgetExceeded на первом же чате обрывает всё, конвейер не зовём
        calls['ingest'].clear(); calls['media'].clear(); calls['pipeline'].clear()

        async def broke(*a, **kw):
            raise kb_ingest.BudgetExceeded(1.5)

        B.ingest_chat = broke
        # force: иначе этап 1 пропустится по метке от прошлого сценария и
        # сбой внутри него просто не случится
        out, code = _quiet(spaces, 1.0, force=True)
        assert code is None, 'лимит бюджета — не аварийный выход'
        assert calls['media'] == [] and calls['pipeline'] == [], calls
        assert 'ЛИМИТ БЮДЖЕТА' in out, out   # пользователю обязаны объяснить
        print('  2/6 обрыв по бюджету (подделан в тесте, денег не тратит) — OK')

        # Ctrl+C: вместо traceback — итог с обещанием продолжить и код 130
        calls['ingest'].clear(); calls['pipeline'].clear()

        async def interrupted(*a, **kw):
            raise KeyboardInterrupt

        B.ingest_chat = interrupted
        out, code = _quiet(spaces, None, force=True)
        assert code == 130, code
        assert 'ПРЕРВАНО' in out and 'без двойной оплаты' in out, out
        assert calls['pipeline'] == [], 'после Ctrl+C конвейер запускать нельзя'
        print('  3/6 Ctrl+C: итог напечатан, код возврата 130 — OK')
        _check_scan_progress()
        print('  4/6 проход по истории виден снаружи (проценты и ETA) — OK')
        _check_live_bar()
        print('  5/6 полоса перерисовывается в терминале, в журнале — строки — OK')

        # Повторный запуск не перечитывает ту же историю заново: она помечена
        # разобранной до id последнего сообщения. Раньше каждый перезапуск
        # стоил полного прохода по чату — на 125 тыс. сообщений это часы
        B.ingest_chat = fake_ingest
        calls['ingest'].clear(); calls['media'].clear()
        out, _ = _quiet(spaces, None, force=True)   # полный прогон: ставит метки
        assert calls['ingest'], 'полный прогон обязан читать историю'
        calls['ingest'].clear(); calls['media'].clear()

        async def nothing_new(client, store, chat_id, progress=None, max_cost=None,
                              media=None, concurrency=None, total=0):
            calls['media'].append((chat_id, total))
            return 0, 0.0                     # всё уже в кэше, платить нечего

        B.enrich_chat_media = nothing_new
        out, _ = _quiet(spaces, None)
        assert not calls['ingest'], f'история перечитана заново: {calls["ingest"]}'
        assert 'история уже разобрана' in out, out
        assert 'новых описаний нет' in out, out
        # знаменатель для этапа [2/3] пережил перезапуск через state
        assert calls['media'] == [(-1001, 5)], calls['media']

        calls['ingest'].clear()
        out, _ = _quiet(spaces, None, force=True)
        assert calls['ingest'], '--force обязан разобрать заново'
        assert 'история уже разобрана' not in out, out
        print('  6/6 повторный запуск не перечитывает историю, --force — да — OK')
    except AssertionError:
        print('--- вывод бэкфилла ---\n' + out)
        raise
    print('kb_backfill_check: OK')


if __name__ == '__main__':
    _selftest()
