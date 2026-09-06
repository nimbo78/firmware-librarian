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
import os
import tempfile

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


class FakeMsg:
    def __init__(self, mid: int):
        self.id, self.document, self.reply_to = mid, object(), None


class FakeClient:
    """Telegram, которого нет: отдаёт по одному документу на чат."""

    def iter_messages(self, chat_id, reverse=False, min_id=None):
        async def gen():
            yield FakeMsg(1)
        return gen()


def _selftest() -> None:
    calls: dict[str, list] = {'ingest': [], 'media': [], 'catalog': [],
                              'pipeline': []}

    async def fake_ingest(client, store, chat_id, min_id=None, progress=None,
                          max_cost=None, enrich_media=True, space='', media=None):
        calls['ingest'].append((chat_id, space, enrich_media))
        # pruned > 0 — тот самый случай, на котором прод падал: раньше строка
        # лога затирала словарь чатов для каталогизации
        return IngestStats(messages=10, new_chunks=3, pruned=2, cost=0.01)

    async def fake_media(client, store, chat_id, progress=None, max_cost=None,
                         media=None, concurrency=None):
        calls['media'].append((chat_id, media.vision, media.voice))
        return 2, 0.008

    async def fake_pipeline(store, spaces, budget=None, progress=None,
                            report='events'):
        calls['pipeline'].append(report)
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
    asyncio.run(B._backfill(FakeClient(), spaces, None))

    # Этапы 1 и 3 прошли по обоим чатам-источникам, с правильными областями
    assert (-1001, 'huawei', False) in calls['ingest'], calls['ingest']
    assert (-2001, 'b4', False) in calls['ingest'], calls['ingest']
    assert (-1001, 'huawei', True) in calls['ingest'], 'этап [3/3] не дошёл'
    # Этап 2: только там, где обогащение включено (у b4 vision/voice = false)
    assert calls['media'] == [(-1001, True, False)], calls['media']
    # Каталогизация чата качалки, не заявленного в chats — ветка, где жил баг
    assert calls['catalog'] == [('S5735-L_V200R019.cc', 'huawei')], calls['catalog']
    assert calls['pipeline'] == ['print'], calls['pipeline']

    # Бюджет: BudgetExceeded на первом же чате обрывает всё, конвейер не зовём
    calls['ingest'].clear(); calls['media'].clear(); calls['pipeline'].clear()

    async def broke(*a, **kw):
        raise kb_ingest.BudgetExceeded(1.5)

    B.ingest_chat = broke
    asyncio.run(B._backfill(FakeClient(), spaces, 1.0))
    assert calls['media'] == [] and calls['pipeline'] == [], calls
    print('kb_backfill_check: OK')


if __name__ == '__main__':
    _selftest()
