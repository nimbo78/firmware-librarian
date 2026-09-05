"""Проверка групповых команд бота на подставном событии.

Отдельный файл, а не _selftest() внутри kb_bot: тот на импорте читает
переменные окружения и открывает базу, поэтому окружение нужно задать
ДО импорта — изнутри модуля это невозможно.

Ловит класс поломок «команда молча падает»: обработчики зовут telethon
только через event, и подменив event мы прогоняем их целиком, без сети.
Именно так в прод уехал вызов _reply_temp без первого аргумента —
py_compile такое не видит, а бот переставал отвечать на /fw.

Запуск: python kb_bot_check.py
"""
from __future__ import annotations

import asyncio
import os
import tempfile

_TMP = tempfile.mkdtemp()
os.environ.update(
    TELEGRAM_API_ID='1', TELEGRAM_API_HASH='x', OPENAI_API_KEY='sk-test',
    KB_DB_PATH=os.path.join(_TMP, 'kb.sqlite'), EMBED_DIM='4',
    KB_ANSWER_CHAT_IDS='-1001111:15,-1002222', KB_CLEANUP_MINUTES='15',
    DOWNLOAD_FOLDER=_TMP)

import kb_bot as B  # noqa: E402  — только после подготовки окружения

CHAT, TOPIC = -1001111, 15


class _Msg:
    def __init__(self, chat: int, mid: int, topic: int | None):
        self.chat_id, self.id, self.is_private = chat, mid, False
        self.reply_to_msg_id = None
        if topic:
            self.reply_to = type('R', (), {
                'forum_topic': True, 'reply_to_top_id': topic,
                'reply_to_msg_id': topic})()
        else:
            self.reply_to = None


class _Sent:
    def __init__(self, chat: int, text: str):
        self.chat_id, self.id, self.raw_text = chat, 999, text
        self.is_private = False


class FakeEvent:
    """Минимальный двойник telethon-события: копит ответы вместо отправки."""

    def __init__(self, text: str, chat: int = CHAT, topic: int | None = TOPIC):
        self.chat_id, self.sender_id, self.raw_text = chat, 42, text
        self.is_private = False
        self.message = _Msg(chat, 100, topic)
        self.sent: list[str] = []

    async def reply(self, text, **kwargs):
        self.sent.append(str(text))
        return _Sent(self.chat_id, str(text))


def _selftest() -> None:
    # разбор «чат:топик»
    p = B._parse_chat_topics
    assert p('') == {}
    assert p('-100123') == {-100123: set()}
    assert p(' -1001111:15 , -1001111:22 ,-1002222 ') == {
        -1001111: {15, 22}, -1002222: set()}

    # гейт: свой топик, чужой топик, General, чат без ограничений, чужой чат
    for topic, chat, want in ((15, CHAT, True), (99, CHAT, False),
                              (None, CHAT, False), (77, -1002222, True),
                              (None, -1002222, True), (15, -1009999, False)):
        got = B._topic_allowed(FakeEvent('привет', chat, topic))
        assert got is want, (chat, topic, got, want)

    # каждая групповая команда обязана ответить (регрессия «молчит /fw»)
    for cmd, handler in (('/fw', B._handle_fw), ('/sw', B._handle_sw),
                         ('/download abc', B._handle_download)):
        ev = FakeEvent(cmd)
        asyncio.run(handler(ev, cmd))
        assert ev.sent, f'{cmd}: обработчик промолчал'

    # и полный путь через диспетчер, вместе с гейтом
    ev = FakeEvent('/help')
    asyncio.run(B.handler(ev))
    assert ev.sent and 'Хранитель знаний' in ev.sent[0], ev.sent

    # инвентарь базы: команда обязана отвечать и на пустой базе
    ev = FakeEvent('/sources')
    asyncio.run(B.handler(ev))
    assert ev.sent and 'пуст' in ev.sent[0], ev.sent

    quiet = FakeEvent('/help', CHAT, 99)
    asyncio.run(B.handler(quiet))
    assert not quiet.sent, 'в запрещённом топике бот обязан молчать'

    # служебные ответы встают в очередь уборки, личка — нет
    assert B._cleanup, 'уборка не запланирована'
    B._cleanup.clear()
    private = _Sent(CHAT, 'x')
    private.is_private = True
    B._schedule_cleanup(private)
    assert not B._cleanup, 'личку админа чистить не надо'

    B.store.close()
    print('kb_bot_check: OK')


if __name__ == '__main__':
    _selftest()
