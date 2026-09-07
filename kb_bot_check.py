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
import time

_TMP = tempfile.mkdtemp()
os.environ.update(
    TELEGRAM_API_ID='1', TELEGRAM_API_HASH='x', OPENAI_API_KEY='sk-test',
    KB_DB_PATH=os.path.join(_TMP, 'kb.sqlite'), EMBED_DIM='4',
    KB_ANSWER_CHAT_IDS='-1001111:15,-1002222', KB_CLEANUP_MINUTES='15',
    DOWNLOAD_FOLDER=_TMP)

from telethon import errors  # noqa: E402

import kb_bot as B  # noqa: E402  — только после подготовки окружения

CHAT, TOPIC = -1001111, 15


def _no_closed_topics() -> None:
    """Заполнить кэш закрытых топиков пустыми множествами: иначе гейт
    полез бы в Telegram за статусами (клиент здесь не подключён)."""
    until = time.monotonic() + 3600
    for chat in (CHAT, -1002222, -1009999):
        B._closed_topics[chat] = (until, set())


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
        self.fail: BaseException | None = None  # что Telegram вернёт на reply

    async def reply(self, text, **kwargs):
        if self.fail is not None:
            raise self.fail
        self.sent.append(str(text))
        return _Sent(self.chat_id, str(text))


def _selftest() -> None:
    # гейт собран из пространств: без spaces.toml — одно неявное из env
    assert B.SPACES.slugs == ('main',), B.SPACES.slugs
    assert B.ANSWER_TOPICS == {-1001111: {15}, -1002222: set()}, B.ANSWER_TOPICS
    # область поиска одиночной установки — всегда своё пространство
    scope, q = B.SPACES.resolve(CHAT, 'вопрос')
    assert (scope.slug, scope.multi, q) == ('main', False, 'вопрос'), scope

    # гейт: свой топик, чужой топик, General, чат без ограничений, чужой чат
    _no_closed_topics()
    for topic, chat, want in ((15, CHAT, True), (99, CHAT, False),
                              (None, CHAT, False), (77, -1002222, True),
                              (None, -1002222, True), (15, -1009999, False)):
        got = asyncio.run(B._topic_allowed(FakeEvent('привет', chat, topic)))
        assert got is want, (chat, topic, got, want)

    # закрытый топик — такой же «не наш», как чужой: молчим ДО дорогой работы
    B._remember_closed(CHAT, TOPIC)
    assert not asyncio.run(B._topic_allowed(FakeEvent('привет')))
    _no_closed_topics()
    assert asyncio.run(B._topic_allowed(FakeEvent('привет')))

    # отказ отправки в закрытый топик: без исключения наружу, с записью в кэш
    closed_ev = FakeEvent('/help')
    closed_ev.fail = errors.BadRequestError(request=None, message='TOPIC_CLOSED',
                                            code=400)
    assert asyncio.run(B._reply_temp(closed_ev, 'привет')) is None
    assert not closed_ev.sent, 'в закрытый топик писать не должны'
    assert not asyncio.run(B._topic_allowed(closed_ev)), 'кэш не запомнил отказ'
    _no_closed_topics()

    # а сбой сети — не «закрытый топик»: такие ошибки летят наружу как раньше
    broken = FakeEvent('/help')
    broken.fail = ConnectionError('сеть отвалилась')
    try:
        asyncio.run(B._reply_temp(broken, 'привет'))
        raise AssertionError('обычная ошибка отправки должна пробрасываться')
    except ConnectionError:
        pass
    assert B._is_mute_error(errors.ChatWriteForbiddenError(request=None))
    assert not B._is_mute_error(ValueError('x'))

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

    # Служебные ответы встают в очередь уборки, личка — нет.
    # Очередь в базе: она обязана пережить перезапуск контейнера, иначе
    # запланированные простыни висят в чате вечно
    assert B.store.cleanup_pending(), 'уборка не запланирована'
    B.store.drop_cleanup(B.store.due_cleanup(time.time() + 10 ** 9))
    private = _Sent(CHAT, 'x')
    private.is_private = True
    B._schedule_cleanup(private)
    assert not B.store.cleanup_pending(), 'личку админа чистить не надо'

    # просроченное за время простоя контейнера удаляется на первом тике
    B.store.schedule_cleanup(CHAT, 555, time.time() - 60)
    due = B.store.due_cleanup(time.time())
    assert (CHAT, 555) in due, due
    B.store.drop_cleanup(due)
    assert not B.store.cleanup_pending()
    # не наступивший срок ещё не выдаётся
    B.store.schedule_cleanup(CHAT, 556, time.time() + 3600)
    assert B.store.due_cleanup(time.time()) == []

    B.store.close()
    print('kb_bot_check: OK')


if __name__ == '__main__':
    _selftest()
