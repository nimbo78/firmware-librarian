"""Опциональный MTProto-прокси для всех Telethon-клиентов проекта.

Включается тремя переменными в .env (пусто = прямое подключение):
    MTPROXY_HOST=proxy.example.com
    MTPROXY_PORT=443
    MTPROXY_SECRET=<hex или dd+hex>

ВАЖНО про секреты: Telethon поддерживает обычный hex-секрет (32 hex-символа)
и dd-вариант (randomized padding, рекомендуется). FakeTLS-секреты (ee...,
с доменом в хвосте) Telethon НЕ умеет — если прокси (telemt/mtg) выдаёт
ссылку с ee-секретом, возьми из его конфига базовый hex-секрет.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def proxy_kwargs() -> dict:
    """kwargs для TelegramClient(...): connection+proxy или пусто (напрямую)."""
    host = os.getenv('MTPROXY_HOST', '').strip()
    if not host:
        return {}
    secret = os.getenv('MTPROXY_SECRET', '').strip()
    if not secret:
        logger.warning('MTPROXY_HOST задан без MTPROXY_SECRET — прокси игнорируется')
        return {}
    if secret.lower().startswith('ee'):
        logger.warning('MTPROXY_SECRET похож на FakeTLS (ee...) — Telethon такое '
                       'не поддерживает, нужен hex- или dd-секрет. Прокси игнорируется')
        return {}
    port = int(os.getenv('MTPROXY_PORT', '443'))
    from telethon import connection
    logger.info('Telegram через MTProxy %s:%d', host, port)
    return {
        'connection': connection.ConnectionTcpMTProxyRandomizedIntermediate,
        'proxy': (host, port, secret),
    }


def _selftest() -> None:
    os.environ.pop('MTPROXY_HOST', None)
    assert proxy_kwargs() == {}
    os.environ['MTPROXY_HOST'] = 'x'
    os.environ.pop('MTPROXY_SECRET', None)
    assert proxy_kwargs() == {}  # без секрета — игнор
    os.environ['MTPROXY_SECRET'] = 'ee1234'
    assert proxy_kwargs() == {}  # faketls — игнор с warning
    os.environ.pop('MTPROXY_HOST', None)
    os.environ.pop('MTPROXY_SECRET', None)
    print('tg_conn selftest: OK')


if __name__ == '__main__':
    _selftest()
