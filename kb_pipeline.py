"""Единый пост-инжест конвейер базы знаний.

Порядок шагов один на всех: каталог (link/reparse/auto-resolve/серии) ->
PDF -> архивы -> HedEx -> LLM-экстракция -> бэкап. Конвейер используют
ночной джоб качалки (librarian.py) и бэкфилл (kb_backfill.py, в т.ч.
--local-only) — раньше цепочка была задублирована в двух файлах и каждая
новая стадия добавлялась руками в оба места.

Каждый шаг изолирован: его ошибка логируется (и уходит событием админу в
режиме events) и не мешает остальным. BudgetExceeded останавливает
конвейер; всё обработанное уже зафиксировано state'ами — повторный запуск
продолжает без двойной оплаты.

Режимы отчёта:
- report='events' — ночной джоб: logger + очередь админ-событий;
- report='print'  — CLI бэкфилла: печать хода в stdout.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)


def _backup_due(store) -> bool:
    """Пора ли делать резервную копию (не чаще раза в KB_BACKUP_DAYS дней)."""
    days = int(os.getenv('KB_BACKUP_DAYS', '7') or 7)
    if days <= 0:
        return True
    last = store.get_state('backup_date', '')
    if not last:
        return True
    try:
        done = datetime.strptime(last, '%Y-%m-%d')
    except ValueError:
        return True
    return (datetime.now() - done).days >= days


async def run_post_ingest(store, download_folder: str, *,
                          budget: float | None = None,
                          progress=None,
                          report: str = 'events') -> tuple[float, bool]:
    """Возвращает (потрачено $, упёрлись ли в бюджет)."""
    from kb_ingest import BudgetExceeded

    spent = 0.0
    to_events = report == 'events'

    def remaining() -> float | None:
        return None if budget is None else max(0.0, budget - spent)

    def note(kind: str, text: str, cost: float = 0.0) -> None:
        if to_events:
            logger.info('%s: %s (~$%.2f)', kind, text, cost)
            store.add_event(kind, text, cost)
        else:
            print(f'{text}, ~${cost:.2f}')

    def status(text: str) -> None:
        """Текущее состояние конвейера -> команда /status в личке бота.
        Шаги архивов и HedEx идут часами; без этого снаружи выглядит,
        будто ничего не происходит."""
        try:
            store.set_state('pipeline_status', text)
        except Exception:
            pass

    def fail(kind: str, text: str) -> None:
        logger.warning('%s step failed: %s', kind, text)
        if to_events:
            store.add_event('error', text)
        else:
            print(f'ОШИБКА [{kind}]: {text}')

    # 1. Каталог: привязка md5 из журнала, перепарс имён после улучшения
    # регулярок, авто-снятие medium-связок, подтверждение серий
    started = datetime.now()
    status(f'каталог, начат в {started:%H:%M}')
    try:
        from kb_firmware import (auto_resolve_firmware, link_local_files,
                                 reparse_files)
        linked = link_local_files(store, download_folder)
        reparsed = reparse_files(store)
        resolved = auto_resolve_firmware(store)
        series = store.confirm_all_series()
        if linked or reparsed or resolved or series:
            note('catalog',
                 f'Каталог: привязано {linked}, связок +{reparsed}, '
                 f'авто-снято medium {resolved}, серий {series}')
    except Exception as e:
        fail('catalog', f'Обслуживание каталога упало: {e}')

    # 2. PDF -> 3. архивы -> 4. HedEx. Порядок важен: архивы извлекают
    # .hdx в hedex_extracted/, шаг HedEx подхватывает их этим же прогоном
    async def _pdf():
        from kb_pdf import ingest_pdfs
        n, c, cost = await ingest_pdfs(store, download_folder,
                                       progress=progress,
                                       max_cost=remaining())
        return n, cost, f'PDF-инжест: {n} файлов -> {c} чанков'

    async def _archives():
        from kb_archive import process_archives
        n, c, cost = await process_archives(store, download_folder,
                                            progress=progress,
                                            max_cost=remaining())
        return n, cost, f'Архивы: {n} просмотрено -> {c} чанков'

    async def _hedex():
        from kb_hedex import ingest_hdx
        n, c, cost = await ingest_hdx(store, download_folder,
                                      progress=progress,
                                      max_cost=remaining())
        return n, cost, f'HedEx-инжест: {n} пакетов -> {c} чанков'

    from kb_archive import archive_enabled
    from kb_hedex import hedex_enabled
    from kb_ingest import pdf_enabled
    for enabled, kind, step in ((pdf_enabled(), 'pdf', _pdf),
                                (archive_enabled(), 'archive', _archives),
                                (hedex_enabled(), 'hedex', _hedex)):
        if not enabled:
            continue
        status(f'{kind}, начат в {datetime.now():%H:%M}')
        try:
            n, cost, text = await step()
            spent += cost
            if n or not to_events:  # ночью тишина = «нового нет», в CLI — отчёт
                note(kind, text, cost)
        except BudgetExceeded as e:
            spent += e.cost
            status('')
            return spent, True
        except Exception as e:
            fail(kind, f'Шаг {kind} упал: {e}')

    # 5. LLM-экстракция подписей + серии; очередь /review
    status(f'LLM-экстракция, начата в {datetime.now():%H:%M}')
    try:
        from kb_extract import run_extraction
        fw, dev, cost = await run_extraction(store)
        spent += cost
        if fw or dev or not to_events:
            note('extract',
                 f'LLM-экстракция: {fw} связок прошивок, {dev} серий', cost)
        pending = store.pending_review_count()
        if pending:
            if to_events:
                store.add_event(
                    'review',
                    f'{pending} записей каталога ждут подтверждения — /review')
            else:
                print(f'На подтверждение (/review в личке бота): {pending}')
    except Exception as e:
        fail('extract', f'LLM-экстракция упала: {e}')

    # 6. Бэкап: VACUUM INTO (горячее копирование файла с WAL небезопасно).
    # Копия весит как сама база (185 тыс. чанков — это 2.1 ГБ), поэтому не
    # каждую ночь: KB_BACKUP_DAYS, по умолчанию раз в неделю. Потеря базы
    # некритична — она пересобирается из Telegram и файлов на томе.
    if _backup_due(store):
        status(f'бэкап, начат в {datetime.now():%H:%M}')
        try:
            store.backup()
            store.set_state('backup_date', datetime.now().strftime('%Y-%m-%d'))
        except Exception as e:
            fail('backup', f'Бэкап базы знаний упал: {e}')
    mins = (datetime.now() - started).total_seconds() / 60
    logger.info('пост-инжест конвейер завершён за %.0f мин, ~$%.2f', mins, spent)
    status('')
    return spent, False
