"""Одноразовый бэкфилл истории чатов KB_CHAT_IDS в базу знаний.

ВАЖНО: скрипт использует ту же сессию bot.session, что и качалка.
Одновременная работа двух процессов с одной сессией роняет соединение
(AuthKeyDuplicatedError). Запускать ТОЛЬКО при остановленной качалке:

    docker compose stop librarian
    docker compose run --rm librarian python kb_backfill.py --dry-run
    docker compose run --rm librarian python kb_backfill.py [--max-cost 10]
    docker compose start librarian

Порядок ввода в строй: сначала полный бэкфилл, потом включать ночной ingest —
иначе границы суточных чанков не совпадут с полными и появятся почти-дубли.

Боевой прогон идёт тремя этапами: [1/3] текст (быстро и дёшево — база отвечает
уже после него; медиа в чанках плейсхолдерами), [2/3] vision/whisper в кэш,
[3/3] пересборка чанков с описаниями медиа (переэмбеддятся только изменённые).
Полный прогон также удаляет чанки с устаревшими границами (prune).

Бюджет: --dry-run печатает разбивку стоимости по категориям (текст, картинки,
голосовые, PDF) с учётом включённых флагов KB_VISION/KB_VOICE/KB_PDF.
--max-cost N останавливает боевой прогон при достижении N$; всё обработанное
кэшируется, повторный запуск после пополнения продолжит без двойной оплаты.

Доведённые до конца этапы помечаются в state вместе с id последнего сообщения
чата, поэтому повторный запуск не перечитывает ту же историю заново (на сотне
тысяч сообщений это часы). Появились новые сообщения — id другой, этап идёт.
--force игнорирует пометки: нужен, если сообщения удаляли (id последнего не
меняется, а содержимое истории — да).

Пространства знаний (kb_spaces): по умолчанию обрабатываются все, --space b4
сужает прогон до одной области — так новое пространство заводится, не трогая
уже проинжещенные.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import shutil
import sys
import time

from telethon import TelegramClient

from kb_ingest import (BudgetExceeded, EMBED_PRICE_PER_MTOK, VISION_COST_PER_IMAGE,
                       WHISPER_PRICE_PER_MIN, enrich_chat_media, ingest_chat,
                       media_policy, pdf_enabled, scan_chat)
from kb_spaces import Spaces, load_spaces
from kb_store import open_store
from tg_conn import proxy_kwargs


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise SystemExit(f'{name} must be provided')
    return val


def _human_time(seconds: float) -> str:
    if seconds < 90:
        return f'{int(seconds)} с'
    if seconds < 90 * 60:
        return f'{int(seconds / 60)} мин'
    h, m = divmod(int(seconds / 60), 60)
    return f'{h} ч {m:02d} мин'


class Progress:
    """Прогресс бэкфилла: полоса, проценты и оценка остатка.

    Бэкфилл идёт часами, и «обработано 220» без знаменателя не отвечает на
    единственный интересный вопрос — сколько ещё ждать. Скорость считается
    по факту с начала этапа, а не по эталонным цифрам: она сильно зависит
    от чата (длина сообщений, доля картинок) и времени суток у провайдера.

    В терминале строка перерисовывается на месте (\\r), в остальных случаях
    печатается по строке на обновление. Различие принципиальное, а не
    косметическое: этот же вывод — журнал многочасового прогона в
    `docker logs` и в перенаправленном в файл выводе, где возврат каретки
    превращается в кашу. Поток берётся в момент печати, а не в конструкторе:
    иначе перехват вывода (selftest, tee) достался бы мимо.

    Этапы разделяются сами: счётчик, который перестал расти, означает новый
    чат или новый этап — время старта сбрасывается, чтобы ETA не врал, а
    предыдущая полоса закрывается переводом строки и остаётся в истории.
    """

    LABELS = {'scan': 'сообщения', 'media': 'медиа', 'embed': 'эмбеддинги',
              'pdf': 'PDF', 'hedex': 'HedEx', 'archive': 'архивы'}
    # этапы с известным объёмом работы: только у них есть полоса, % и ETA
    MEASURED = ('scan', 'media', 'embed')
    BAR = 24

    def __init__(self):
        self._start: dict[str, tuple[float, int]] = {}
        self._open = False        # есть ли незакрытая перерисовываемая строка

    @staticmethod
    def _live() -> bool:
        out = sys.stdout
        try:
            return bool(out.isatty())
        except Exception:
            return False

    def close(self) -> None:
        """Закрыть живую строку, чтобы следующий вывод не наехал на неё."""
        if self._open:
            print(flush=True)
            self._open = False

    def say(self, text: str = '') -> None:
        """Обычная строка вывода. Через неё обязан идти ВЕСЬ вывод прогона —
        иначе обычный print затрёт наполовину нарисованную полосу."""
        self.close()
        print(text, flush=True)

    def _bar(self, frac: float) -> str:
        filled = int(round(frac * self.BAR))
        return '█' * filled + '░' * (self.BAR - filled)

    def __call__(self, stage: str, done: int, total: int, cost: float) -> None:
        t0, base = self._start.get(stage, (0.0, 0))
        if not t0 or done <= base:
            # base = done, а не 0: работа до сброса сделана за неизвестное
            # время, и включать её в скорость — врать в оптимистичную сторону
            t0, base = time.monotonic(), done
            self._start[stage] = (t0, base)
            self.close()          # прошлый этап остаётся отдельной строкой
        label = self.LABELS.get(stage, stage)
        live = self._live()
        if stage in self.MEASURED and total > 0:
            frac = min(done / total, 1.0)
            # полоса — только в терминале: в журнале прогона она лишний шум
            line = (f'  {label} {self._bar(frac)} {int(frac * 100):3d}% {done}/{total}'
                    if live else
                    f'  {label}: {done}/{total} ({int(frac * 100)}%)')
        else:
            line = f'  {label}: {done}'
            if stage in ('hedex', 'archive') and total:
                line += f', чанков {total}'
            elif stage == 'pdf':
                line += ' файлов'
        if cost:
            line += f', ~${cost:.2f}'
        elapsed = time.monotonic() - t0
        speed = (done - base) / elapsed if elapsed > 1 else 0.0
        if speed > 0 and total > done and stage in self.MEASURED:
            line += f', осталось ~{_human_time((total - done) / speed)}'
        elif speed > 0:
            line += f', {speed * 60:.0f}/мин'
        if live:
            width = max(40, shutil.get_terminal_size((100, 24)).columns - 1)
            # обрезаем и добиваем пробелами: короткая строка не должна
            # оставлять хвост предыдущей, длинная — переноситься и плодить
            # строки вместо перерисовки
            print('\r' + line[:width].ljust(width), end='', flush=True)
            self._open = True
        else:
            print(line, flush=True)


# Один экземпляр на прогон: этапы различаются по ключу stage
_progress = Progress()


async def _dry_run(client, spaces) -> None:
    total = 0.0
    for space in spaces.all:
        media = media_policy(space.vision, space.voice)
        for chat_id in space.chats:
            _progress.say(f'Сканирую {chat_id} — вся история, на большом чате это '
                  f'минуты/десятки минут...')
            st = await scan_chat(client, chat_id, progress=lambda n: _progress.say(
                f'  просмотрено сообщений: {n}'))
            embed = st.chars / 3 / 1e6 * EMBED_PRICE_PER_MTOK
            vision = st.images * VISION_COST_PER_IMAGE if media.vision else 0.0
            voice = (st.voice_seconds / 60 * WHISPER_PRICE_PER_MIN
                     if media.voice else 0.0)
            total += embed + vision + voice
            _progress.say(f'{chat_id} (область {space.slug}):')
            _progress.say(f'  сообщений: {st.messages}, ~{st.chars // 3} токенов '
                  f'-> эмбеддинги ~${embed:.2f}')
            mark = '' if media.vision else ' (vision выключен — не считается)'
            _progress.say(f'  картинок: {st.images} -> vision ~${vision:.2f}{mark}')
            mark = '' if media.voice else ' (whisper выключен — не считается)'
            _progress.say(f'  голосовых: {st.voice_seconds // 60} мин '
                  f'-> whisper ~${voice:.2f}{mark}')
    from kb_archive import archive_enabled
    from kb_hedex import hedex_enabled
    if not pdf_enabled():
        _progress.say('PDF: KB_PDF выключен — не считается')
    if not archive_enabled():
        _progress.say('Архивы: KB_ARCHIVE выключен — не считается')
    if not hedex_enabled():
        _progress.say('HedEx: KB_HEDEX выключен — не считается')
    store = open_store()
    for space in spaces.all:
        if not space.folder:
            continue
        if pdf_enabled():
            from kb_pdf import scan_pdfs
            files, pages = scan_pdfs(space.folder)
            # ~1800 символов на страницу мануала — грубая оценка
            pdf_cost = pages * 1800 / 3 / 1e6 * EMBED_PRICE_PER_MTOK
            total += pdf_cost
            _progress.say(f'PDF в {space.folder}: {files} файлов, {pages} страниц '
                  f'-> эмбеддинги ~${pdf_cost:.2f}')
        if archive_enabled():
            from kb_archive import scan_archives
            files, chars = scan_archives(store, space.folder, space=space.slug)
            arc_cost = chars / 3 / 1e6 * EMBED_PRICE_PER_MTOK
            total += arc_cost
            _progress.say(f'Архивы в {space.folder}: новых {files} '
                  f'-> эмбеддинги ~${arc_cost:.2f} (грубая оценка по листингам)')
        if hedex_enabled():
            from kb_hedex import scan_hdx
            files, chars = scan_hdx(store, space.folder, space=space.slug)
            hedex_cost = chars / 3 / 1e6 * EMBED_PRICE_PER_MTOK
            total += hedex_cost
            _progress.say(f'HedEx в {space.folder}: новых пакетов {files} '
                  f'-> эмбеддинги ~${hedex_cost:.2f} (без кросс-версионного '
                  f'дедупа — реально будет меньше)')
    _progress.say(f'\nИтого оценка: ~${total:.2f}')
    _progress.say('Подсказка: --max-cost N остановит боевой прогон при достижении N$.')


def _finish(store, spent: float, outcome: str) -> None:
    """Итог прогона — один на все исходы: конец, лимит бюджета, Ctrl+C.

    Прерывание безопасно ровно по той же причине, что и лимит: чанки пишутся
    пачками в транзакциях, media_cache — по одному элементу, а last_seen_id
    двигается только после успешной обработки чата. Поэтому обещание везде
    одно: повторный запуск продолжит без двойной оплаты."""
    _progress.say(f'\nЧанков в базе: {store.count()}. '
          f'Потрачено в этом прогоне: ~${spent:.2f}')
    if outcome == 'budget':
        _progress.say('ЛИМИТ БЮДЖЕТА ДОСТИГНУТ. Обработанное закэшировано — пополни '
              'баланс API и запусти повторно, прогон продолжит с места '
              'остановки без двойной оплаты.')
        store.add_event('backfill', f'Бэкфилл ОСТАНОВЛЕН по лимиту бюджета, '
                                    f'чанков: {store.count()}', spent)
    elif outcome == 'interrupted':
        _progress.say('ПРЕРВАНО (Ctrl+C). Обработанное сохранено — повторный запуск '
              'продолжит с места остановки без двойной оплаты.')
        store.add_event('backfill', f'Бэкфилл прерван вручную, '
                                    f'чанков: {store.count()}', spent)
    else:
        store.add_event('backfill',
                        f'Бэкфилл завершён, чанков в базе: {store.count()}', spent)


async def _local_only(spaces, max_cost: float | None) -> None:
    """--local-only: без подключения к Telegram (сессию не трогает, качалку
    можно не гасить) — весь пост-инжест конвейер kb_pipeline. Нужен только
    ключ эмбеддингов/OpenAI. Гонки с ночным джобом безопасны (state/md5),
    но осмысленнее не пересекаться по времени."""
    from kb_pipeline import run_post_ingest
    store = open_store()
    try:
        spent, stopped = await run_post_ingest(store, spaces, budget=max_cost,
                                               progress=_progress, report='print',
                                               say=_progress.say)
    except KeyboardInterrupt:
        # шаги конвейера идемпотентны и метят сделанное в state, так что
        # прерывание стоит только незавершённого шага
        _finish(store, 0.0, 'interrupted')
        raise SystemExit(130)
    _finish(store, spent, 'budget' if stopped else 'done')


async def _chat_tip(client, chat_id: int) -> int:
    """Id последнего сообщения чата — один RPC. 0 = узнать не удалось."""
    try:
        msgs = await client.get_messages(chat_id, limit=1)
        return msgs[0].id if msgs else 0
    except Exception as e:
        logging.getLogger(__name__).debug('tip unavailable for %s: %s', chat_id, e)
        return 0


async def _backfill(client, spaces, max_cost: float | None,
                    force: bool = False) -> None:
    """Полный разбор истории. Этапы, доведённые до конца, помечаются в state
    вместе с id последнего сообщения чата — повторный запуск не перечитывает
    ту же историю заново.

    Пропуск безопасен именно потому, что этап 1 полный: при совпадении id
    последнего сообщения он дал бы те же чанки с теми же id и хэшами, то есть
    ноль записей в базу. Появились новые сообщения — id другой, этап идёт.
    --force игнорирует пометки (нужен, если сообщения удаляли: id последнего
    не меняется, а содержимое истории — да)."""
    store = open_store()
    # чат -> пространство; чаты качалки вне chats каталогизируем без инжеста
    # в RAG — иначе их файлы невидимы для /fw и кнопок 📎
    chat_space = {c: s for s in spaces.all for c in s.chats}
    chat_ids = list(chat_space)
    # только каталогизируем (без RAG): чаты качалки, не заявленные в chats
    catalog_only = {c: s for s in spaces.all for c in s.download_chats
                    if c not in chat_space}
    spent = 0.0
    stopped = False
    media_total: dict[int, int] = {}   # чат -> сколько в нём медиа (для ETA)
    tips: dict[int, int] = {}          # чат -> id последнего сообщения
    paid_media: dict[int, int] = {}    # чат -> оплачено медиа в ЭТОМ прогоне
    # политика обогащения — своя у каждой области (см. MediaPolicy)
    chat_media = {c: media_policy(s.vision, s.voice)
                  for s in spaces.all for c in s.chats}
    media_wanted = any(m.vision or m.voice for m in chat_media.values())

    def remaining() -> float | None:
        return None if max_cost is None else max(max_cost - spent, 0.0)

    try:
        # Этап 1: только текст (быстро и дёшево) — база отвечает уже после него.
        # Медиа входит в чанки плейсхолдерами, поэтому границы окончательные.
        for n, chat_id in enumerate(chat_ids, 1):
            _progress.say(f'[1/3] Текст: {chat_id} (чат {n} из {len(chat_ids)})...')
            tip = await _chat_tip(client, chat_id)
            tips[chat_id] = tip
            # докуда история уже разобрана полностью (0 = ни разу)
            done_upto = 0 if force else int(
                store.get_state(f'backfill_full:{chat_id}', '0') or 0)
            seen_before = int(
                store.get_state(f'backfill_media_seen:{chat_id}', '0') or 0)
            if done_upto and tip and tip <= done_upto:
                media_total[chat_id] = seen_before
                _progress.say(f'  история разобрана до сообщения {done_upto}, '
                              f'нового нет — пропуск')
                continue
            if done_upto:
                # чат живой: заново читать 100+ тыс. сообщений ради хвоста в
                # пару сотен незачем. Границы чанков хвоста считаются от
                # done_upto — как в ночном инжесте, который так работает всегда
                _progress.say(f'  разобрано до {done_upto}, дочитываю хвост...')
            try:
                stats = await ingest_chat(client, store, chat_id, min_id=done_upto,
                                          progress=_progress, max_cost=remaining(),
                                          enrich_media=False,
                                          space=chat_space[chat_id].slug)
            except BudgetExceeded as e:
                spent += e.cost
                stopped = True
                break
            spent += stats.cost
            # знаменатель этапа 2 — медиа по ВСЕЙ истории. Полный проход даёт
            # его сразу, дочитывание хвоста — только прирост, который надо
            # добавить к посчитанному раньше
            media_total[chat_id] = (seen_before + stats.media_seen if done_upto
                                    else stats.media_seen)
            if tip:
                store.set_state(f'backfill_media_seen:{chat_id}',
                                str(media_total[chat_id]))
                store.set_state(f'backfill_full:{chat_id}', str(tip))
            pruned_note = (f', удалено устаревших чанков: {stats.pruned}'
                           if stats.pruned else '')
            _progress.say(f'  {stats.messages} сообщений -> {stats.new_chunks} чанков'
                  f'{pruned_note}, ~${stats.cost:.2f}')
        # Каталогизация документов из чатов качалки, не входящих в KB_CHAT_IDS:
        # без инжеста в RAG, только files/firmware — иначе файлы, скачанные из
        # «не-KB» чатов, невидимы для /fw и кнопок 📎
        if not stopped and catalog_only:
            from kb_firmware import document_filename, record_file
            from kb_ingest import fetch_topic_names, message_topic_id
            for chat_id, space in catalog_only.items():
                _progress.say(f'Каталог (без инжеста): {chat_id}...')
                topics = await fetch_topic_names(client, chat_id)
                recorded = 0
                async for msg in client.iter_messages(chat_id, reverse=True):
                    if msg.document is None:
                        continue
                    fname = document_filename(msg)
                    if not fname:
                        continue
                    record_file(store, msg, fname,
                                topics.get(message_topic_id(msg), ''),
                                space=space.slug)
                    recorded += 1
                _progress.say(f'  документов закаталогизировано: {recorded}')
        # Этап 2: vision/whisper — только наполнение кэша, чанки не трогаем
        if media_wanted and not stopped:
            for n, chat_id in enumerate(chat_ids, 1):
                total = media_total.get(chat_id, 0)
                _progress.say(f'[2/3] Медиа: {chat_id} (чат {n} из {len(chat_ids)}), '
                      f'всего медиа: {total}...')
                if not (chat_media[chat_id].vision or chat_media[chat_id].voice):
                    _progress.say('  обогащение выключено для этой области — пропуск')
                    continue
                if not total:
                    _progress.say('  медиа в чате нет — пропуск')
                    continue
                # продолжение прерванного этапа: докуда дошли в прошлый раз
                at_key, seen_key = (f'backfill_media_at:{chat_id}',
                                    f'backfill_media_seen_at:{chat_id}')
                at = 0 if force else int(store.get_state(at_key, '0') or 0)
                seen0 = 0 if force else int(store.get_state(seen_key, '0') or 0)
                if at:
                    _progress.say(f'  продолжаю с сообщения {at} '
                                  f'({seen0} медиа уже пройдено)')

                def _save(msg_id: int, seen: int, k=at_key, s=seen_key) -> None:
                    store.set_state(k, str(msg_id))
                    store.set_state(s, str(seen))

                try:
                    paid, cost = await enrich_chat_media(client, store, chat_id,
                                                         progress=_progress,
                                                         max_cost=remaining(),
                                                         media=chat_media[chat_id],
                                                         total=total, min_id=at,
                                                         seen0=seen0,
                                                         checkpoint=_save)
                except BudgetExceeded as e:
                    spent += e.cost
                    stopped = True
                    break
                spent += cost
                paid_media[chat_id] = paid
                # этап дошёл до конца: следующий прогон начнёт с начала
                # истории (и почти весь возьмёт из кэша), а не с середины
                store.set_state(at_key, '0')
                store.set_state(seen_key, '0')
                cached = max(total - paid, 0)
                _progress.say(f'  оплачено в этом прогоне: {paid}, ~${cost:.2f} '
                      f'(из кэша: {cached})')

        # Этап 3: пересборка чанков с описаниями из кэша; переэмбеддятся
        # только изменившиеся (id те же — сравнение по хэшу текста)
        if media_wanted and not stopped:
            for n, chat_id in enumerate(chat_ids, 1):
                _progress.say(f'[3/3] Чанки с медиа: {chat_id} '
                      f'(чат {n} из {len(chat_ids)})...')
                tip = tips.get(chat_id, 0)
                # пересобирать нечего: этап 2 ничего нового не оплатил, а
                # чанки с описаниями из кэша уже собраны на этой же истории
                if (not force and tip and not paid_media.get(chat_id)
                        and store.get_state(f'backfill_media:{chat_id}') == str(tip)):
                    _progress.say('  новых описаний нет, чанки уже собраны — пропуск')
                    continue
                try:
                    stats = await ingest_chat(client, store, chat_id, min_id=0,
                                              progress=_progress,
                                              max_cost=remaining(),
                                              space=chat_space[chat_id].slug,
                                              media=chat_media[chat_id])
                except BudgetExceeded as e:
                    spent += e.cost
                    stopped = True
                    break
                spent += stats.cost
                if tips.get(chat_id):
                    store.set_state(f'backfill_media:{chat_id}', str(tips[chat_id]))
                _progress.say(f'  обновлено чанков: {stats.new_chunks}, ~${stats.cost:.2f}')
        if not stopped:
            # общий пост-инжест конвейер: каталог -> PDF -> архивы -> HedEx ->
            # экстракция -> бэкап (kb_pipeline, тот же путь, что у ночного джоба)
            from kb_pipeline import run_post_ingest
            more, stopped = await run_post_ingest(store, spaces,
                                                  budget=remaining(),
                                                  progress=_progress,
                                                  report='print',
                                                  say=_progress.say)
            spent += more
        else:
            store.backup()
        _finish(store, spent, 'budget' if stopped else 'done')
    except KeyboardInterrupt:
        # Ctrl+C безопасен ровно по той же причине, что и лимит бюджета:
        # чанки пишутся пачками в транзакциях, media_cache — по одному
        # элементу, а state двигается только после успешной обработки чата
        _finish(store, spent, 'interrupted')
        raise SystemExit(130)


async def main() -> None:
    parser = argparse.ArgumentParser(description='Бэкфилл базы знаний')
    parser.add_argument('--dry-run', action='store_true',
                        help='посчитать объём и стоимость по категориям, без OpenAI')
    parser.add_argument('--max-cost', type=float, default=None, metavar='N',
                        help='остановиться при достижении бюджета N$ (безопасно: '
                             'повторный запуск продолжит с кэша)')
    parser.add_argument('--local-only', action='store_true',
                        help='только локальные конвейеры (архивы, HedEx, PDF, '
                             'экстракция) — БЕЗ подключения к Telegram: сессию '
                             'не трогает, качалку можно не останавливать')
    parser.add_argument('--force', action='store_true',
                        help='разобрать историю заново, игнорируя пометки о '
                             'уже разобранном (нужно, если сообщения удаляли)')
    parser.add_argument('--space', default='', metavar='SLUG',
                        help='обработать только одну область знаний '
                             '(по умолчанию — все из spaces.toml)')
    args = parser.parse_args()

    spaces = load_spaces()
    if args.space:
        one = spaces.get(args.space)
        if one is None:
            raise SystemExit(f'нет области «{args.space}»; есть: '
                             + ', '.join(spaces.slugs))
        spaces = Spaces([one])

    if args.local_only:
        if args.dry_run:
            raise SystemExit('--local-only несовместим с --dry-run: оценка '
                             'печатается самими конвейерами')
        await _local_only(spaces, args.max_cost)
        return

    api_id = int(_require('TELEGRAM_API_ID'))
    api_hash = _require('TELEGRAM_API_HASH')
    if not any(s.chats for s in spaces.all):
        raise SystemExit('ни одна область не задаёт чаты-источники '
                         '(KB_CHAT_IDS или chats в spaces.toml)')

    client = TelegramClient(
        'bot', api_id, api_hash,
        connection_retries=-1,
        retry_delay=300,
        auto_reconnect=True,
        request_retries=5,
        timeout=30,
        # полный прогон истории: длинные FloodWait пересыпаем, а не падаем
        flood_sleep_threshold=86400,
        **proxy_kwargs(),
    )
    # телеграмные предупреждения (FloodWait, обрывы) — в stderr, а не в тишину;
    # %(name)s подписывает источник (telethon.network.* vs наши модули)
    logging.basicConfig(level=logging.WARNING,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _progress.say('Подключаюсь к Telegram...')
    await client.start(phone=lambda: input('Enter your phone: '))
    _progress.say('Подключился.')
    try:
        if args.dry_run:
            await _dry_run(client, spaces)
        else:
            await _backfill(client, spaces, args.max_cost, force=args.force)
    finally:
        await client.disconnect()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # прерывание вне этапов: подключение к Telegram, --dry-run
        _progress.say('\nПрервано.')
        raise SystemExit(130)
