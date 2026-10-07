"""Почему файла нет на NAS: вся цепочка «чат → каталог → журнал → диск».

Скачивание зависит от нескольких условий сразу, и по одному лишь факту
«кнопки 📎 нет» не понять, какое из них не выполнилось: чат не качается,
расширение не в списке, качалка не видела сообщение (оно пришло, пока
контейнер лежал), файл скачан, но журнал потерян. Этот скрипт проверяет
их по очереди и называет причину.

    docker compose run --rm librarian python kb_diag.py S5735-V2_V600R025
    docker compose run --rm librarian python kb_diag.py --config

Telegram не нужен — только база, журналы и диск, поэтому качалку
останавливать не надо.
"""
from __future__ import annotations

import argparse
import os
import sys

from kb_firmware import _load_md5_journal
from kb_spaces import load_spaces
from kb_store import open_store

# Конвенция каталога: синтетические записи файлов-«сирот» (в журнале и на
# диске, но без сообщения в чате) получают отрицательный doc_id из md5
ORPHAN = 'сирота (есть на диске, сообщения в каталоге нет)'


def show_config(spaces) -> None:
    """Что именно качалка качает. Первое, что надо исключить: чат или
    расширение просто не в списке — тогда файл и не должен был скачаться."""
    print('Что качает качалка (download в spaces.toml или CHAT_IDS/'
          'FILE_EXTENSIONS из .env):\n')
    for s in spaces.all:
        print(f'#{s.slug} ({s.label})')
        print(f'  папка: {s.folder or "— не задана, скачивание невозможно"}')
        chats = ', '.join(str(c) for c in s.download_chats) or '— нет, не качаем'
        print(f'  чаты скачивания: {chats}')
        exts = ', '.join(s.download_extensions) or '— нет, не качаем'
        print(f'  расширения: {exts}')
        src = ', '.join(str(c) for c in s.chats) or '—'
        print(f'  чаты-источники знаний (каталог без скачивания): {src}')
        if s.folder:
            journal = _load_md5_journal(s.folder)
            mark = '' if journal else '   ← ПУСТО: файлы на диске будут без 📎'
            print(f'  журнал дедупликации: {len(journal)} записей{mark}')
        print()


def space_of_chat(spaces, chat_id: int):
    """Область, отвечающая за этот чат, — включая чаты, которые ТОЛЬКО
    качаются. `Spaces.for_chat` их не знает: он про «чей это вопрос»
    (чаты-источники и чаты ответа), а здесь важно «чьи это файлы»."""
    if not chat_id:
        return spaces.default
    for s in spaces.all:
        if chat_id in s.download_chats:
            return s
    return spaces.for_chat(chat_id)


def _verdict(space, row, journal_md5: str | None, on_disk: bool) -> str:
    """Главное в отчёте: не «что видно», а почему файла нет."""
    doc_id, name, md5, chat_id = row
    if on_disk and md5:
        return 'OK: скачан и привязан к каталогу — 📎 работает'
    if on_disk and not md5:
        return ('на диске ЕСТЬ, но в каталоге нет md5 — 📎 не покажется. '
                'Чинится ночным конвейером (link_local_files) или '
                'kb_backfill.py --local-only')
    if journal_md5 and not on_disk:
        return ('числится в журнале, но файла на диске НЕТ — удалён вручную '
                'или потерян том; журнал считает его скачанным и повторно '
                'качалка его не возьмёт')
    if space is None:
        return ('чат этого сообщения не привязан ни к одной области — '
                'ни скачивания, ни каталога')
    if chat_id and chat_id not in space.download_chats:
        return (f'чат {chat_id} НЕ в списке скачивания области #{space.slug} '
                f'(download.chats) — файл только каталогизируется')
    ext = name.rsplit('.', 1)[-1].lower() if '.' in name else ''
    if ext and ext not in space.download_extensions:
        return (f'расширение «{ext}» не входит в download.extensions области '
                f'#{space.slug} — скачивание пропущено намеренно')
    return ('условия скачивания выполнены, но файла нет: качалка НЕ ВИДЕЛА '
            'это сообщение вживую (контейнер лежал, шёл бэкфилл с той же '
            'сессией, рвалась сеть). Запись в каталоге появилась позже — '
            'ночной инжест каталогизирует, но НЕ скачивает')


def show_file(store, spaces, needle: str, limit: int) -> None:
    rows = [r for r in store.all_files()
            if needle.lower() in os.path.basename(r[1]).lower()]
    if not rows:
        print(f'В каталоге нет файлов с «{needle}».\n'
              'Значит, качалка не видела сообщение и ночной инжест его не '
              'разбирал: проверь, что чат есть в chats или download.chats '
              '(kb_diag.py --config) и что контейнер librarian жив.')
        return
    print(f'Записей в каталоге с «{needle}»: {len(rows)}'
          f'{f" (показаны первые {limit})" if len(rows) > limit else ""}\n')
    for doc_id, name in rows[:limit]:
        # file_by_doc_id: (name, md5, chat_id, msg_id, date)
        rec = store.file_by_doc_id(doc_id)
        md5 = rec[1] if rec else ''
        chat_id = rec[2] if rec else 0
        msg_id, date = (rec[3], rec[4]) if rec else (0, '')
        base = os.path.basename(name)
        space = space_of_chat(spaces, chat_id)
        folder = space.folder if space else ''
        journal = _load_md5_journal(folder) if folder else {}
        journal_md5 = journal.get(base)
        path = os.path.join(folder, base) if folder else ''
        on_disk = bool(path) and os.path.exists(path)

        print(f'### {base}')
        print(f'  запись каталога: doc_id={doc_id}'
              f'{" · " + ORPHAN if doc_id < 0 else ""}')
        link = (f'https://t.me/c/{str(chat_id)[4:]}/{msg_id}'
                if str(chat_id).startswith('-100') and msg_id else '')
        print(f'  чат: {chat_id or "—"} · область: '
              f'#{space.slug if space else "—"} · дата: {date or "—"}'
              f'{" · " + link if link else ""}')
        print(f'  md5 в каталоге: {md5 or "нет (значит, кнопки 📎 не будет)"}')
        print(f'  в журнале скачиваний: {journal_md5 or "нет"}')
        print(f'  на диске: {"да" if on_disk else "нет"}'
              f'{" · " + path if path else ""}')
        print(f'  ВЕРДИКТ: {_verdict(space, (doc_id, name, md5, chat_id), journal_md5, on_disk)}')
        print()


def _selftest() -> None:
    """Проверка вердиктов: именно они — весь смысл скрипта, и именно их
    легко разъехать с кодом качалки."""
    from kb_spaces import Space

    sp = Space(slug='main', folder='/dl', download_chats=(-1001,),
               download_extensions=('cc', 'pat'))
    row = (111, 'S5735-V2_V600R025C00SPC500.cc', '', -1001)

    # всё сошлось
    assert _verdict(sp, (111, row[1], 'abc', -1001), 'abc', True).startswith('OK')
    # на диске есть, но каталог без md5 — кнопки 📎 не будет
    assert 'link_local_files' in _verdict(sp, row, 'abc', True)
    # журнал считает скачанным, а файла нет — повторно качалка не возьмёт
    assert 'повторно' in _verdict(sp, row, 'abc', False)
    # чужой чат: только каталогизируем
    other = (111, row[1], '', -2002)
    assert 'НЕ в списке скачивания' in _verdict(sp, other, None, False)
    # расширение вне списка
    doc = (111, 'ReleaseNotes.pdf', '', -1001)
    assert 'расширение «pdf»' in _verdict(sp, doc, None, False)
    # всё разрешено, а файла нет — значит сообщение прошло мимо качалки
    assert 'НЕ ВИДЕЛА' in _verdict(sp, row, None, False)
    # чат вне областей
    assert 'не привязан' in _verdict(None, (111, row[1], '', -9), None, False)

    # чат, который ТОЛЬКО качается (в CHAT_IDS, но не в KB_CHAT_IDS):
    # Spaces.for_chat про него не знает, и вердикт был бы ложным
    class _Spaces:
        def __init__(self, s): self.all, self.default = (s,), s
        def for_chat(self, chat_id): return None       # как в проде для такого чата

    only_dl = Space(slug='main', folder='/dl', download_chats=(-1001,),
                    download_extensions=('cc',))
    assert space_of_chat(_Spaces(only_dl), -1001) is only_dl
    assert space_of_chat(_Spaces(only_dl), -7777) is None
    assert space_of_chat(_Spaces(only_dl), 0) is only_dl   # документы, не чат
    print('kb_diag selftest: OK')


def main() -> None:
    ap = argparse.ArgumentParser(
        description='Почему файла нет на NAS: каталог, журнал, диск')
    ap.add_argument('needle', nargs='?', default='',
                    help='часть имени файла, например S5735-V2_V600R025')
    ap.add_argument('--config', action='store_true',
                    help='только показать, какие чаты и расширения качаются')
    ap.add_argument('-n', type=int, default=10, help='сколько записей показать')
    ap.add_argument('--selftest', action='store_true',
                    help='проверить логику вердиктов (без базы и сети)')
    args = ap.parse_args()

    if args.selftest:
        _selftest()
        return

    spaces = load_spaces()
    if args.config or not args.needle:
        show_config(spaces)
        if not args.needle:
            return
    store = open_store()
    show_file(store, spaces, args.needle, args.n)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
