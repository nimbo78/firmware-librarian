"""Каталог прошивок, фаза A: детерминированный разбор имён файлов Huawei.

Модель и версия извлекаются регулярками из имени файла — без LLM.
Файлы каталогизируются по telegram document id (doc_id): живой захват в
качалке и ночной инжест/бэкфилл пишут в одну таблицу `files`, md5
дозаписывается после физического скачивания. Связка «файл → модель/версия» —
таблица `firmware` (фаза B добавит источники caption/LLM с confidence).
"""
from __future__ import annotations

import os
import re

_MD5_RE = re.compile(r'^[0-9a-f]{32}$')

# S5735-L, MA5608T, HG8145V5, AR3260, CE6857-48S6CQ-EI, NE40E, USG6300…
# Сегменты суффикса ограничены 6 символами и не могут начинаться с V<цифра> —
# иначе жадный матч съедает версию ('S5735-L-V200R019...' → модель S5735-L).
# Границы — lookaround вместо \b: '_' в именах файлов является словесным
# символом, и 'MA5608T_V800…' с \b не матчится.
MODEL_RE = re.compile(
    r'(?<![A-Z0-9])'
    r'((?:MA|HG|EG|AR|CE|NE|USG|ATN|OLT)\d{3,5}[A-Z0-9]*(?:-(?!V\d)[A-Z0-9]{1,6})*'
    r'|S\d{4}(?:-(?!V\d)[A-Z0-9]{1,6})*)'
    r'(?![A-Z0-9])')

# V800R018C10SPC500, V5R019C00S100 (ONT), V200R019C00SPC500H01 (патч)
VERSION_RE = re.compile(
    r'(?<![A-Z0-9])'
    r'V(\d{1,4})R(\d{1,4})(?:C(\d{1,4}))?(?:(?:SPC|SPH|S)(\d{1,4}))?'
    r'(?:H([A-Z0-9]{1,4}))?'
    r'(?![A-Z0-9])')


def parse_firmware_name(name: str) -> tuple[list[str], str, str]:
    """(модели, версия, ключ сортировки версии).

    Ключ — нулепаддинг компонентов V.R.C.SPC, лексикографическое сравнение
    ключей корректно упорядочивает версии ('какая последняя').
    """
    up = name.upper()
    models: list[str] = []
    for m in MODEL_RE.finditer(up):
        tok = m.group(1)
        if tok not in models:
            models.append(tok)
    vm = VERSION_RE.search(up)
    if not vm:
        return models, '', ''
    v, r, c, spc, h = vm.groups()
    key = f'{int(v):04d}.{int(r):04d}.{int(c or 0):04d}.{int(spc or 0):04d}'
    if h:
        key += f'.{h}'
    return models, vm.group(0), key


def document_filename(msg) -> str | None:
    """Имя файла из документа Telegram-сообщения (None, если не документ)."""
    from telethon.tl.types import DocumentAttributeFilename
    doc = getattr(msg, 'document', None)
    if doc is None:
        return None
    for attr in doc.attributes:
        if isinstance(attr, DocumentAttributeFilename):
            return attr.file_name
    return None


def record_file(store, msg, file_name: str, topic_name: str = '',
                md5: str = '') -> None:
    """Каталогизировать документ: метаданные в files + разбор имени в firmware."""
    doc = msg.document
    store.upsert_file(
        doc_id=doc.id, name=file_name, size=getattr(doc, 'size', 0) or 0,
        md5=md5, chat_id=msg.chat_id or 0, msg_id=msg.id,
        caption=(msg.raw_text or '')[:500], topic_name=topic_name,
        date=f'{msg.date:%Y-%m-%d}')
    models, version, version_key = parse_firmware_name(file_name)
    for model in models:
        store.upsert_firmware(doc.id, model, version, version_key)


def _load_md5_journal(folder: str) -> dict:
    """downloaded_files.txt качалки -> {имя: md5}. Формат журнала —
    <md5>,<имя> (новый) и <имя>,<md5> (старый); парсинг продублирован из
    download_telegram_files, который нельзя импортировать (side effects)."""
    path = os.path.join(folder, 'downloaded_files.txt')
    out: dict = {}
    if not os.path.exists(path):
        return out
    with open(path, encoding='utf-8') as f:
        for line in f.read().splitlines():
            head, sep, tail = line.partition(',')
            if not sep:
                continue
            if _MD5_RE.match(head):
                out[tail] = head
            elif _MD5_RE.match(tail):
                out[head] = tail
    return out


def link_local_files(store, folder: str) -> int:
    """Проставляет files.md5 записям каталога, чьи файлы скачаны на NAS ещё
    ДО появления каталога (md5 берётся из журнала дедупликации): без md5
    кнопка 📎 в /fw не показывается. Идемпотентно — обрабатываются только
    записи с пустым md5. Возвращает число привязанных файлов."""
    journal = _load_md5_journal(folder)
    if not journal:
        return 0
    linked = 0
    for doc_id, name in store.files_without_md5():
        base = os.path.basename(name)
        md5 = journal.get(base)
        if md5 and os.path.exists(os.path.join(folder, base)):
            store.set_file_md5(doc_id, md5)
            linked += 1
    return linked


def _selftest() -> None:
    cases = {
        'MA5608T_V800R018C10SPC500.zip': (['MA5608T'], 'V800R018C10SPC500'),
        'S5735-L-V200R019C00SPC500.cc': (['S5735-L'], 'V200R019C00SPC500'),
        'HG8145V5-V5R019C00S100.bin': (['HG8145V5'], 'V5R019C00S100'),
        'SmartAX_MA5608T_V800R017C10.tar.gz': (['MA5608T'], 'V800R017C10'),
        'CE6857-48S6CQ-EI-V200R005C10SPC800.cc': (['CE6857-48S6CQ-EI'],
                                                  'V200R005C10SPC800'),
        'V800R018C10SPC500H01.pat': ([], 'V800R018C10SPC500H01'),
        'manual.pdf': ([], ''),
        'photo_2026-01-01.jpg': ([], ''),
    }
    for name, (want_models, want_version) in cases.items():
        got_models, got_version, _ = parse_firmware_name(name)
        assert got_models == want_models, (name, got_models, want_models)
        assert got_version == want_version, (name, got_version, want_version)
    # порядок версий: R018 новее R017, ключи сравниваются лексикографически
    _, _, k_old = parse_firmware_name('MA5608T_V800R017C10SPC200.zip')
    _, _, k_new = parse_firmware_name('MA5608T_V800R018C10SPC500.zip')
    assert k_new > k_old, (k_old, k_new)

    # журнал дедупликации: оба формата, битые строки игнорируются
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, 'downloaded_files.txt'), 'w',
                  encoding='utf-8') as f:
            f.write('a' * 32 + ',fw1.zip\n')          # новый формат
            f.write('old_manual.pdf,' + 'b' * 32 + '\n')  # старый формат
            f.write('битая строка без запятой\n')
            f.write('имя,но-не-хэш\n')
        j = _load_md5_journal(tmp)
        assert j == {'fw1.zip': 'a' * 32, 'old_manual.pdf': 'b' * 32}, j
        assert _load_md5_journal(os.path.join(tmp, 'нет-такой-папки')) == {}
    print('kb_firmware selftest: OK')


if __name__ == '__main__':
    _selftest()
