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
from datetime import datetime

_MD5_RE = re.compile(r'^[0-9a-f]{32}$')

# S5735-L, S5735-V2 (поколение железа!), MA5608T, HG8145V5, AR3260, AC6805,
# CE6857-48S6CQ-EI, NE40E, CX600-M2, USG6300, AirEngine9700-M, AirEngineX761…
# Длинные словесные префиксы стоят в альтернативе первыми, чтобы короткие
# (NE, AR) не перехватывали их начало; словесные префиксы допускают
# разделитель перед номером ('CloudEngine 5882'). Сегменты суффикса
# ограничены 6 символами и не могут начинаться с ВЕРСИИ V<цифры>R<цифра> —
# иначе жадный матч съедает версию ('S5735-L-V200R019...' → модель S5735-L);
# при этом короткое '-V2' (второе поколение) — легитимная часть модели.
# Границы — lookaround вместо \b: '_' в именах файлов является словесным
# символом, и 'MA5608T_V800…' с \b не матчится.
MODEL_RE = re.compile(
    r'(?<![A-Z0-9])'
    r'((?:(?:AIRENGINEX|AIRENGINE|CLOUDENGINE|NETENGINE|OCEANSTOR)[ _-]?'
    r'|USG|ATN|OLT|MA|HG|EG|AR|CE|NE|AP|AC|CX)'
    r'\d{3,5}[A-Z0-9]*(?:-(?!V\d{1,4}R\d)[A-Z0-9]{1,6})*'
    r'|S\d{3,5}(?:SERIES)?[A-Z]{0,3}(?:-(?!V\d{1,4}R\d)[A-Z0-9]{1,6})*'
    r'|UPS\d{3,5})'
    r'(?![A-Z0-9])')

# Продукты-слова без числовой модели: софт-платформы, СХД, узкие семейства
WORD_MODEL_RE = re.compile(
    r'(?<![A-Z0-9])'
    r'(IMASTER[ _]?NCE(?:[ _]?(?:CAMPUSINSIGHT|CAMPUS|FABRICINSIGHT'
    r'|SERVERINSTALL|T))?'
    r'|SMARTKIT|EASYSUITE|EASYOPS|ESIGHT|DCUPDATECHECK|SMARTDC|IBMA|UEN'
    r'|CLOUDLINK(?:[ _](?:BOX[ _]?\d+|ENDPOINTS))?'
    r'|FUSIONSPHERE(?:[ _]OPENSTACK)?|FUSIONSERVER(?:[ _]PRO)?'
    r'|STORAGE[ _]MEDIUM'
    r'|OCEANSTOR(?:[ _]DORADO)?)'
    r'(?![A-Z0-9])')

# Серверные узлы FusionServer: '1288H V5', '2288X V5', '5288 V3', 'CH242 V3',
# 'XH321 V5', 'RH2288H V3' — Vn здесь поколение железа, версии софта свои
SERVER_MODEL_RE = re.compile(
    r'(?<![A-Z0-9])'
    r'((?:[CXR]H\d{3,4}[A-Z]{0,2})|\d{4}[A-Z]{1,2}|\d{4})[ _-](V\d)'
    r'(?![A-Z0-9])')

# Бандлы «семейство + перечень номеров»: 'AirEngine 5700&6700&8700&9700D',
# 'S200,_S300,_S500...', 'CloudEngine_5800&6800' → отдельная модель на номер
BUNDLE_RE = re.compile(
    r'(?<![A-Z0-9])'
    r'(AIRENGINEX|AIRENGINE|CLOUDENGINE|NETENGINE|CE|AR|AC|S)[ _-]?'
    r'(\d{3,5}[A-Z]{0,2}(?:[ _]*[&,][ _]*(?:AND[ _]+)?\d{3,5}[A-Z]{0,2})+)'
    r'(?![A-Z0-9])')
_BUNDLE_ITEM_RE = re.compile(r'\d{3,5}[A-Z]{0,2}')

# Dash-бандлы: 'CE6800-8800-9800' = серии CE6800+CE8800+CE9800 (семантика
# имён Huawei — подтверждено владельцем). Каждый сегмент — ЦЕЛИКОМ из цифр:
# 'CE8850-64CQ-EI' бандлом не является ('64CQ' с буквами = суффикс модели).
DASH_BUNDLE_RE = re.compile(
    r'(?<![A-Z0-9])'
    r'(AIRENGINE|CLOUDENGINE|NETENGINE|CE|AR|AC|S)[ _-]?'
    r'(\d{3,5}(?:-\d{3,5})+)'
    r'(?![A-Z0-9])')

# Подписи ОС для веток версий — только подтверждённые владельцем маппинги
OS_NAMES = {'V200': 'VRP', 'V600': 'YunShan OS'}

# Версионные токены в ЗАПРОСЕ пользователя ('R025', 'V600', 'SPC500'):
# отделяются от модели и работают фильтром по версии, а не частью имени
VERSION_TOKEN_RE = re.compile(
    r'^(?:V\d{1,4}[A-Z0-9]*|R\d{1,4}|C\d{1,4}|SPC\d{1,4}|SPH\d{1,4})$',
    re.IGNORECASE)


def split_query(query: str) -> tuple[str, list[str]]:
    """'S5735-S-V2 R025' -> ('S5735-S-V2', ['R025']).

    Версионные токены, написанные ОТДЕЛЬНЫМИ словами, уходят в фильтр;
    '-V2' внутри модели не трогается (нет пробела).
    """
    model_parts: list[str] = []
    version_tokens: list[str] = []
    for token in query.split():
        if VERSION_TOKEN_RE.match(token):
            version_tokens.append(token.upper())
        else:
            model_parts.append(token)
    if not model_parts:
        # запрос из одной версии — ищем как есть, фильтровать нечего
        return query, []
    return ' '.join(model_parts), version_tokens

# V800R018C10SPC500, V5R019C00S100 (ONT), V200R024SPH1B0 (hex в патче),
# V200R022HP1501 (hot patch), V200R019C00SPC500H01.
# Lookbehind пропускает ЦИФРУ перед V — версия бывает приклеена к модели
# без разделителя ('AC6805V200R022C10SPC100'); буква перед V блокируется.
VERSION_RE = re.compile(
    r'(?<![A-Z])'
    r'V(\d{1,4})R(\d{1,4})(?:C(\d{1,4}))?'
    r'(?:(?:SPC|SPH|HP|S)([0-9A-Z]{1,4}))?'
    r'(?:H([0-9A-Z]{1,4}))?'
    r'(?![A-Z0-9])')

# Точечные версии СХД/UC: '6.1.8.SPH30', '20.1.103.SPC28', '5.1.0.48'.
# Хвостовой lookahead запрещает только продолжение цифрами (обрубок длинной
# версии), а точку расширения ('…SPC28.zip') пропускает.
DOTTED_VERSION_RE = re.compile(
    r'(?<![0-9.])'
    r'(\d{1,2})\.(\d{1,3})\.(\d{1,3})'
    r'(?:\.?(?:SPC|SPH)?(\d{1,4}))?'
    r'(?![0-9])(?!\.[0-9])')


def _pad(token: str | None) -> str:
    """Нулепаддинг компонента версии; буквенно-цифровые ('1B0') — zfill."""
    if not token:
        return '0000'
    return f'{int(token):04d}' if token.isdigit() else token.zfill(4)


def parse_firmware_name(name: str) -> tuple[list[str], str, str]:
    """(модели, версия, ключ сортировки версии).

    Версия ищется ПЕРВОЙ и вырезается из строки — иначе жадный хвост модели
    заглатывает приклеенную версию ('AC6805V200R022...'). Ключ — нулепаддинг
    компонентов, лексикографическое сравнение упорядочивает версии.
    """
    up = name.upper()
    version = ''
    key = ''
    vm = VERSION_RE.search(up)
    if vm:
        v, r, c, spc, h = vm.groups()
        version = vm.group(0)
        key = f'{_pad(v)}.{_pad(r)}.{_pad(c)}.{_pad(spc)}'
        if h:
            key += f'.{h.zfill(4)}'
        up = up[:vm.start()] + '§' + up[vm.end():]
    else:
        dm = DOTTED_VERSION_RE.search(up)
        if dm:
            a, b, c2, d = dm.groups()
            version = dm.group(0)
            key = f'{_pad(a)}.{_pad(b)}.{_pad(c2)}.{_pad(d)}'
            up = up[:dm.start()] + '§' + up[dm.end():]

    models: list[str] = []

    def add(tok: str) -> None:
        tok = tok.strip(' _-')
        tok = re.sub(r'SERIES$', '', tok)  # 'S9300SERIES' -> 'S9300'
        if tok and tok not in models:
            models.append(tok)

    bundle_spans = []
    for bm in BUNDLE_RE.finditer(up):
        bundle_spans.append(bm.span())
        prefix = bm.group(1)
        for num in _BUNDLE_ITEM_RE.findall(bm.group(2)):
            add(prefix + num)
    for bm in DASH_BUNDLE_RE.finditer(up):
        bundle_spans.append(bm.span())
        prefix = bm.group(1)
        for num in bm.group(2).split('-'):
            add(prefix + num)

    def in_bundle(pos: int) -> bool:
        return any(s <= pos < e for s, e in bundle_spans)

    for m in MODEL_RE.finditer(up):
        if not in_bundle(m.start()):
            add(re.sub(r'[ _]+', '', m.group(1)))
    for m in WORD_MODEL_RE.finditer(up):
        add(re.sub(r'[ _]+', '-', m.group(1)))
    for m in SERVER_MODEL_RE.finditer(up):
        add(f'{m.group(1)}-{m.group(2)}')
    return models, version, key


# Категории верхнего уровня для навигации /fw (как разделы на support.huawei):
# детерминированно по префиксу модели, порядок проверок важен (S — последним
# среди букв, чтобы не съесть SMARTKIT; AP раньше AR не нужен — разные буквы)
PRODUCT_CATEGORIES = ['Коммутаторы', 'WLAN', 'Роутеры', 'Доступ (OLT/ONT)',
                      'СХД', 'Серверы', 'Безопасность', 'ПО и инструменты',
                      'Прочее']


_VR_LABEL_RE = re.compile(r'V(\d+)R(\d+)', re.IGNORECASE)
_DOTTED_LABEL_RE = re.compile(r'(\d+)\.(\d+)')


def version_branch_label(version: str) -> str:
    """Короткая метка ветки версий для кнопки навигации, из СЫРОЙ версии
    (не из padded-ключа): 'V600R025C00SPC500' -> 'V600 R025' (нули как у
    Huawei), '6.1.8.SPH30' -> '6.1', '' -> 'без версии'."""
    if not version:
        return 'без версии'
    m = _VR_LABEL_RE.search(version)
    if m:
        return f'V{m.group(1)} R{m.group(2)}'
    m = _DOTTED_LABEL_RE.search(version)
    if m:
        return f'{m.group(1)}.{m.group(2)}'
    return version[:16]


def product_category(model: str) -> str:
    m = model.upper()
    if m.startswith(('IMASTER', 'SMARTKIT', 'EASYSUITE', 'EASYOPS', 'ESIGHT',
                     'DCUPDATECHECK', 'SMARTDC', 'CLOUDLINK', 'FUSIONSPHERE',
                     'FUSIONSERVER', 'IBMA', 'UEN', 'STORAGE')):
        return 'ПО и инструменты'
    if m.startswith('OCEANSTOR'):
        return 'СХД'
    if m.startswith(('AC', 'AIRENGINE', 'AP', 'WA', 'WX')):
        return 'WLAN'
    if m.startswith(('MA', 'OLT', 'HG', 'EG')):
        return 'Доступ (OLT/ONT)'
    if m.startswith(('NE', 'CX', 'AR', 'ATN', 'NETENGINE')):
        return 'Роутеры'
    if m.startswith('USG'):
        return 'Безопасность'
    if m.startswith(('CH', 'XH', 'RH')) or re.match(r'\d{4}', m):
        return 'Серверы'
    if m.startswith(('CE', 'CLOUDENGINE', 'S')):
        return 'Коммутаторы'
    return 'Прочее'


_SIGNATURE_RE = re.compile(r'\.(asc|p7s|cms|crl)(\.(asc|p7s))?$', re.IGNORECASE)
_DOC_WORDS_RE = re.compile(
    r'guide|documentation|description|matrix|password|acceptance|training'
    r'|introduction|report|notes|upgrade|информац|материал', re.IGNORECASE)
_PATCH_RE = re.compile(r'(?:sph|hp)[0-9a-z]{1,4}(?![a-z0-9])|\bpatch\b',
                       re.IGNORECASE)


def classify_name(name: str) -> str:
    """Тип файла по имени: signature/patch/software/doc/release_notes/mib/tool.

    Подписи (.asc/.p7s/.cms/.crl) — 40% журнала: в выдаче /fw это мусор,
    поэтому classify первым делом отсекает их. Пустая строка = не определили.
    """
    low = name.lower()
    if _SIGNATURE_RE.search(low):
        return 'signature'
    if re.search(r'(?<![a-z])mibs?(?![a-z])', low):
        return 'mib'
    if 'release note' in low or 'release_note' in low:
        return 'release_notes'
    if _DOC_WORDS_RE.search(low):
        return 'doc'
    if low.endswith('.pat') or _PATCH_RE.search(low):
        return 'patch'
    if re.search(r'smartkit|easysuite|easyops|dcupdatecheck|(?<![a-z])tool',
                 low):
        return 'tool'
    if low.endswith(('.cc', '.bin', '.mod')) or '.web.' in low:
        return 'software'
    if low.endswith(('.docx', '.doc', '.pdf', '.xlsx', '.xls', '.txt',
                     '.chm')):
        return 'doc'
    if low.endswith(('.zip', '.rar', '.7z', '.tar.gz')):
        return 'software'
    return ''


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
        date=f'{msg.date:%Y-%m-%d}', kind=classify_name(file_name))
    models, version, version_key = parse_firmware_name(file_name)
    for model in models:
        store.upsert_firmware(doc.id, model, version, version_key)


def _norm_model(s: str) -> str:
    return re.sub(r'[^A-Z0-9]', '', s.upper())


def auto_resolve_firmware(store) -> int:
    """Снимает medium-связки, которые подтверждает детерминированный разбор
    имени того же файла (парсер независимо нашёл ту же модель). Такая связка
    избыточна — её уже представляет high-запись, созданная reparse_files.
    Запускать ПОСЛЕ reparse_files. Возвращает число снятых связок."""
    removed = 0
    for rowid, model, name in store.medium_firmware_with_names():
        det_models, _, _ = parse_firmware_name(name)
        if _norm_model(model) in {_norm_model(m) for m in det_models}:
            store.delete_firmware_row(rowid)
            removed += 1
    return removed


def reparse_files(store) -> int:
    """Перепрогоняет разбор имён по ВСЕМУ каталогу. Нужен после улучшения
    регулярок: старые записи files получают новые связки firmware и типы
    (kind) без повторного инжеста. Идемпотентен (PK + DO NOTHING), дёшев —
    регулярки по нескольким тысячам имён. Возвращает число новых связок."""
    added = 0
    for row in store.all_files(with_kind=True):
        doc_id, name, kind = row
        new_kind = classify_name(name)
        if new_kind and new_kind != kind:
            store.set_file_kind(doc_id, new_kind)
        models, version, version_key = parse_firmware_name(name)
        for model in models:
            if store.upsert_firmware(doc_id, model, version, version_key):
                added += 1
        if version:
            # трупы старого парсера: связка есть, версия пустая — теперь
            # версию видим, пустой дубль только мусорит ветку «без версии»
            store.delete_empty_version_rows(doc_id, models)
    return added


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
    """Синхронизация каталога с журналом дедупликации и диском. Идемпотентно.

    1) Проставляет files.md5 записям каталога, чьи файлы скачаны на NAS ещё
       ДО появления каталога: без md5 кнопка 📎 в /fw не показывается.
    2) Файлы-«сироты»: есть в журнале и на диске, но сообщения-источника в
       каталоге нет (удалено из чата, файл положили в папку руками) —
       создаётся синтетическая запись. doc_id — ОТРИЦАТЕЛЬНЫЙ, из md5
       (у telegram-документов id положительные, коллизий нет), chat_id=0 —
       ссылки на пост не будет, но /fw найдёт и 📎 отправит.
    Возвращает число привязанных/созданных файлов."""
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
    known = {os.path.basename(n) for _, n in store.all_files()}
    for name, md5 in journal.items():
        base = os.path.basename(name)
        if base in known:
            continue
        path = os.path.join(folder, base)
        if not os.path.exists(path):
            continue
        doc_id = -int(md5[:12], 16)
        mdate = datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d')
        store.upsert_file(doc_id=doc_id, name=base, size=os.path.getsize(path),
                          md5=md5, chat_id=0, msg_id=0, caption='',
                          topic_name='', date=mdate, kind=classify_name(base))
        models, version, version_key = parse_firmware_name(base)
        for model in models:
            store.upsert_firmware(doc_id, model, version, version_key)
        linked += 1
    return linked


def _selftest() -> None:
    cases = {
        'MA5608T_V800R018C10SPC500.zip': (['MA5608T'], 'V800R018C10SPC500'),
        'S5735-L-V200R019C00SPC500.cc': (['S5735-L'], 'V200R019C00SPC500'),
        'HG8145V5-V5R019C00S100.bin': (['HG8145V5'], 'V5R019C00S100'),
        'AirEngine9700-M_V200R021C00SPH010.pat':
            (['AIRENGINE9700-M'], 'V200R021C00SPH010'),
        'NetEngine8000-M8_V800R022C00SPC600.cc':
            (['NETENGINE8000-M8'], 'V800R022C00SPC600'),
        'AP7060DN-V200R021C00.bin': (['AP7060DN'], 'V200R021C00'),
        # поколение -V2 — часть модели, а версия после '_' не заглатывается
        'S5735-V2_V600R025C00SPC500.cc': (['S5735-V2'], 'V600R025C00SPC500'),
        'S5735-S-V2_V600R025SPH120.PAT.asc': (['S5735-S-V2'], 'V600R025SPH120'),
        # реальные кейсы из журнала: приклеенная версия, hex-патчи, hot patch
        'AC6805V200R022C10SPC100.cc': (['AC6805'], 'V200R022C10SPC100'),
        'AC6805_V200R022HP1501.pat': (['AC6805'], 'V200R022HP1501'),
        'S5731-H_V200R024SPH1b0.pat': (['S5731-H'], 'V200R024SPH1B0'),
        # семейство + пробел, серия '9300series', роутеры CX
        'CloudEngine 5882 V200R023SPH150 Patch Release Notes(word).zip':
            (['CLOUDENGINE5882'], 'V200R023SPH150'),
        'S9300series_V200R021SPH257.pat': (['S9300'], 'V200R021SPH257'),
        'CX600-M2 V800R011SPH110 Patch Release Notes.zip':
            (['CX600-M2'], 'V800R011SPH110'),
        'AirEngineX761-V200R025C00SPH001.pat':
            (['AIRENGINEX761'], 'V200R025C00SPH001'),
        # бандлы: перечень номеров с общим префиксом
        'S3700&S5700&S6700_V200R024SPH1b0.7z':
            (['S3700', 'S5700', 'S6700'], 'V200R024SPH1B0'),
        'AirEngine 5700&6700&8700&9700D V200R023C00SPC100.zip':
            (['AIRENGINE5700', 'AIRENGINE6700', 'AIRENGINE8700',
              'AIRENGINE9700D'], 'V200R023C00SPC100'),
        # dash-бандлы: несколько серий в одном имени; суффиксы с буквами —
        # НЕ бандл, а модель (CE8850-64CQ-EI)
        'CE6800-8800-9800_V300R024C00SPC500.cc':
            (['CE6800', 'CE8800', 'CE9800'], 'V300R024C00SPC500'),
        'CE8850-64CQ-EI-V200R005C10SPC800_2.cc':
            (['CE8850-64CQ-EI'], 'V200R005C10SPC800'),
        # серверы, софт-платформы, точечные версии СХД/UC
        '1288H_V5_V100R005C00SPC272.zip': (['1288H-V5'], 'V100R005C00SPC272'),
        'iMasterNCE_Campus_V300R022C00SPC202_Campus_Combine_linux_x86_64.zip':
            (['IMASTERNCE-CAMPUS'], 'V300R022C00SPC202'),
        'CloudLink Box 300 20.1.103.SPC28.zip':
            (['CLOUDLINK-BOX-300'], '20.1.103.SPC28'),
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

    # метки веток версий: нули как у Huawei, точечные версии не как V/R
    assert version_branch_label('V600R025C00SPC500') == 'V600 R025'
    assert version_branch_label('V200R024SPH1B0') == 'V200 R024'
    assert version_branch_label('6.1.8.SPH30') == '6.1'
    assert version_branch_label('20.1.103.SPC28') == '20.1'
    assert version_branch_label('') == 'без версии'

    # категории навигации
    assert product_category('S5735-L') == 'Коммутаторы'
    assert product_category('SMARTKIT') == 'ПО и инструменты'
    assert product_category('AC6805') == 'WLAN'
    assert product_category('MA5800') == 'Доступ (OLT/ONT)'
    assert product_category('1288H-V5') == 'Серверы'
    assert product_category('OCEANSTOR-DORADO') == 'СХД'
    assert product_category('CX600-M2') == 'Роутеры'

    # классификатор типов: подписи — почти половина журнала, режем из выдачи
    assert classify_name('AC6805V200R022C10SPC100.cc.p7s') == 'signature'
    assert classify_name('EasyOps_V100R022C10HP0010.zip.cms.asc') == 'signature'
    assert classify_name('S5731-H_V200R024SPH1b0.pat') == 'patch'
    assert classify_name('AC6805V200R022C10SPC100.cc') == 'software'
    assert classify_name('CE_V200R023C00SPC500_MIB.zip') == 'mib'
    assert classify_name('AC V200R024SPH150 Patch Release Notes(word).zip') \
        == 'release_notes'
    assert classify_name('MA5800 Feature Guide 11PDF.zip') == 'doc'
    assert classify_name('SmartKit_V100R023C00SPC521.zip') == 'tool'
    assert classify_name('S3700&S5700&S6700_V200R024SPH1b0.7z') == 'patch'

    # разбор запроса: версия отдельным словом -> фильтр, -V2 внутри — модель
    assert split_query('S5735-S-V2 R025') == ('S5735-S-V2', ['R025'])
    assert split_query('MA5608T V800R018 SPC500') == ('MA5608T',
                                                      ['V800R018', 'SPC500'])
    assert split_query('5735') == ('5735', [])
    assert split_query('R025') == ('R025', [])  # только версия — не пустим модель

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
