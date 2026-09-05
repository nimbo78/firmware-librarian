"""Инжест документации Huawei HedEx (.hdx) в базу знаний (флаг KB_HEDEX=1).

.hdx — это zip с DITA-HTML: profile.xml (продукт/версия/язык), resources/
navi.xml (дерево разделов с заголовками), resources/*.html (топики).
Чанк — страница документации: заголовок `[продукт версия — путь по
оглавлению]` + текст; длинные страницы режутся по ~4000 символов,
навигационные заглушки короче MIN_PAGE_CHARS отсекаются. Инжестятся только
страницы из navi.xml (вне оглавления — попапы и служебные фрагменты).

Синтетический ПОЛОЖИТЕЛЬНЫЙ chat_id из MD5 пакета (как у PDF, kb_pdf.py);
topic_id=1 отличает HedEx-чанки от PDF (topic_id=0) — бот показывает
«документация: …» вместо «файл, стр. N».

Кросс-версионный дедуп: страницы с идентичным ТЕЛОМ (sha1 без строки
заголовка — в ней версия) инжестятся один раз; пакеты обрабатываются от
новых версий к старым, поэтому общие страницы приписываются новейшей.
Пакет учитывается по MD5 (state hedex_ingested:<md5>) — повторный прогон
уже проиндексированное не трогает. Прерывание по BudgetExceeded безопасно:
чанки и state пишутся по ходу, продолжение не платит дважды.

Selftest (без Telegram и OpenAI): python kb_hedex.py
"""
from __future__ import annotations

import hashlib
import html as html_mod
import logging
import os
import re
import xml.etree.ElementTree as ET
import zipfile

from kb_store import Chunk

logger = logging.getLogger(__name__)

PAGE_CHUNK_CHARS = 4000   # как у kb_pdf/kb_ingest
MIN_PAGE_CHARS = 150      # короче — навигационная заглушка, пропускаем
BREADCRUMB_LEVELS = 3     # последних уровней оглавления в заголовке чанка
HTML_TEXT_RATIO = 0.35    # оценка доли текста в DITA-HTML для dry-run

# конвенция topic_id для синтетических чатов (см. ScoredChunk в kb_store)
HEDEX_TOPIC_ID = 1

_BLOCK_RE = re.compile(
    r'</(?:p|div|tr|li|h[1-6]|table|ul|ol|dl|dd|pre|section)>|<br\s*/?>',
    re.IGNORECASE)
_DROP_RE = re.compile(r'(?is)<(script|style|head)\b.*?</\1>')
_TAG_RE = re.compile(r'<[^>]+>')


def hedex_enabled() -> bool:
    return os.getenv('KB_HEDEX', '0') == '1'


def _file_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 16), b''):
            h.update(block)
    return h.hexdigest()


def list_hdx(folder: str) -> list[str]:
    """Относительные пути .hdx, рекурсивно — включая hedex_extracted/,
    куда kb_archive складывает пакеты, извлечённые из архивов."""
    if not os.path.isdir(folder):
        return []
    out = []
    for root, dirs, files in os.walk(folder):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for n in files:
            if n.lower().endswith('.hdx'):
                out.append(os.path.relpath(os.path.join(root, n), folder))
    return sorted(out)


def parse_profile(z: zipfile.ZipFile) -> dict:
    """product/version/name/language/date из profile.xml пакета."""
    root = ET.fromstring(z.read('profile.xml'))

    def val(tag: str, default: str = '') -> str:
        el = root.find(tag)
        return (el.text or default).strip() if el is not None and el.text else default

    return {
        'product': val('productType'),
        'version': val('productVersion'),
        'name': val('libName'),
        'language': val('language'),
        'date': val('issueDate') or '1970-01-01',
    }


def parse_navi(z: zipfile.ZipFile) -> list[tuple[str, tuple[str, ...]]]:
    """[(url, breadcrumbs)] в порядке оглавления; url без якоря, первым
    вхождением (страница может встречаться в дереве несколько раз)."""
    root = ET.fromstring(z.read('resources/navi.xml'))
    seen: set[str] = set()
    out: list[tuple[str, tuple[str, ...]]] = []

    def walk(node, trail: tuple[str, ...]):
        txt = (node.get('txt') or '').strip()
        crumbs = trail + (txt,) if txt else trail
        url = (node.get('url') or '').split('#')[0]
        if url and url not in seen:
            seen.add(url)
            out.append((url, crumbs))
        for child in node.findall('topic'):
            walk(child, crumbs)

    for top in root.findall('topic'):
        walk(top, ())
    return out


def page_text(html: str) -> str:
    """DITA-HTML -> плоский текст: блочные теги дают перевод строки,
    остальная разметка убирается, сущности разворачиваются."""
    html = _DROP_RE.sub(' ', html)
    html = _BLOCK_RE.sub('\n', html)
    text = _TAG_RE.sub(' ', html)
    text = html_mod.unescape(text)
    lines = [re.sub(r'[ \t\xa0]+', ' ', ln).strip() for ln in text.split('\n')]
    return '\n'.join(ln for ln in lines if ln)


def split_text(text: str, limit: int = PAGE_CHUNK_CHARS) -> list[str]:
    """Режет текст на куски НЕ ДЛИННЕЕ limit символов.

    Гарантия «не длиннее» держится и на сверхдлинной ОДИНОЧНОЙ строке:
    в HTML документации переносов может не быть вовсе (широкая таблица
    в один <tr>, абзац-простыня), и такую строку надо рубить принудительно.
    Прежняя версия клала её в кусок целиком — чанк на 28 тыс. символов
    уезжал в эмбеддер, провайдер отвечал HTTP 400 («8193 > 8192 токенов»)
    и ронял весь шаг конвейера.

    На текстах без сверхдлинных строк результат совпадает с прежним —
    границы существующих чанков не сдвигаются (иначе поменялись бы их id
    и вся документация переэмбеддилась бы заново).
    """
    parts: list[str] = []
    buf: list[str] = []
    size = 0

    def flush() -> None:
        nonlocal buf, size
        if buf:
            parts.append('\n'.join(buf))
            buf, size = [], 0

    for line in text.split('\n'):
        while len(line) > limit:
            flush()
            cut = line.rfind(' ', limit - 200, limit)  # по возможности по слову
            if cut <= 0:
                cut = limit
            parts.append(line[:cut])
            line = line[cut:].lstrip()
        if buf and size + len(line) > limit:
            flush()
        buf.append(line)
        size += len(line) + 1
    flush()
    return parts


def hdx_chunks(path: str, md5: str) -> tuple[dict, list[tuple[Chunk, str]]]:
    """(инфо пакета, [(чанк, sha1 тела)]) в порядке оглавления.

    msg_first/msg_last — сквозной номер чанка: id чанка детерминирован
    от (chat_id, номер), а порядок стабилен, пока не меняется сам файл
    (state по MD5 гарантирует, что пакет не перечитывается)."""
    z = zipfile.ZipFile(path)
    info = parse_profile(z)
    synthetic_chat = int(md5[:12], 16)  # >0, как у kb_pdf
    label = f"{info['product']} {info['version']}".strip()
    names = {i.filename for i in z.infolist()}

    out: list[tuple[Chunk, str]] = []
    seq = 0
    for url, crumbs in parse_navi(z):
        fn = 'resources/' + url
        if fn not in names:
            continue
        try:
            body = page_text(z.read(fn).decode('utf-8', 'replace'))
        except Exception as e:
            logger.warning('hedex page failed %s in %s: %s', url, path, e)
            continue
        if len(body) < MIN_PAGE_CHARS:
            continue
        crumb = ' → '.join(crumbs[-BREADCRUMB_LEVELS:]) or url
        topic_name = f'{label} — {crumb}'[:200]
        for part in split_text(body):
            seq += 1
            body_sha = hashlib.sha1(part.encode('utf-8')).hexdigest()
            out.append((Chunk(
                chat_id=synthetic_chat, topic_id=HEDEX_TOPIC_ID,
                topic_name=topic_name,
                date_from=info['date'], date_to=info['date'],
                msg_first=seq, msg_last=seq, authors='',
                text=f'[{topic_name}]\n{part}'), body_sha))
    return info, out


def scan_hdx(store, folder: str) -> tuple[int, int]:
    """Для dry-run: (новых пакетов, оценка символов текста). Без распаковки
    страниц — по размерам HTML внутри zip и доле текста в разметке."""
    files = 0
    chars = 0
    for name in list_hdx(folder):
        path = os.path.join(folder, name)
        md5 = _file_md5(path)
        if store.get_state(f'hedex_ingested:{md5}'):
            continue
        try:
            z = zipfile.ZipFile(path)
            html_bytes = sum(i.file_size for i in z.infolist()
                             if i.filename.lower().endswith(('.html', '.htm'))
                             and 'toctopics' not in i.filename)
        except Exception as e:
            logger.warning('hedex scan failed for %s: %s', name, e)
            continue
        files += 1
        chars += int(html_bytes * HTML_TEXT_RATIO)
    return files, chars


def _version_key(version: str) -> str:
    """Сортировочный ключ версии: числа дополняются нулями, чтобы
    R025 > R023 и V200 > V100 лексикографически."""
    return re.sub(r'\d+', lambda m: m.group(0).zfill(6), version or '')


async def ingest_hdx(store, folder: str, progress=None,
                     max_cost: float | None = None) -> tuple[int, int, float]:
    """Инжест новых .hdx из folder. Возвращает (пакетов, чанков, стоимость $).

    Пакеты сортируются по версии от новых к старым: при кросс-версионном
    дедупе общая страница достаётся новейшей документации."""
    from kb_ingest import BudgetExceeded, EMBED_BATCH, embed_cost, embed_texts

    todo: list[tuple[str, str, str]] = []  # (version, path, md5)
    for name in list_hdx(folder):
        path = os.path.join(folder, name)
        md5 = _file_md5(path)
        if store.get_state(f'hedex_ingested:{md5}'):
            continue
        try:
            with zipfile.ZipFile(path) as z:
                version = parse_profile(z)['version']
        except Exception as e:
            logger.warning('hedex profile failed for %s: %s', name, e)
            continue
        todo.append((version, path, md5))
    todo.sort(key=lambda t: _version_key(t[0]), reverse=True)

    seen_bodies = store.doc_text_hashes() if todo else set()
    files = 0
    chunks_total = 0
    cost = 0.0
    for version, path, md5 in todo:
        info, pairs = hdx_chunks(path, md5)
        fresh: list[Chunk] = []
        for chunk, body_sha in pairs:
            if body_sha in seen_bodies:
                continue
            seen_bodies.add(body_sha)
            fresh.append(chunk)
        known = store.existing_ids([c.id for c in fresh])
        new_chunks = [c for c in fresh if c.id not in known]
        logger.warning('HedEx %s %s: страниц-чанков %d, новых %d',
                       info['name'], version, len(pairs), len(new_chunks))
        for i in range(0, len(new_chunks), EMBED_BATCH):
            part = new_chunks[i:i + EMBED_BATCH]
            vectors = await embed_texts([c.text for c in part])
            for c, v in zip(part, vectors):
                c.embedding = v
            store.upsert_chunks(part)
            cost += embed_cost([c.text for c in part])
            if progress and (i // EMBED_BATCH) % 10 == 0:
                progress('hedex', files, i + len(part), cost)
            if max_cost is not None and cost >= max_cost:
                raise BudgetExceeded(cost)
        store.set_state(f'hedex_ingested:{md5}', os.path.basename(path))
        files += 1
        chunks_total += len(new_chunks)
        if progress:
            progress('hedex', files, chunks_total, cost)
    return files, chunks_total, cost


def _selftest() -> None:
    import io
    import tempfile

    def make_hdx(path: str, version: str, extra_page: str = '') -> None:
        long_body = '<p>' + '</p><p>'.join(
            f'Строка конфигурации номер {i} с достаточно длинным текстом '
            f'про WLAN и роуминг.' for i in range(120)) + '</p>'
        pages = {
            'resources/cfg_wpa3.html':
                '<html><head><script>junk()</script></head><body><h1>WPA3'
                '</h1><p>Общая страница: настройка WPA3-SAE на контроллере, '
                'одинаковая в обеих версиях документации. Содержит процедуру '
                'включения, требования к версиям точек доступа и проверку '
                'результата командой display. Включает таблицу параметров.'
                '</p><table><tr><td>Параметр</td><td>Значение &amp; примечание'
                '</td></tr></table></body></html>',
            'resources/long_guide.html':
                f'<html><body><h1>Долгий гайд</h1>{long_body}</body></html>',
            'resources/stub.html': '<html><body><p>NAV</p></body></html>',
        }
        navi = ('<?xml version="1.0" encoding="UTF-8"?><topics>'
                '<topic txt="Configuration" url="">'
                '<topic txt="Security" url="cfg_wpa3.html"/>'
                '<topic txt="Long" url="long_guide.html"/>'
                '<topic txt="Stub" url="stub.html"/></topic>'
                f'{extra_page}</topics>')
        profile = (f'<?xml version="1.0" encoding="UTF-8"?><profile>'
                   f'<libName>WLAN Product Documentation</libName>'
                   f'<productType>WLAN</productType>'
                   f'<productVersion>{version}</productVersion>'
                   f'<language>en</language><issueDate>2026-06-01</issueDate>'
                   f'</profile>')
        with zipfile.ZipFile(path, 'w') as z:
            z.writestr('profile.xml', profile)
            z.writestr('resources/navi.xml', navi)
            for fn, content in pages.items():
                z.writestr(fn, content)
            if extra_page:
                z.writestr('resources/only_new.html',
                           '<html><body><p>Страница только новой версии: '
                           'изменённое поведение DFS-каналов после апгрейда, '
                           'подробности и ограничения для радиопланирования, '
                           'таблица совместимости точек доступа по сериям и '
                           'рекомендации по порядку перезагрузки контроллеров '
                           'в кластере при переходе между ветками.'
                           '</p></body></html>')

    with tempfile.TemporaryDirectory() as tmp:
        old_pkg = os.path.join(tmp, 'old.hdx')
        new_pkg = os.path.join(tmp, 'new.hdx')
        make_hdx(old_pkg, 'V200R023C00')
        make_hdx(new_pkg, 'V200R025C00',
                 '<topic txt="Whats New" url="only_new.html"/>')

        md5 = _file_md5(new_pkg)
        with zipfile.ZipFile(new_pkg) as z:
            info = parse_profile(z)
            navi = parse_navi(z)
        assert info['version'] == 'V200R025C00' and info['product'] == 'WLAN'
        assert navi[0] == ('cfg_wpa3.html', ('Configuration', 'Security')), navi[0]
        urls = [u for u, _ in navi if u]
        assert urls == ['cfg_wpa3.html', 'long_guide.html', 'stub.html',
                        'only_new.html'], urls

        text = page_text('<p>a&amp;b</p><script>x</script><td>c</td>')
        assert 'a&b' in text and 'x' not in text, text

        info, pairs = hdx_chunks(new_pkg, md5)
        names = [c.topic_name for c, _ in pairs]
        # заглушка отсечена, длинная страница разбита на несколько чанков
        assert not any('Stub' in n for n in names), names
        assert sum('Long' in n for n in names) >= 2, names
        assert all(c.chat_id > 0 and c.topic_id == HEDEX_TOPIC_ID
                   for c, _ in pairs)
        assert all(len(c.text) <= PAGE_CHUNK_CHARS + 300 for c, _ in pairs)
        wpa = next(c for c, _ in pairs if 'Security' in c.topic_name)
        assert wpa.topic_name.startswith('WLAN V200R025C00 — ')
        assert 'Значение & примечание' in wpa.text

        # кросс-версионный дедуп: тела общих страниц совпадают,
        # уникальная страница новой версии — нет
        _, old_pairs = hdx_chunks(old_pkg, _file_md5(old_pkg))
        old_hashes = {sha for _, sha in old_pairs}
        new_hashes = {sha for _, sha in pairs}
        assert old_hashes < new_hashes, 'общие тела должны совпадать по sha1'
        only_new = new_hashes - old_hashes
        assert len(only_new) == 1, only_new

        # сортировка версий: новые раньше старых
        vs = ['V200R023C00', 'V200R025C00', 'V200R024C10']
        assert sorted(vs, key=_version_key, reverse=True)[0] == 'V200R025C00'

        # РЕГРЕССИЯ: сверхдлинная ОДИНОЧНАЯ строка обязана рубиться.
        # В HTML документации переносов может не быть вовсе (широкая
        # таблица в один <tr>), и такой кусок уезжал в эмбеддер целиком —
        # провайдер отвечал HTTP 400 «8193 > 8192 токенов» и ронял шаг.
        assert split_text('x' * 10) == ['x' * 10]
        wide = 'ячейка данных ' * 3000            # ~42000 симв., без переносов
        parts = split_text(wide)
        assert all(len(p) <= PAGE_CHUNK_CHARS for p in parts), \
            [len(p) for p in parts]
        assert len(parts) >= 10, len(parts)
        assert ''.join(p.replace(' ', '') for p in parts) == \
            wide.replace(' ', ''), 'рубка не должна терять содержимое'
        # смесь: короткие строки + одна гигантская, порядок сохраняется
        mixed = split_text('начало\n' + 'q' * 9000 + '\nконец')
        assert all(len(p) <= PAGE_CHUNK_CHARS for p in mixed), mixed
        # хвост длинной строки склеивается со следующей короткой —
        # содержимое не теряется и порядок сохраняется
        assert mixed[0] == 'начало', mixed[0]
        assert mixed[-1].endswith('конец'), mixed[-1][-20:]
        # страница целиком: заголовок + тело влезают в лимит эмбеддера
        for c, _ in pairs:
            assert len(c.text) <= PAGE_CHUNK_CHARS + 300, len(c.text)
    print('kb_hedex selftest: OK')


if __name__ == '__main__':
    _selftest()
