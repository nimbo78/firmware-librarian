"""Рендер каталога и навигации для kb_bot: чистое форматирование.

Вынесено из kb_bot, чтобы это можно было тестировать: kb_bot не
импортируется без переменных окружения Telegram (создаёт клиента прямо
на импорте), а вёрстка каталога и дерево навигации — самая capризная
часть, которую хочется закреплять тестами.

Хранилище передаётся параметром, глобального состояния тут нет.

Selftest (без Telegram и OpenAI): python kb_render.py
"""
from __future__ import annotations

import re

from telethon import Button

from kb_firmware import (OS_NAMES, PRODUCT_CATEGORIES, product_category,
                         version_branch_label)


def msg_link(chat_id: int, msg_id: int) -> str | None:
    s = str(chat_id)
    if s.startswith('-100'):
        return f'https://t.me/c/{s[4:]}/{msg_id}'
    return None


def fmt_event(ts: str, kind: str, text: str, cost: float) -> str:
    line = f'{ts[5:16]} [{kind}] {text}'
    if cost:
        line += f' ~${cost:.2f}'
    return line


def user_help(bot_username: str = '') -> str:
    mention = f'@{bot_username}' if bot_username else '@<имя бота>'
    return (
        '🤖 Хранитель знаний чата. Что умею:\n'
        '\n'
        '❓ Вопросы по базе знаний (история чата + документация):\n'
        f'• /ask <вопрос> — или просто упомяни меня: {mention} <вопрос>\n'
        '• Ответь реплаем на мой ответ — продолжу диалог с учётом контекста\n'
        '  (можно уточнять: «а на R024?», «подробнее про DFS»).\n'
        '• Под ответом кнопки 👍/👎 — оценки делают базу лучше.\n'
        '• Если ответа не нашлось — вопрос запоминается; как только в чате\n'
        '  появится обсуждение, я сам отвечу реплаем.\n'
        '\n'
        '📦 Каталог прошивок и документации:\n'
        '• /fw — навигация по разделам, как на support.huawei.com\n'
        '• /fw <модель> [версия] — поиск: /fw S5735-S R024, /fw 5735\n'
        '• /sw <модель> — сводка по веткам софта\n'
        '• /download <начало имени> — все файлы с этим префиксом подряд\n'
        '  (включая .asc/.p7s для проверки подписи и многотомники)\n'
        '• В выдаче: софт по веткам от новых к старым (💿 образ, 🩹 патчи),\n'
        '  документация и прочее — блоком 📖 в конце.\n'
        '• Кнопка 📎 присылает файл прямо в чат (до 2 ГБ).\n'
        '  После трёх отправок с одного списка он удаляется — не мусорим.'
    )


ADMIN_HELP_EXTRA = (
    '\n\n🔧 Команды администратора (только в личке):\n'
    '/status — база, стоимость, курсоры инжеста (+кнопка инжеста)\n'
    '/events — последние 20 событий\n'
    '/gaps — вопросы без ответа или с 👎\n'
    '/review — подтвердить связки каталога (есть «принять все»)\n'
    '/ingest — внеплановый инжест сейчас (не ждать ночи)\n'
    '/notify on|off — уведомления о событиях в личку\n'
    'Любой другой текст в личке — вопрос к базе знаний.'
)


_SW_KIND_ORDER = {'software': 0, 'patch': 1, 'release_notes': 2, 'doc': 3,
                  'mib': 4, 'tool': 5, '': 6}


_SW_KIND_TITLES = {'software': '💿 Образ', 'patch': '🩹 Патчи',
                   'release_notes': '📃 Release notes',
                   'doc': '📖 Документация', 'mib': '🧾 MIB',
                   'tool': '🛠 Инструменты', '': '📁 Прочее'}


_RENDER_MAX_BRANCHES = 6


_RENDER_MAX_DOCS = 8


_SW_KINDS = ('software', 'patch')  # «софт-часть» ветки; остальное — в конец


NAV_PAGE_SIZE = 14


def render_grouped(rows: list, query: str, model_query: str = '',
                    max_models: int = 3) -> tuple[str, list, set]:
    """Единый рендер каталога для /fw, /sw и листа навигации.

    Порядок (решение владельца): сначала софт по веткам от новых к старым —
    🔀 R025 (💿 образ + 🩹 патчи), затем R024 и т.д., — а вся документация/
    RN/MIB/прочее одним блоком 📖 в конце модели с пометкой ветки.
    Запрошенная модель всегда первой — иначе серия (S5700) заливает лимит
    4096 и запрошенное отрезается. Возвращает (текст, кнопки 📎, seen_doc_ids)
    — seen нужен вызывающему, чтобы дополнять кнопки без дублей.
    """
    if not rows:
        return (f'По запросу «{query}» в каталоге пусто. Каталог наполняется '
                f'из имён файлов в чатах.', [], set())
    grouped: dict = {}
    is_series: dict = {}
    for r in rows:
        branch = version_branch_label(r[1])
        grouped.setdefault(r[0], {}).setdefault(branch, []).append(r)
        is_series[r[0]] = is_series.get(r[0], False) or bool(r[7])
    qnorm = re.sub(r'[^A-Z0-9]', '', (model_query or query).upper())

    def _model_rank(m: str) -> tuple:
        return (0 if qnorm and qnorm in re.sub(r'[^A-Z0-9]', '', m) else 1, m)

    out = [f'Каталог по запросу «{query}»:']
    buttons: list = []
    seen: set[int] = set()

    def _emit(r: tuple, extra: str = '') -> None:
        unconfirmed = ' · не подтверждено' if r[6] != 'high' else ''
        line = f'  • {r[2]} · {r[5]}{extra}{unconfirmed}'
        link = msg_link(r[3], r[4])
        if link:
            line += f'\n    {link}'
        out.append(line)
        if r[8] and r[9] not in seen and len(buttons) < 10:
            seen.add(r[9])
            buttons.append([Button.inline(f'📎 {r[2][:40]}',
                                          f'g:{r[9]}'.encode())])

    for model in sorted(grouped, key=_model_rank)[:max_models]:
        suffix = (' (вся серия — проверь совместимость!)'
                  if is_series[model] else '')
        out.append(f'\n📦 {model}{suffix}')
        branches = sorted((b for b in grouped[model] if b != 'без версии'),
                          reverse=True)
        if 'без версии' in grouped[model]:
            branches.append('без версии')
        # софт-часть по веткам; документация копится в общий хвост модели
        sw_branches: list[tuple[str, dict]] = []
        docs_pool: list[tuple[str, tuple]] = []
        doc_names: set[str] = set()
        for branch in branches:
            seen_names: set[str] = set()
            by_kind: dict = {}
            for r in grouped[model][branch]:
                if r[2] in seen_names:
                    continue  # повторные посты того же файла не дублируем
                seen_names.add(r[2])
                if r[10] in _SW_KINDS:
                    by_kind.setdefault(r[10], []).append(r)
                elif r[2] not in doc_names:
                    doc_names.add(r[2])
                    docs_pool.append((branch, r))
            if by_kind:
                sw_branches.append((branch, by_kind))
        for branch, by_kind in sw_branches[:_RENDER_MAX_BRANCHES]:
            os_name = OS_NAMES.get(branch.split(' ')[0], '')
            os_mark = f' · {os_name}' if os_name else ''
            out.append(f'🔀 {branch}{os_mark}')
            for kind in sorted(by_kind, key=lambda k: _SW_KIND_ORDER.get(k, 6)):
                out.append(f'  {_SW_KIND_TITLES.get(kind, "📁 Прочее")}:')
                for r in by_kind[kind][:5]:
                    _emit(r)
                if len(by_kind[kind]) > 5:
                    out.append(f'    … и ещё {len(by_kind[kind]) - 5}')
        if len(sw_branches) > _RENDER_MAX_BRANCHES:
            out.append(f'  … и ещё веток с софтом: '
                       f'{len(sw_branches) - _RENDER_MAX_BRANCHES} '
                       f'(уточни версией: /fw {model} R0xx)')
        if docs_pool:
            out.append('📖 Документация и прочее:')
            for branch, r in docs_pool[:_RENDER_MAX_DOCS]:
                emoji = _SW_KIND_TITLES.get(r[10], '📁 Прочее').split()[0]
                branch_mark = f' {branch}' if branch != 'без версии' else ''
                _emit(r, extra=f' · {emoji}{branch_mark}')
            if len(docs_pool) > _RENDER_MAX_DOCS:
                out.append(f'    … и ещё {len(docs_pool) - _RENDER_MAX_DOCS} '
                           f'(все — в /fw через навигацию)')
    return '\n'.join(out), buttons, seen


def nav_tree(store) -> dict:
    """Категория -> {модель -> {rkey(9 симв. version_key) -> (счётчик, метка)}}.
    Метка ветки берётся из сырой версии (version_branch_label), не из ключа."""
    tree: dict = {}
    for model, version, vkey in store.fw_all():
        cat = product_category(model)
        rkey = (vkey or '')[:9] or '-'
        node = tree.setdefault(cat, {}).setdefault(model, {})
        if rkey in node:
            node[rkey] = (node[rkey][0] + 1, node[rkey][1])
        else:
            node[rkey] = (1, version_branch_label(version))
    return tree


def branch_files(branch: dict) -> int:
    """Сумма файлов по всем веткам модели (значения — (счётчик, метка))."""
    return sum(cnt for cnt, _ in branch.values())


def nav_root_view(store) -> tuple[str, list]:
    tree = nav_tree(store)
    if not tree:
        return 'Каталог пока пуст — файлы появятся после инжеста.', []
    buttons = []
    for ci, cat in enumerate(PRODUCT_CATEGORIES):
        models = tree.get(cat)
        if models:
            n_files = sum(branch_files(r) for r in models.values())
            buttons.append([Button.inline(
                f'{cat} · {len(models)} моделей · {n_files} файлов',
                f'n:c:{ci}:0'.encode())])
    return ('Каталог прошивок и документации. Выбери раздел '
            '(или сразу /fw <модель>):'), buttons


def nav_category_view(store, ci: int, page: int) -> tuple[str, list]:
    cat = PRODUCT_CATEGORIES[ci]
    models = sorted(nav_tree(store).get(cat, {}).items())
    if not models:
        return f'{cat}: пусто.', [[Button.inline('⬅️ Разделы', b'n:r')]]
    start = page * NAV_PAGE_SIZE
    chunk = models[start:start + NAV_PAGE_SIZE]
    buttons = []
    for i in range(0, len(chunk), 2):  # по две модели в ряд
        row = [Button.inline(f'{m} ({branch_files(r)})', f'n:m:{m}'.encode())
               for m, r in chunk[i:i + 2]]
        buttons.append(row)
    nav_row = []
    if page > 0:
        nav_row.append(Button.inline('◀️', f'n:c:{ci}:{page - 1}'.encode()))
    nav_row.append(Button.inline('⬅️ Разделы', b'n:r'))
    if start + NAV_PAGE_SIZE < len(models):
        nav_row.append(Button.inline('▶️', f'n:c:{ci}:{page + 1}'.encode()))
    buttons.append(nav_row)
    pages = (len(models) - 1) // NAV_PAGE_SIZE + 1
    return f'{cat} — модели ({page + 1}/{pages}):', buttons


def nav_model_view(store, model: str) -> tuple[str, list]:
    branches = {}
    for m, rkeys in nav_tree(store).get(product_category(model), {}).items():
        if m == model:
            branches = rkeys
    if not branches:
        return f'{model}: файлов нет.', [[Button.inline('⬅️ Разделы', b'n:r')]]
    ci = PRODUCT_CATEGORIES.index(product_category(model))
    buttons = []
    for rkey in sorted(branches, reverse=True):
        count, label = branches[rkey]  # метка уже человекочитаемая
        buttons.append([Button.inline(f'{label} · {count} файл(ов)',
                                      f'n:v:{model}:{rkey}'.encode())])
    buttons.append([Button.inline('⬅️ Модели', f'n:c:{ci}:0'.encode())])
    return f'{model} — ветки версий:', buttons


def nav_files_view(store, model: str, rkey: str) -> tuple[str, list]:
    rows = store.find_firmware_exact(model, '' if rkey == '-' else rkey)
    if rkey == '-':
        rows = [r for r in rows if not r[1]]
    text, buttons, _ = render_grouped(rows, model, model, max_models=1)
    buttons.append([Button.inline('⬅️ Ветки версий', f'n:m:{model}'.encode())])
    return text[:4000], buttons


def _selftest() -> None:
    """Проверяет вёрстку каталога и дерево навигации — то, что раньше
    гонялось руками: kb_bot не импортировался без окружения Telegram."""
    import os
    import tempfile

    from kb_store import open_store

    with tempfile.TemporaryDirectory() as tmp:
        os.environ['KB_DB_PATH'] = os.path.join(tmp, 'kb.sqlite')
        os.environ['EMBED_DIM'] = '4'
        store = open_store()

        def add(doc_id, name, model, version, vkey, kind):
            store.upsert_file(doc_id=doc_id, name=name, size=1,
                              md5=f'{doc_id:032d}', chat_id=-1001234, msg_id=doc_id,
                              caption='', topic_name='', date='2026-09-05', kind=kind)
            store.upsert_firmware(doc_id, model, version, vkey)

        add(1, 'S5735-S_V200R024SPH121.pat', 'S5735-S', 'V200R024SPH121',
            '000200.000024.000000.000121', 'patch')
        add(2, 'S5735-S_V200R025C00SPC500.cc', 'S5735-S', 'V200R025C00SPC500',
            '000200.000025.000000.000500', 'software')
        add(3, 'S5735-S ReleaseNotes.pdf', 'S5735-S', 'V200R025C00SPC500',
            '000200.000025.000000.000500', 'release_notes')
        add(4, 'S6730-H_V600R025C00SPC500.cc', 'S6730-H', 'V600R025C00SPC500',
            '000600.000025.000000.000500', 'software')

        rows = store.find_firmware('S5735-S')
        text, buttons, seen = render_grouped(rows, 'S5735-S', 'S5735-S')
        # запрошенная модель первой, софт по веткам от новых к старым
        assert text.index('S5735-S') < text.index('R025') , text
        assert text.index('R025') < text.index('R024'), 'ветки от новых к старым'
        # документация — одним блоком в конце модели
        assert '📖' in text and text.index('💿') < text.index('📖'), text
        assert '🩹' in text, text
        assert len(seen) == 3 and len(buttons) == 3, (seen, buttons)

        # метка ветки берётся из СЫРОЙ версии: V600 R025, а не «V6 R1»
        tree = nav_tree(store)
        labels = [lbl for models in tree.values() for br in models.values()
                  for _, lbl in br.values()]
        assert any('R025' in x for x in labels), labels
        assert not any(re.fullmatch(r'V\d R\d', x) for x in labels), labels

        root_text, root_buttons = nav_root_view(store)
        assert root_buttons and 'Каталог' in root_text
        cat = list(tree)[0]
        ci = PRODUCT_CATEGORIES.index(cat)
        cat_text, cat_buttons = nav_category_view(store, ci, 0)
        assert '(1/1)' in cat_text and cat_buttons, cat_text
        mod_text, mod_buttons = nav_model_view(store, 'S5735-S')
        assert 'ветки версий' in mod_text and len(mod_buttons) >= 2, mod_text
        files_text, files_buttons = nav_files_view(store, 'S5735-S',
                                                   '000200.000025')
        assert 'S5735-S' in files_text and files_buttons, files_text

        # пустой каталог не роняет вёрстку
        assert render_grouped([], 'нетмодели')[0].startswith('По запросу')

        assert msg_link(-1001234567890, 42) == 'https://t.me/c/1234567890/42'
        assert msg_link(12345, 42) is None, 'у синтетических чатов ссылок нет'
        # год из метки отрезается (в чате важны месяц/день/время)
        assert fmt_event('2026-09-05 05:00', 'ingest', 'текст',
                         0.0) == '09-05 05:00 [ingest] текст'
        assert '~$0.12' in fmt_event('2026-09-05 05:00', 'pdf', 'т', 0.12)
        assert '@mybot' in user_help('mybot') and '@<имя бота>' in user_help('')
        store.close()
    print('kb_render selftest: OK')


if __name__ == '__main__':
    _selftest()
