"""Сквозная проверка мультипространственного режима (два пространства).

Отдельный файл, как и kb_bot_check: librarian и kb_store читают окружение
на импорте, поэтому spaces.toml и env задаются ДО импортов — изнутри
модуля это невозможно.

Ловит класс поломок «всё уехало в одно пространство»: чанки без метки,
общий журнал скачиваний, общий ключ state у одноимённых файлов в разных
папках, глобальный поиск вместо своей области. Telegram и OpenAI не
нужны: эмбеддер подменяется детерминированной заглушкой.

Запуск: python kb_spaces_check.py
"""
import asyncio
import os
import sys
import tempfile

TMP = tempfile.mkdtemp()
HW = os.path.join(TMP, 'dl-huawei')
B4 = os.path.join(TMP, 'dl-b4')
os.makedirs(HW)
os.makedirs(B4)
CFG = os.path.join(TMP, 'spaces.toml')
with open(CFG, 'w', encoding='utf-8') as f:
    f.write(f'''
[huawei]
title = "Huawei"
persona = "инженеров по оборудованию Huawei"
chats = [-1001]
folder = "{HW.replace(os.sep, '/')}"
catalog = "huawei"
download = {{ chats = [-1001], extensions = ["pdf"] }}
gaps_chat = -1001

[b4]
title = "B4"
persona = "администраторов B4"
chats = [-2001]
folder = "{B4.replace(os.sep, '/')}"
catalog = "none"
''')

os.environ.update(
    KB_SPACES_FILE=CFG, KB_DB_PATH=os.path.join(TMP, 'kb.sqlite'),
    EMBED_DIM='8', KB_PDF='1', KB_ARCHIVE='0', KB_HEDEX='0',
    OPENAI_API_KEY='sk-test', TELEGRAM_API_ID='1', TELEGRAM_API_HASH='x',
    CHAT_IDS='-1001', KB_CHAT_IDS='-1001')

import kb_ingest  # noqa: E402
import kb_pdf  # noqa: E402
from kb_spaces import load_spaces  # noqa: E402
from kb_store import Chunk, open_store  # noqa: E402


async def fake_embed(texts):
    """Детерминированный «эмбеддинг»: по длине текста, без сети."""
    out = []
    for t in texts:
        v = [0.0] * 8
        v[len(t) % 8] = 1.0
        out.append(v)
    return out


kb_ingest.embed_texts = fake_embed
kb_pdf.embed_texts = fake_embed


def make_pdf(path: str, text: str) -> None:
    """Минимальный одностраничный PDF (pypdf его читает)."""
    from pypdf import PdfWriter
    w = PdfWriter()
    w.add_blank_page(width=200, height=200)
    with open(path, 'wb') as f:
        w.write(f)
    # текстовый слой не нужен: проверяем маршрутизацию, а не извлечение
    del text


async def main() -> int:
    spaces = load_spaces()
    store = open_store()
    assert store.default_space == 'huawei', store.default_space

    # 1. Чанки чатов метятся своим пространством
    for chat, space, text in ((-1001, 'huawei', 'прошивка S5735 лежит на ftp'),
                              (-2001, 'b4', 'mihomo не поднимает туннель')):
        c = Chunk(chat, 1, 'Топик', '2026-01-01', '2026-01-01', 1, 2, 'ivan',
                  text, (await fake_embed([text]))[0], space=space)
        store.upsert_chunks([c])
    assert dict(store.count_by_space()) == {'huawei': 1, 'b4': 1}, \
        store.count_by_space()

    # 2. Поиск: своя область — только свои фрагменты
    hw_vec = (await fake_embed(['прошивка S5735 лежит на ftp']))[0]
    assert [h.space for h in store.search('прошивка', hw_vec, space='huawei')] \
        == ['huawei']
    assert store.search('mihomo', None, space='huawei') == []
    assert [h.space for h in store.search('mihomo', None, space='b4')] == ['b4']

    # 3. Область поиска по чату + указатель
    sc, q = spaces.resolve(-1001, 'как обновить?')
    assert (sc.slug, sc.fallback, q) == ('huawei', True, 'как обновить?')
    sc, q = spaces.resolve(-1001, '#b4 как обновить?')
    assert (sc.slug, sc.explicit, q) == ('b4', True, 'как обновить?')
    assert spaces.resolve(999, 'в личке')[0].slug is None

    # 4. Конвейер: PDF каждой папки метится своим пространством,
    #    ключи state у неосновного пространства — со своим именем
    make_pdf(os.path.join(HW, 'manual.pdf'), 'huawei')
    make_pdf(os.path.join(B4, 'manual.pdf'), 'b4')   # то же имя!
    from kb_pipeline import run_post_ingest
    spent, stopped = await run_post_ingest(store, spaces, report='print')
    assert not stopped, 'бюджет не задан — остановок быть не должно'
    keys = [k for k, _ in store.state_items('pdf_ingested:')]
    assert len(keys) == 2, keys
    assert any(':b4:' in k for k in keys), 'у неосновного пространства свой ключ'
    assert not any(':huawei:' in k for k in keys), \
        'у пространства по умолчанию ключ обязан остаться историческим'

    # 5. Качалка: чат -> своя папка и свои расширения
    sys.modules.pop('librarian', None)
    import librarian
    assert librarian.CHAT_IDS == {-1001}
    same = os.path.normcase(os.path.normpath(
        librarian.DOWNLOAD_SPACES[-1001].folder)) == os.path.normcase(HW)
    assert same, librarian.DOWNLOAD_SPACES[-1001].folder
    assert librarian.KB_CHAT_SPACES[-2001].slug == 'b4'
    hw_folder = librarian.DOWNLOAD_SPACES[-1001].folder
    assert librarian.downloaded_files(hw_folder) == {}
    librarian.save_downloaded_file(hw_folder, 'a.pdf', 'f' * 32)
    assert librarian.downloaded_files(hw_folder) == {'a.pdf': 'f' * 32}
    assert librarian.downloaded_files(B4) == {}, 'журналы папок не пересекаются'
    assert os.path.exists(os.path.join(HW, 'downloaded_files.txt'))
    assert not os.path.exists(os.path.join(B4, 'downloaded_files.txt'))

    # 6. Инвентарь /sources знает про области
    rep = store.sources_report()
    assert dict(rep['spaces'])['b4'] == 1, rep['spaces']
    from kb_render import render_sources, spaces_help
    out = render_sources(rep)
    assert '#b4' in out and 'Области знаний' in out, out
    assert '#b4 — B4' in spaces_help(spaces)

    store.close()
    print('kb_spaces_check: OK')
    return 0


sys.exit(asyncio.run(main()))
