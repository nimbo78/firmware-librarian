"""Пространства знаний: что относится к одной области (Huawei, B4, …).

Пространство = чаты-источники + чаты, где бот отвечает + папка документов +
персона в промпте + подсказки терминологии. У чанка ровно одно пространство
происхождения (колонка chunks.space), у вопроса — область поиска: одно
пространство или все (см. SPACES_PLAN.md).

Это ЕДИНСТВЕННОЕ место, где конфиг превращается в объекты, chat_id — в своё
пространство, а текст «#b4 вопрос» — в область поиска. Остальные модули
знают только slug.

Конфиг — spaces.toml (путь в KB_SPACES_FILE, по умолчанию ./spaces.toml;
читается stdlib-tomllib). Без файла из переменных окружения собирается одно
неявное пространство с slug KB_SPACE (по умолчанию «main») — поведение
одиночной установки не меняется, .env править не нужно.

Селфтест: python kb_spaces.py
"""
from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass, field

# Указатель «искать везде»: зарезервирован, пространство так назвать нельзя
ALL = 'all'
SLUG_RE = re.compile(r'^[a-z0-9][a-z0-9_-]{0,31}$')
# «#b4» отдельным словом: в начале или после пробела, дальше пробел/конец.
# Хэштег, а не «/ask@b4»: так Telegram адресует команды другому боту.
_SELECTOR_RE = re.compile(r'(?:(?<=\s)|^)#([A-Za-z0-9][A-Za-z0-9_-]*)(?=\s|$)')
CATALOGS = ('huawei', 'none')
SCOPES = ('home', 'all')

# Персона и подсказки неявного пространства = то, что зашито в промптах до
# появления пространств: включение пространств не меняет ни тон ответов,
# ни качество разворота сленга в терминологию Huawei
_DEFAULT_PERSONA = 'инженеров по оборудованию Huawei'
_DEFAULT_HINTS = 'оборудование Huawei: VRP, CloudEngine, AirEngine, версии V200R0xx'


def parse_chat_topics(raw) -> dict[int, set[int]]:
    """'-100123:15,-100123:22,-100999' -> {-100123: {15, 22}, -100999: set()}.

    Принимает CSV-строку (KB_ANSWER_CHAT_IDS) или список записей из toml
    (строки «чат:топик» либо просто числа). Пустой набор топиков = отвечаем
    в чате где угодно — в форуме иначе бот засоряет все топики подряд."""
    items = raw.split(',') if isinstance(raw, str) else [str(x) for x in raw]
    out: dict[int, set[int]] = {}
    for item in items:
        item = item.strip()
        if not item:
            continue
        chat, _, topic = item.partition(':')
        topics = out.setdefault(int(chat), set())
        if topic.strip():
            topics.add(int(topic))
    return out


def _csv_ints(raw: str) -> tuple[int, ...]:
    return tuple(int(x) for x in raw.split(',') if x.strip())


@dataclass(frozen=True)
class Space:
    slug: str
    title: str = ''
    persona: str = _DEFAULT_PERSONA       # «ассистент чата <persona>»
    hints: str = ''                       # терминология для расширения запроса
    chats: tuple[int, ...] = ()           # источники знаний (ночной инжест)
    # где отвечает: {чат: {топики}}; пусто в конфиге = все чаты-источники целиком
    answer: dict[int, set[int]] = field(default_factory=dict)
    folder: str = ''                      # документы и прошивки; пусто = нет
    catalog: str = 'none'                 # 'huawei' — парсер имён и /fw; 'none'
    download_chats: tuple[int, ...] = ()  # откуда качалка тянет файлы
    download_extensions: tuple[str, ...] = ()
    gaps_chat: int = 0                    # еженедельный пост «помогите» (0 = нет)
    gaps_topic: int = 0
    default_scope: str = 'home'           # 'home' — своё, потом все; 'all' — сразу все

    @property
    def label(self) -> str:
        return self.title or self.slug

    @property
    def answer_chats(self) -> set[int]:
        return set(self.answer)


@dataclass(frozen=True)
class Scope:
    """Область поиска для одного вопроса — всё, что о ней нужно знать
    отвечающему коду: где искать, можно ли расширяться и как подписать ответ.

    Собирается только в Spaces.resolve(): правила «указатель важнее чата»,
    «чат без привязки ищет везде» и «своё пространство с фолбэком» живут
    там, а не расползаются по обработчикам бота."""
    space: Space | None          # None — искать по всем пространствам
    fallback: bool = False       # пусто в своём пространстве -> повтор по всем
    explicit: bool = False       # область названа указателем (#b4 / #all)
    multi: bool = False          # пространств в системе больше одного

    @property
    def slug(self) -> str | None:
        """Аргумент store.search(space=…): None — по всем."""
        return self.space.slug if self.space else None

    @property
    def label(self) -> str:
        return self.space.label if self.space else 'все области'

    @property
    def hints(self) -> str:
        return self.space.hints if self.space else ''


class Spaces:
    """Набор пространств; первое в конфиге — по умолчанию (получает
    чанки без явной метки и данные, созданные до появления пространств)."""

    def __init__(self, spaces: list[Space]):
        if not spaces:
            raise ValueError('нужно хотя бы одно пространство')
        self.all: tuple[Space, ...] = tuple(spaces)
        self.default: Space = spaces[0]
        self._by_slug = {s.slug: s for s in spaces}
        # чат -> пространство: сначала по чатам ответа, затем по источникам
        self._by_chat: dict[int, Space] = {}
        for s in spaces:
            for chat in list(s.chats) + list(s.answer):
                self._by_chat.setdefault(chat, s)

    @property
    def slugs(self) -> tuple[str, ...]:
        return tuple(s.slug for s in self.all)

    def get(self, slug: str) -> Space | None:
        return self._by_slug.get(slug)

    def for_chat(self, chat_id: int) -> Space | None:
        """Домашнее пространство чата (None — чат ни к чему не привязан:
        личка админа, общий чат — там ищем везде)."""
        return self._by_chat.get(chat_id)

    def answer_topics(self) -> dict[int, set[int]]:
        """Объединённый гейт бота: {чат: {топики}} по всем пространствам."""
        out: dict[int, set[int]] = {}
        for s in self.all:
            for chat, topics in s.answer.items():
                if chat in out and (not out[chat] or not topics):
                    out[chat] = set()      # один из конфигов разрешает весь чат
                else:
                    out.setdefault(chat, set()).update(topics)
        return out

    def resolve(self, chat_id: int, text: str) -> tuple[Scope, str]:
        """(область поиска, текст без указателя) для входящего вопроса.

        Указатель важнее всего: «#b4 …» — только B4, «#all …» — везде, и в
        обоих случаях фолбэка нет (человек сказал, где искать). Иначе —
        домашнее пространство чата с фолбэком на все области; чат без
        привязки (личка админа, чужой чат) ищет везде сразу."""
        tag, rest = self.parse_selector(text)
        multi = len(self.all) > 1
        if not multi:
            # одно пространство: «везде» и «в своём» — одно и то же, но
            # искать по конкретному разделу vec0 дешевле полного скана
            return Scope(self.default, multi=False), rest
        if tag == ALL:
            return Scope(None, explicit=True, multi=True), rest
        if tag:
            return Scope(self._by_slug[tag], explicit=True, multi=True), rest
        home = self.for_chat(chat_id)
        if home is None or home.default_scope == 'all':
            return Scope(None, multi=True), rest
        return Scope(home, fallback=True, multi=True), rest

    def parse_selector(self, text: str) -> tuple[str | None, str]:
        """«#b4 как настроить» -> ('b4', 'как настроить'); «#all …» -> ('all', …);
        без указателя -> (None, text). Чужие хэштеги (не имена пространств)
        остаются в тексте — люди ставят их и по своим поводам."""
        for m in _SELECTOR_RE.finditer(text):
            tag = m.group(1).lower()
            if tag == ALL or tag in self._by_slug:
                rest = (text[:m.start()] + ' ' + text[m.end():]).strip()
                return tag, re.sub(r'\s{2,}', ' ', rest)
        return None, text.strip()


def _space_from_table(slug: str, tbl: dict) -> Space:
    if not SLUG_RE.match(slug) or slug == ALL:
        raise ValueError(f'spaces.toml: недопустимое имя пространства «{slug}» '
                         f'(латиница/цифры/-/_, до 32 символов, не «{ALL}»)')
    if not isinstance(tbl, dict):
        raise ValueError(f'spaces.toml: [{slug}] должен быть таблицей')
    chats = tuple(int(x) for x in tbl.get('chats', ()))
    answer = parse_chat_topics(tbl.get('answer', ()))
    if not answer:
        answer = {chat: set() for chat in chats}
    catalog = str(tbl.get('catalog', 'none'))
    scope = str(tbl.get('default_scope', 'home'))
    if catalog not in CATALOGS:
        raise ValueError(f'spaces.toml: [{slug}] catalog = «{catalog}», '
                         f'допустимо {"/".join(CATALOGS)}')
    if scope not in SCOPES:
        raise ValueError(f'spaces.toml: [{slug}] default_scope = «{scope}», '
                         f'допустимо {"/".join(SCOPES)}')
    dl = tbl.get('download', {}) or {}
    return Space(
        slug=slug, title=str(tbl.get('title', '')),
        persona=str(tbl.get('persona', _DEFAULT_PERSONA)),
        hints=str(tbl.get('hints', '')), chats=chats, answer=answer,
        folder=str(tbl.get('folder', '')), catalog=catalog,
        download_chats=tuple(int(x) for x in dl.get('chats', ())),
        download_extensions=tuple(
            str(x).strip().lower().lstrip('.') for x in dl.get('extensions', ())),
        gaps_chat=int(tbl.get('gaps_chat', 0) or 0),
        gaps_topic=int(tbl.get('gaps_topic', 0) or 0),
        default_scope=scope)


def _implicit_space(env) -> Space:
    """Одиночная установка без spaces.toml: всё из переменных окружения,
    как было до пространств. catalog='huawei' — каталог сейчас включён всегда."""
    slug = env.get('KB_SPACE', '').strip() or 'main'
    if not SLUG_RE.match(slug) or slug == ALL:
        raise ValueError(f'KB_SPACE=«{slug}»: латиница/цифры/-/_, не «{ALL}»')
    chats = _csv_ints(env.get('KB_CHAT_IDS', ''))
    answer = parse_chat_topics(env.get('KB_ANSWER_CHAT_IDS', ''))
    return Space(
        slug=slug, title=env.get('KB_SPACE_TITLE', ''),
        persona=env.get('KB_PERSONA', '').strip() or _DEFAULT_PERSONA,
        hints=env.get('KB_HINTS', '').strip() or _DEFAULT_HINTS,
        chats=chats, answer=answer or {c: set() for c in chats},
        folder=env.get('DOWNLOAD_FOLDER', './downloads'), catalog='huawei',
        download_chats=_csv_ints(env.get('CHAT_IDS', '')),
        download_extensions=tuple(
            x.strip().lower().lstrip('.')
            for x in env.get('FILE_EXTENSIONS', 'pdf,jpg,png').split(',')
            if x.strip()),
        gaps_chat=int(env.get('KB_GAPS_CHAT_ID', '0') or 0),
        gaps_topic=int(env.get('KB_GAPS_TOPIC_ID', '0') or 0))


def load_spaces(path: str | None = None, env=None) -> Spaces:
    """spaces.toml, если он есть и не пуст, иначе одно неявное из env.

    Пустой файл и каталог вместо файла (docker создаёт папку под
    несуществующий bind-mount) означают «области не настроены»: одиночная
    установка не должна падать из-за примонтированной пустышки."""
    env = os.environ if env is None else env
    path = path or env.get('KB_SPACES_FILE', '') or 'spaces.toml'
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return Spaces([_implicit_space(env)])
    with open(path, 'rb') as f:
        data = tomllib.load(f)
    if not data:
        # файл есть, но в нём одни комментарии — тоже «не настроено»
        return Spaces([_implicit_space(env)])
    return Spaces([_space_from_table(slug, tbl) for slug, tbl in data.items()])


def _selftest() -> None:
    import tempfile

    # неявное пространство из env — как одиночная установка
    env = {'KB_CHAT_IDS': '-1001, -1002', 'KB_ANSWER_CHAT_IDS': '-1001:15,-1001:22',
           'DOWNLOAD_FOLDER': '/dl', 'CHAT_IDS': '-1001', 'FILE_EXTENSIONS': 'PDF, .zip'}
    sp = load_spaces(path=os.path.join(tempfile.gettempdir(), 'нет-такого.toml'),
                     env=env)
    s = sp.default
    assert s.slug == 'main' and sp.slugs == ('main',)
    assert s.chats == (-1001, -1002) and s.answer == {-1001: {15, 22}}
    assert s.catalog == 'huawei' and s.folder == '/dl'
    assert s.download_extensions == ('pdf', 'zip')
    assert sp.for_chat(-1002) is s and sp.for_chat(-1009) is None
    assert sp.answer_topics() == {-1001: {15, 22}}
    assert sp.parse_selector('#main вопрос') == ('main', 'вопрос')
    assert sp.parse_selector('вопрос про #s5735 и #huawei') == (
        None, 'вопрос про #s5735 и #huawei'), 'чужие хэштеги остаются'
    # без KB_ANSWER_CHAT_IDS отвечаем во всех чатах-источниках
    sp2 = load_spaces(path='/nonexistent/spaces.toml',
                      env={'KB_CHAT_IDS': '-1001', 'KB_SPACE': 'huawei'})
    assert sp2.default.slug == 'huawei' and sp2.default.answer == {-1001: set()}
    for bad in ('all', 'Huawei', 'b 4', ''):
        try:
            _implicit_space({'KB_SPACE': bad}) if bad else None
            assert bad == '', bad          # пустое = дефолт main, остальное — ошибка
        except ValueError:
            pass

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'spaces.toml')
        with open(path, 'w', encoding='utf-8') as f:
            f.write('''
[huawei]
title = "Huawei"
persona = "инженеров по оборудованию Huawei"
hints = "VRP, CloudEngine"
chats = [-1001]
answer = ["-1001:15", "-1001:22"]
folder = "/app/downloads"
catalog = "huawei"
download = { chats = [-1001], extensions = ["pdf", "HDX", ".zip"] }
gaps_chat = -1001
gaps_topic = 22

[b4]
title = "B4"
persona = "администраторов B4 и MikroTik"
chats = [-2001, -2002]
default_scope = "all"
''')
        sp = load_spaces(path=path)
        assert sp.slugs == ('huawei', 'b4') and sp.default.slug == 'huawei'
        hw, b4 = sp.get('huawei'), sp.get('b4')
        assert hw.answer == {-1001: {15, 22}} and hw.gaps_topic == 22
        assert hw.download_extensions == ('pdf', 'hdx', 'zip')
        assert b4.answer == {-2001: set(), -2002: set()}, 'answer по умолчанию = chats'
        assert b4.catalog == 'none' and b4.default_scope == 'all' and b4.folder == ''
        assert b4.persona.startswith('администраторов')
        assert sp.for_chat(-2002) is b4 and sp.for_chat(-1001) is hw
        assert sp.answer_topics() == {-1001: {15, 22}, -2001: set(), -2002: set()}
        # указатели: своё пространство, все, регистр, чужой хэштег, середина текста
        assert sp.parse_selector('#b4 как настроить') == ('b4', 'как настроить')
        assert sp.parse_selector('#ALL что такое VRP') == ('all', 'что такое VRP')
        assert sp.parse_selector('что там #b4 по mihomo?') == ('b4', 'что там по mihomo?')
        assert sp.parse_selector('#b4') == ('b4', '')
        assert sp.parse_selector('тег#b4 внутри слова') == (None, 'тег#b4 внутри слова')
        assert sp.parse_selector('  без указателя ') == (None, 'без указателя')
        assert sp.get('nope') is None

        # область поиска: указатель важнее чата, чат без привязки — везде
        sc, q = sp.resolve(-1001, 'как обновить R025')
        assert (sc.slug, sc.fallback, sc.explicit, q) == (
            'huawei', True, False, 'как обновить R025'), sc
        sc, q = sp.resolve(-1001, '#b4 а тут как')
        assert (sc.slug, sc.fallback, sc.explicit, q) == (
            'b4', False, True, 'а тут как'), sc
        sc, _ = sp.resolve(-1001, '#all что угодно')
        assert sc.slug is None and sc.explicit and not sc.fallback
        sc, _ = sp.resolve(777, 'вопрос из лички админа')
        assert sc.slug is None and not sc.explicit and sc.multi
        # default_scope='all' у b4: свой чат ищет сразу везде
        assert sp.resolve(-2001, 'вопрос')[0].slug is None
        assert sp.resolve(-1001, 'вопрос')[0].label == 'Huawei'
        assert sp.resolve(-1001, '#all x')[0].label == 'все области'

        # одиночная установка: всегда своё единственное пространство —
        # ни фолбэка, ни пометок, ни глобального скана
        one = load_spaces(path='/nonexistent.toml', env={'KB_CHAT_IDS': '-1001'})
        for text, chat in (('вопрос', -1001), ('#all вопрос', -1001),
                           ('вопрос', 777)):
            sc, _ = one.resolve(chat, text)
            assert sc.slug == 'main' and not sc.fallback and not sc.multi, (text, sc)
        # каталог вместо файла = файла нет
        os.mkdir(os.path.join(tmp, 'dir.toml'))
        assert load_spaces(path=os.path.join(tmp, 'dir.toml'),
                           env={}).default.slug == 'main'
        # кривые конфиги — понятные ошибки
        for body in ('[all]\nchats=[1]', '[Huawei]\nchats=[1]',
                     '[x]\ncatalog="mikrotik"', '[x]\ndefault_scope="везде"'):
            with open(path, 'w', encoding='utf-8') as f:
                f.write(body)
            try:
                load_spaces(path=path)
                raise AssertionError(f'ожидали ValueError: {body!r}')
            except ValueError:
                pass
        # пустой файл и файл из одних комментариев = «области не настроены»,
        # а не ошибка: docker монтирует пустышку, падать из-за неё нельзя
        for body in ('', '# только комментарий\n'):
            with open(path, 'w', encoding='utf-8') as f:
                f.write(body)
            assert load_spaces(path=path, env={}).default.slug == 'main'
    print('kb_spaces selftest: OK')


if __name__ == '__main__':
    _selftest()
