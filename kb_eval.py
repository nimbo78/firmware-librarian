"""Проверка релевантности ответов на реальных вопросах из чатов.

Берёт вопросы не выдуманные, а прямо из проинжещенной истории области:
только они показывают, как бот справится с настоящим жаргоном и
недосказанностью, с которыми к нему придут люди.

    # что вообще легло в базу (бесплатно, без сети)
    docker compose run --rm librarian python kb_eval.py --stats

    # 10 случайных реальных вопросов: только поиск, без LLM (почти бесплатно)
    docker compose run --rm librarian python kb_eval.py --space b4 --retrieval

    # то же, но с полными ответами бота (платно, ~$0.01 за вопрос)
    docker compose run --rm librarian python kb_eval.py --space b4 --answers

    # свои вопросы из файла (по одному на строку)
    docker compose run --rm librarian python kb_eval.py --space b4 --answers \
        --file /app/downloads/questions.txt

Качалку останавливать не нужно: Telegram не используется, только база.
"""
from __future__ import annotations

import argparse
import asyncio
import random
import re
import sys

from kb_spaces import Scope, load_spaces
from kb_store import open_store

# Вопрос из чата годится в выборку, если он похож на самостоятельный:
# «а у тебя как?» проверять бессмысленно — без контекста беседы на него
# не ответит и человек.
MIN_QUESTION_CHARS = 25
MAX_QUESTION_CHARS = 200
_LINE_RE = re.compile(r'^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}\] [^:]{1,40}: (.+)$')
# местоимения без антецедента — верный признак вопроса-продолжения
_CONTEXTUAL = re.compile(
    r'^(а|и|но|ну|это|оно|там|тут|тогда|значит)\b|\b(его|её|их|этот|эта|это|'
    r'том|таком|туда|оттуда)\b', re.IGNORECASE)


def sample_questions(store, space: str, limit: int, seed: int = 0) -> list[str]:
    """Реальные вопросы из истории области: строки чанков, кончающиеся «?»."""
    rows = store.db.execute(
        'SELECT text FROM chunks WHERE space = ? AND chat_id < 0', (space,)
    ).fetchall()
    found: list[str] = []
    for (text,) in rows:
        for line in text.split('\n'):
            m = _LINE_RE.match(line.strip())
            if not m:
                continue
            q = m.group(1).strip()
            if not q.endswith('?'):
                continue
            if not (MIN_QUESTION_CHARS <= len(q) <= MAX_QUESTION_CHARS):
                continue
            if _CONTEXTUAL.search(q):
                continue
            found.append(q)
    rnd = random.Random(seed)          # повторяемая выборка: сравнимо между прогонами
    rnd.shuffle(found)
    return found[:limit]


def show_stats(store, spaces) -> None:
    """Что легло в базу: по областям, чатам и датам. Ни сети, ни денег."""
    by_space = dict(store.count_by_space())
    print(f'Всего фрагментов: {store.count()}\n')
    for sp in spaces.all:
        n = by_space.get(sp.slug, 0)
        mark = '' if n else '   ← ПУСТО: инжест не доехал'
        print(f'#{sp.slug} ({sp.label}): {n} фрагментов{mark}')
        rows = store.db.execute(
            'SELECT chat_id, count(*), min(date_from), max(date_to) FROM chunks '
            'WHERE space = ? GROUP BY chat_id ORDER BY 2 DESC', (sp.slug,)
        ).fetchall()
        for chat_id, cnt, d1, d2 in rows:
            kind = 'чат' if chat_id < 0 else 'документы'
            print(f'    {kind} {chat_id}: {cnt} фрагментов, {d1} — {d2}')
        media = store.db.execute(
            "SELECT count(*) FROM chunks WHERE space = ? AND text LIKE '%[изображение:%'",
            (sp.slug,)).fetchone()[0]
        if media:
            print(f'    из них с описаниями картинок: {media}')
    unknown = set(by_space) - {s.slug for s in spaces.all}
    for slug in sorted(unknown):
        print(f'#{slug or "(пусто)"}: {by_space[slug]} фрагментов '
              f'— области нет в spaces.toml!')


async def run(store, spaces, slug: str, questions: list[str],
              answers: bool) -> None:
    from kb_answer import answer_question, search_expanded

    space = spaces.get(slug)
    if space is None:
        raise SystemExit(f'нет области «{slug}»; есть: ' + ', '.join(spaces.slugs))
    # та же область поиска, что даёт указатель «#slug вопрос» в чате
    scope = Scope(space, explicit=True, multi=len(spaces.all) > 1)
    for i, q in enumerate(questions, 1):
        print(f'\n{"=" * 70}\n[{i}/{len(questions)}] {q}')
        if answers:
            text, found = await answer_question(store, q, scope=scope)
            print(f'\n{text}')
            if not found:
                print('\n⚠️  found=False — в базе не нашлось (попадёт в /gaps)')
        else:
            hits, _ = await search_expanded(store, q, scope=scope)
            if not hits:
                print('  ✗ ничего не найдено')
                continue
            for j, h in enumerate(hits[:4], 1):
                where = h.topic_name or h.date_from
                preview = ' | '.join(h.text.split('\n')[1:3])[:180]
                print(f'  {j}. [{h.space}] {where} {h.date_from}\n     {preview}')


async def main() -> None:
    ap = argparse.ArgumentParser(description='Проверка качества ответов')
    ap.add_argument('--stats', action='store_true',
                    help='что легло в базу по областям (без сети и без денег)')
    ap.add_argument('--space', default='', metavar='SLUG')
    ap.add_argument('--retrieval', action='store_true',
                    help='только поиск: какие фрагменты находятся (без LLM)')
    ap.add_argument('--answers', action='store_true',
                    help='полные ответы бота (~$0.01 за вопрос)')
    ap.add_argument('-n', type=int, default=10, help='сколько вопросов (по умолчанию 10)')
    ap.add_argument('--seed', type=int, default=0, help='другая выборка вопросов')
    ap.add_argument('--file', default='', metavar='PATH',
                    help='свои вопросы вместо выбранных из чата, по одному на строку')
    args = ap.parse_args()

    store = open_store()
    spaces = load_spaces()
    if args.stats or not (args.retrieval or args.answers):
        show_stats(store, spaces)
        if not (args.retrieval or args.answers):
            return
    if not args.space:
        raise SystemExit('укажи --space <slug>')

    if args.file:
        with open(args.file, encoding='utf-8') as f:
            questions = [ln.strip() for ln in f if ln.strip()][:args.n]
        source = f'из файла {args.file}'
    else:
        questions = sample_questions(store, args.space, args.n, args.seed)
        source = 'реальные вопросы из истории чата'
    if not questions:
        raise SystemExit('вопросов не нашлось — попробуй --seed 1 или --file')
    print(f'Область #{args.space}, {len(questions)} вопросов ({source})')
    if args.answers:
        print(f'Оценка расходов: ~${0.01 * len(questions):.2f}\n')
    await run(store, spaces, args.space, questions, args.answers)
    print(f'\n{"=" * 70}\nГотово. Оцени сам: ответ по делу? источники те самые?')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\nПрервано.')
        sys.exit(130)
