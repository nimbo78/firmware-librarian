"""Бенчмарк моделей эмбеддингов на РЕАЛЬНОМ корпусе базы знаний.

Методика (синтетический retrieval-бенчмарк):
1) сэмплируем N чанков из kb.sqlite;
2) LLM генерирует к каждому два вопроса: нормальный и сленговый/разговорный
   (кэшируются в JSON — повторные прогоны не платят за генерацию);
3) каждой моделью эмбеддим весь корпус и вопросы, считаем Recall@1/5/10 и
   MRR@10 — находит ли чистый векторный поиск исходный чанк.

ВАЖНО: прод — гибрид (вектор + FTS5 + RRF + query expansion), бенчмарк
меряет только векторную часть. Локальные модели получают свои префиксы
запрос/документ (BERTA — search_query/search_document), иначе сравнение
нечестное.

Запуск ЛОКАЛЬНО (не на NAS — torch на Celeron это часы; на CUDA-GPU
локальные модели считаются в fp16, минуты на весь корпус):
    # скопировать /volume1/docker/tg-kb/kb.sqlite в fromChat/kb.sqlite
    pip install openai numpy sentence-transformers
    pip install torch --index-url https://download.pytorch.org/whl/cu128
    python kb_bench.py --db fromChat/kb.sqlite --sample 100
Первый запуск скачает модели с HF (BGE-M3 ~2.2 ГБ, e5-large ~2.2 ГБ,
Qwen3-0.6B ~1.2 ГБ, BERTA ~0.5 ГБ).
Только API-модели: --models openai-small,openai-large
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import sys

QUESTION_PROMPT = (
    'Фрагмент обсуждения из технического чата про оборудование Huawei:\n'
    '---\n{text}\n---\n'
    'Сгенерируй два вопроса, ответ на которые содержится в этом фрагменте:\n'
    '1) "q" — обычный вопрос, как в поиске;\n'
    '2) "q_slang" — как спросил бы инженер в чате: разговорно, со сленгом/'
    'сокращениями («зеркалка», «капля», «прошивка», «пятитысячник» и т.п.), '
    'без точных цитат из фрагмента.\n'
    'Верни JSON {{"q": "...", "q_slang": "..."}}.'
)

MODELS = {
    'openai-small': {'kind': 'openai', 'model': 'text-embedding-3-small',
                     'dim': 512},
    'openai-large': {'kind': 'openai', 'model': 'text-embedding-3-large',
                     'dim': 1024},
    'bge-m3': {'kind': 'st', 'model': 'BAAI/bge-m3',
               'q_prefix': '', 'd_prefix': ''},
    'berta': {'kind': 'st', 'model': 'sergeyzh/BERTA',
              'q_prefix': 'search_query: ', 'd_prefix': 'search_document: '},
    'e5-large': {'kind': 'st', 'model': 'intfloat/multilingual-e5-large',
                 'q_prefix': 'query: ', 'd_prefix': 'passage: '},
    'qwen3-0.6b': {'kind': 'st', 'model': 'Qwen/Qwen3-Embedding-0.6B',
                   'q_prefix': 'Instruct: Given a web search query, retrieve '
                               'relevant passages that answer the query'
                               '\nQuery: ',
                   'd_prefix': ''},
}


def load_chunks(db_path: str, min_chars: int = 200) -> list[tuple[int, str]]:
    db = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    rows = db.execute('SELECT rowid, text FROM chunks').fetchall()
    db.close()
    return [(r, t) for r, t in rows if len(t) >= min_chars]


def gen_questions(chunks: list, sample_ids: list[int], cache_path: str) -> dict:
    """rowid -> {'q':…, 'q_slang':…}; кэш переживает повторные прогоны."""
    cache: dict = {}
    if os.path.exists(cache_path):
        with open(cache_path, encoding='utf-8') as f:
            cache = json.load(f)
    by_id = dict(chunks)
    from openai import OpenAI
    oa = OpenAI()
    model = os.getenv('ANSWER_MODEL', 'gpt-5-mini')
    todo = [rid for rid in sample_ids if str(rid) not in cache]
    for n, rid in enumerate(todo, 1):
        resp = oa.chat.completions.create(
            model=model, response_format={'type': 'json_object'},
            messages=[{'role': 'user', 'content':
                       QUESTION_PROMPT.format(text=by_id[rid][:2500])}])
        try:
            data = json.loads(resp.choices[0].message.content or '{}')
            if data.get('q') and data.get('q_slang'):
                cache[str(rid)] = {'q': data['q'], 'q_slang': data['q_slang']}
        except Exception as e:
            print(f'  вопрос для {rid} не сгенерировался: {e}')
        if n % 10 == 0:
            print(f'  вопросы: {n}/{len(todo)}', flush=True)
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False)
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False)
    return cache


def embed_openai(texts: list[str], model: str, dim: int, tag: str,
                 cache_dir: str):
    import numpy as np
    os.makedirs(cache_dir, exist_ok=True)
    cache = os.path.join(cache_dir, f'{tag}.npy')
    if os.path.exists(cache):
        arr = np.load(cache)
        if arr.shape[0] == len(texts):
            return arr
    from openai import OpenAI
    oa = OpenAI()
    out = []
    for i in range(0, len(texts), 96):
        batch = [t[:20000] for t in texts[i:i + 96]]
        resp = oa.embeddings.create(model=model, input=batch, dimensions=dim)
        out.extend(d.embedding for d in resp.data)
        print(f'  {tag}: {min(i + 96, len(texts))}/{len(texts)}', flush=True)
    arr = np.asarray(out, dtype='float32')
    np.save(cache, arr)
    return arr


def embed_st(texts: list[str], model_name: str, prefix: str, tag: str,
             cache_dir: str):
    import numpy as np
    os.makedirs(cache_dir, exist_ok=True)
    cache = os.path.join(cache_dir, f'{tag}.npy')
    if os.path.exists(cache):
        arr = np.load(cache)
        if arr.shape[0] == len(texts):
            return arr
    import torch
    from sentence_transformers import SentenceTransformer
    cuda = torch.cuda.is_available()
    st = SentenceTransformer(
        model_name, device='cuda' if cuda else 'cpu',
        model_kwargs={'torch_dtype': torch.float16} if cuda else {})
    # ~4000 симв. чанка ≈ 1000-1500 токенов; кап единый для всех моделей,
    # иначе 8k-контекст bge-m3/qwen3 взорвёт память на длинных батчах
    st.max_seq_length = min(st.max_seq_length or 2048, 2048)
    # prompt= вместо ручной конкатенации: явный prompt (даже пустой)
    # отключает default_prompt_name модели (у BERTA это 'Classification' —
    # иначе он приклеился бы ПОВЕРХ нашего префикса)
    arr = st.encode(list(texts), prompt=prefix,
                    batch_size=32 if cuda else 8,
                    show_progress_bar=True, normalize_embeddings=True)
    arr = np.asarray(arr, dtype='float32')
    np.save(cache, arr)
    del st
    if cuda:
        torch.cuda.empty_cache()
    return arr


def evaluate(corpus_emb, query_emb, gold_positions: list[int]) -> dict:
    """gold_positions[i] — индекс правильного чанка для i-го вопроса."""
    import numpy as np

    def _norm(a):
        return a / (np.linalg.norm(a, axis=1, keepdims=True) + 1e-9)

    corpus = _norm(corpus_emb)
    queries = _norm(query_emb)
    sims = queries @ corpus.T  # (Q, C)
    ranks = []
    for i, gold in enumerate(gold_positions):
        order = np.argsort(-sims[i])
        rank = int(np.where(order == gold)[0][0]) + 1
        ranks.append(rank)
    n = len(ranks)
    return {
        'R@1': sum(r <= 1 for r in ranks) / n,
        'R@5': sum(r <= 5 for r in ranks) / n,
        'R@10': sum(r <= 10 for r in ranks) / n,
        'MRR@10': sum(1 / r for r in ranks if r <= 10) / n,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--db', default='fromChat/kb.sqlite')
    parser.add_argument('--sample', type=int, default=100)
    parser.add_argument(
        '--models',
        default='openai-small,openai-large,bge-m3,berta,e5-large,qwen3-0.6b')
    parser.add_argument('--cache-dir', default='fromChat/bench_cache')
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f'Нет {args.db} — скопируй kb.sqlite с NAS '
                         f'(/volume1/docker/tg-kb/kb.sqlite)')
    os.makedirs(args.cache_dir, exist_ok=True)
    chunks = load_chunks(args.db)
    print(f'Чанков в корпусе (>=200 симв.): {len(chunks)}')
    random.seed(args.seed)
    sample_ids = [rid for rid, _ in
                  random.sample(chunks, min(args.sample, len(chunks)))]
    questions = gen_questions(chunks, sample_ids,
                              os.path.join(args.cache_dir, 'questions.json'))
    eval_ids = [rid for rid in sample_ids if str(rid) in questions]
    print(f'Вопросов готово: {len(eval_ids)} × 2 (обычный + сленг)')

    corpus_texts = [t for _, t in chunks]
    pos_by_id = {rid: i for i, (rid, _) in enumerate(chunks)}
    gold = [pos_by_id[rid] for rid in eval_ids]
    q_norm = [questions[str(rid)]['q'] for rid in eval_ids]
    q_slang = [questions[str(rid)]['q_slang'] for rid in eval_ids]

    results = {}
    for name in [m.strip() for m in args.models.split(',') if m.strip()]:
        cfg = MODELS[name]
        print(f'\n=== {name} ===')
        if cfg['kind'] == 'openai':
            corpus_emb = embed_openai(corpus_texts, cfg['model'], cfg['dim'],
                                      f'{name}-corpus', args.cache_dir)
            qn = embed_openai(q_norm, cfg['model'], cfg['dim'],
                              f'{name}-qnorm', args.cache_dir)
            qs = embed_openai(q_slang, cfg['model'], cfg['dim'],
                              f'{name}-qslang', args.cache_dir)
        else:
            corpus_emb = embed_st(corpus_texts, cfg['model'], cfg['d_prefix'],
                                  f'{name}-corpus', args.cache_dir)
            qn = embed_st(q_norm, cfg['model'], cfg['q_prefix'],
                          f'{name}-qnorm', args.cache_dir)
            qs = embed_st(q_slang, cfg['model'], cfg['q_prefix'],
                          f'{name}-qslang', args.cache_dir)
        results[name] = {'обычные': evaluate(corpus_emb, qn, gold),
                         'сленг': evaluate(corpus_emb, qs, gold)}

    print('\n' + '=' * 72)
    print(f'{"модель":16} {"вопросы":8} {"R@1":>7} {"R@5":>7} '
          f'{"R@10":>7} {"MRR@10":>8}')
    for name, sets in results.items():
        for flavor, m in sets.items():
            print(f'{name:16} {flavor:8} {m["R@1"]:7.3f} {m["R@5"]:7.3f} '
                  f'{m["R@10"]:7.3f} {m["MRR@10"]:8.3f}')
    print('\nПомни: прод — гибрид (вектор+FTS+RRF+expansion), здесь только '
          'векторная часть.')


def _selftest() -> None:
    import numpy as np
    # игрушечный корпус: вопрос 0 должен найти чанк 0, вопрос 1 — чанк 2
    corpus = np.asarray([[1, 0, 0], [0, 1, 0], [0, 0, 1]], dtype='float32')
    queries = np.asarray([[0.9, 0.1, 0], [0.1, 0, 0.9]], dtype='float32')
    m = evaluate(corpus, queries, [0, 2])
    assert m['R@1'] == 1.0 and m['MRR@10'] == 1.0, m
    m2 = evaluate(corpus, queries, [1, 1])  # ранги 2 и 3 -> MRR = 5/12
    assert m2['R@1'] == 0.0 and m2['R@5'] == 1.0, m2
    assert abs(m2['MRR@10'] - 5 / 12) < 1e-9, m2
    print('kb_bench selftest: OK')


if __name__ == '__main__':
    if '--selftest' in sys.argv:
        _selftest()
    else:
        main()
