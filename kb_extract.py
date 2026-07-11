"""Фаза B каталога прошивок: LLM-обогащение.

1) Извлечение связок «файл → модель/версия» из подписей к файлам, у которых
   разбор имени (kb_firmware) ничего не дал — source='caption',
   confidence='medium'.
2) Таксономия серий: какой серии принадлежит модель (S5735-L → S5700) —
   знания модели без интернета, confidence через devices.confirmed=0.

Всё «medium»/неподтверждённое ждёт решения админа: /review в личке kb-bot.
Ошибки LLM не помечают файл обработанным — следующий прогон повторит попытку.
"""
from __future__ import annotations

import json
import logging
import os

from kb_firmware import parse_firmware_name

logger = logging.getLogger(__name__)


def _openai_client():
    # ленивый импорт: kb_ingest тянет telethon, а selftest должен
    # работать без установленных зависимостей
    from kb_ingest import openai_client
    return openai_client()

CALL_COST_ESTIMATE = 0.0003  # $ за вызов лёгкой модели, грубая оценка для событий
IMAGE_EXTS = ('.jpg', '.jpeg', '.png', '.gif', '.webp')

CAPTION_PROMPT = (
    'Файл из технического чата про оборудование Huawei.\n'
    'Имя файла: {name}\n'
    'Подпись к файлу: {caption}\n\n'
    'Если файл — прошивка/софт для конкретных моделей оборудования Huawei, '
    'верни JSON {{"items": [{{"model": "...", "version": "..."}}]}} '
    '(model — каноничное имя модели, например MA5608T или S5735-L; version — '
    'строка вида V800R018C10SPC500, пустая строка если версии нет). '
    'Если подписи нет — определяй только по имени файла. '
    'Если связка неочевидна или файл не прошивка — верни {{"items": []}}. '
    'Не выдумывай модели и версии, бери только явно указанное.'
)

SERIES_PROMPT = (
    'Для каждой модели оборудования Huawei укажи её серию/семейство '
    '(например S5735-L -> S5700, MA5608T -> MA5600T). '
    'Верни JSON {{"series": {{"<модель>": "<серия>"}}}}. '
    'Если серия неизвестна или модель не Huawei — пустая строка.\n'
    'Модели: {models}'
)


def _model_name() -> str:
    return os.getenv('ANSWER_MODEL', 'gpt-5-mini')


def _parse_json(text: str) -> dict:
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


async def extract_from_captions(store, limit: int = 200) -> tuple[int, float]:
    """Возвращает (добавлено связок, оценка стоимости $)."""
    rows = store.files_for_extraction(limit)
    added = 0
    calls = 0
    oa = _openai_client()
    for doc_id, name, caption in rows:
        if name.lower().endswith(IMAGE_EXTS):
            store.mark_file_extracted(doc_id)  # картинки — не прошивки
            continue
        try:
            resp = await oa.chat.completions.create(
                model=_model_name(),
                response_format={'type': 'json_object'},
                messages=[{'role': 'user', 'content': CAPTION_PROMPT.format(
                    name=name[:200], caption=caption[:500])}])
            calls += 1
        except Exception as e:
            logger.warning('caption extraction failed for %s: %s', doc_id, e)
            continue  # llm_done не ставим — попробуем в следующий прогон
        data = _parse_json(resp.choices[0].message.content or '')
        for item in data.get('items', []):
            if not isinstance(item, dict):
                continue
            model = str(item.get('model', '')).strip().upper()
            version = str(item.get('version', '')).strip().upper()
            if not model or len(model) > 40:
                continue
            version_key = parse_firmware_name(version)[2] if version else ''
            store.upsert_firmware(doc_id, model, version, version_key,
                                  source='caption', confidence='medium')
            added += 1
        store.mark_file_extracted(doc_id)
    return added, calls * CALL_COST_ESTIMATE


async def build_series(store, limit: int = 50) -> tuple[int, float]:
    """Таксономия серий для моделей, которых ещё нет в devices."""
    models = store.models_without_device(limit)
    if not models:
        return 0, 0.0
    oa = _openai_client()
    try:
        resp = await oa.chat.completions.create(
            model=_model_name(),
            response_format={'type': 'json_object'},
            messages=[{'role': 'user', 'content': SERIES_PROMPT.format(
                models=', '.join(models))}])
    except Exception as e:
        logger.warning('series taxonomy failed: %s', e)
        return 0, 0.0
    mapping = _parse_json(resp.choices[0].message.content or '').get('series', {})
    if not isinstance(mapping, dict):
        mapping = {}
    added = 0
    for model in models:
        series = str(mapping.get(model, '') or '').strip().upper()
        if series == model:
            series = ''  # модель «сама себе серия» — бесполезная связка
        # без серии — сразу confirmed: нечего подтверждать
        store.upsert_device(model, kind='model', parent=series,
                            confirmed=0 if series else 1)
        if series:
            store.upsert_device(series, kind='series', parent='', confirmed=1)
            added += 1
    return added, CALL_COST_ESTIMATE


async def run_extraction(store) -> tuple[int, int, float]:
    """Полный проход фазы B: (связок из подписей, серий, стоимость $)."""
    fw_added, cost1 = await extract_from_captions(store)
    dev_added, cost2 = await build_series(store)
    return fw_added, dev_added, cost1 + cost2


def _selftest() -> None:
    assert _parse_json('{"items": [{"model": "MA5608T"}]}')['items']
    assert _parse_json('мусор') == {}
    assert _parse_json('[1,2]') == {}
    assert 'photo.JPG'.lower().endswith(IMAGE_EXTS)
    assert not 'fw.zip'.lower().endswith(IMAGE_EXTS)
    print('kb_extract selftest: OK')


if __name__ == '__main__':
    _selftest()
