FROM python:3.13-slim

# tzdata: без него TZ не работает и INGEST_HOUR трактуется как UTC
# unar: бэкенд rarfile для извлечения из RAR (включая RAR5; из main, не non-free)
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata unar \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements.txt
# gcc ставится только на время pip install (C-расширения зависимостей py7zr —
# pyppmd и т.п. — не всегда имеют готовые wheel под свежий Python) и вычищается
# в том же слое, чтобы не раздувать образ
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev \
    && pip install -r requirements.txt \
    && apt-get purge -y gcc libc6-dev \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY librarian.py tg_conn.py kb_store.py kb_ingest.py kb_render.py kb_answer.py kb_pipeline.py kb_firmware.py kb_extract.py kb_pdf.py kb_hedex.py kb_archive.py kb_backfill.py kb_bot.py kb_search.py kb_reembed.py kb_spaces.py kb_spaces_migrate.py kb_chats.py ./
# Проверка качества ответов на реальных вопросах из чатов
COPY kb_eval.py ./
# Селфтесты: не нужны для работы, но дают прогнать проверку на NAS перед
# долгим бэкфиллом (docker compose run --rm librarian python kb_backfill_check.py)
COPY kb_bot_check.py kb_spaces_check.py kb_backfill_check.py ./
COPY bot.session bot.session

CMD ["python", "librarian.py"]
