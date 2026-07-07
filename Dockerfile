FROM python:3.12-slim

# tzdata: без него TZ не работает и INGEST_HOUR трактуется как UTC
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY download_telegram_files.py kb_store.py kb_ingest.py kb_backfill.py kb_bot.py kb_search.py ./
COPY bot.session bot.session

CMD ["python", "download_telegram_files.py"]
