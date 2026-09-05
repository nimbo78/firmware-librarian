#!/bin/sh
set -eu

# redeploy.sh — пересборка и запуск firmware-librarian.
# Запуск: ./redeploy.sh из любого каталога (cd делается автоматически).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f .env ]; then
    echo "ERROR: .env not found next to docker-compose.yml — compose не подтянет переменные" >&2
    echo "       cp .env.example .env  # и заполни значения" >&2
    exit 1
fi

# Пустой spaces.toml = «областей не настроено», всё берётся из .env.
# Файл обязан существовать: без него docker создаст на его месте КАТАЛОГ
# (bind-mount несуществующего пути) и намусорит в репозитории.
[ -e spaces.toml ] || : > spaces.toml

if [ ! -f bot.session ]; then
    echo "ERROR: bot.session not found — без неё контейнер заблокируется на запросе телефона" >&2
    exit 1
fi

echo "==> Stopping existing container"
docker compose down --remove-orphans

echo "==> Building and starting"
docker compose up --build -d

echo "==> Status"
docker compose ps

echo "==> Recent logs"
docker compose logs --tail=30
