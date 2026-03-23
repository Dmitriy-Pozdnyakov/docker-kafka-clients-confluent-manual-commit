#!/usr/bin/env bash
set -euo pipefail

# cron_cdc.sh (Linux-only)
# -------------------------------------------
# Кратко как работает:
# 1) определяет режим запуска: producer | schema | pipeline;
# 2) пишет лог в state/cron-logs/<mode>-YYYY-MM-DD.log (или LOG_FILE);
# 3) берет lock через flock, чтобы не было параллельных запусков;
# 4) выполняет docker compose run --rm нужного сервиса(ов);
# 5) при занятом lock завершает работу с кодом 0 (тихий skip).
#
# Режимы:
# - producer: только oracle-producer-archivelog-sr
# - schema: только oracle-schema-build
# - pipeline: сначала schema-build, затем producer
#
# Linux requirement:
# - нужен flock (обычно пакет util-linux)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# MODE берется из первого аргумента. По умолчанию — producer.
MODE="${1:-producer}"
if [[ $# -gt 0 ]]; then
  # Сейчас доп. аргументы не используются, но shift оставлен,
  # чтобы в будущем проще было добавить positional параметры.
  shift
fi

case "${MODE}" in
  producer|schema|schema-build|pipeline)
    ;;
  *)
    echo "Usage: $0 [producer|schema|schema-build|pipeline]" >&2
    exit 1
    ;;
esac

# Каталог и файл логов для cron-запусков.
LOG_DIR="${LOG_DIR:-${PROJECT_DIR}/state/cron-logs}"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_FILE:-${LOG_DIR}/${MODE}-$(date +%F).log}"

# Один lock-файл на весь скрипт.
# Если нужен отдельный lock для разных задач — переопредели LOCK_FILE в cron.
LOCK_FILE="${LOCK_FILE:-/tmp/oracle-cdc.lock}"

now() {
  date '+%Y-%m-%d %H:%M:%S%z'
}

log() {
  echo "[$(now)] [cron-cdc:${MODE}] $*"
}

run_schema_build() {
  # env override-ы для schema-build:
  # - SCHEMA_OVERWRITE=yes  -> прокидываем OVERWRITE=yes
  # - SCHEMA_REGISTER_SR=yes -> прокидываем REGISTER_SR=yes
  # - SCHEMA_DSN=...        -> временно переопределяем ORACLE_DSN
  #
  # Важно:
  # - если SCHEMA_REGISTER_SR не задан, используется REGISTER_SR из env_file compose;
  # - schema-build реально запускается каждый раз в режимах schema/pipeline,
  #   но запись файлов может быть skipped внутри build-скрипта (если OVERWRITE пустой
  #   и файлы уже существуют).
  local env_args=()
  if [[ -n "${SCHEMA_OVERWRITE:-}" ]]; then
    env_args+=( -e "OVERWRITE=${SCHEMA_OVERWRITE}" )
  fi
  if [[ -n "${SCHEMA_REGISTER_SR:-}" ]]; then
    env_args+=( -e "REGISTER_SR=${SCHEMA_REGISTER_SR}" )
  fi
  if [[ -n "${SCHEMA_DSN:-}" ]]; then
    env_args+=( -e "ORACLE_DSN=${SCHEMA_DSN}" )
  fi

  log "run oracle-schema-build"
  docker compose run --rm "${env_args[@]}" oracle-schema-build
}

run_producer() {
  # PRODUCER_DSN позволяет разово переопределить ORACLE_DSN без правки env-файла.
  local env_args=()
  if [[ -n "${PRODUCER_DSN:-}" ]]; then
    env_args+=( -e "ORACLE_DSN=${PRODUCER_DSN}" )
  fi

  log "run oracle-producer-archivelog-sr"
  docker compose run --rm "${env_args[@]}" oracle-producer-archivelog-sr
}

# С этого места весь stdout/stderr уходит в LOG_FILE.
exec >>"${LOG_FILE}" 2>&1
cd "${PROJECT_DIR}"

if ! command -v flock >/dev/null 2>&1; then
  log "ERROR: flock is required (Linux util-linux package)"
  exit 1
fi

{
  flock -n 9 || {
    # Для cron это не считается ошибкой: просто не запускаем дубликат задачи.
    log "skip: lock is busy (${LOCK_FILE})"
    exit 0
  }

  log "start"
  case "${MODE}" in
    producer)
      run_producer
      ;;
    schema|schema-build)
      run_schema_build
      ;;
    pipeline)
      run_schema_build
      run_producer
      ;;
  esac
  log "done"
} 9>"${LOCK_FILE}"
