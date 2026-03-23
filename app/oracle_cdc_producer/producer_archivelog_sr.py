#!/usr/bin/env python3
"""Oracle LogMiner -> Kafka CDC + Schema Registry runner (one-shot).

Важно:
- Это отдельный сценарий, стабильный скрипт не затрагивается.
- Kafka auth/SSL поддержаны (PLAINTEXT/SSL/SASL_* как в стабильном скрипте).
- Для Schema Registry здесь оставлен только URL (без отдельного SR auth/SSL слоя).

Как читать этот файл:
1) В этом файле оставлен runtime-скелет (конфиг, подключение, batch-run, state).
2) Сложная CDC/SR логика вынесена в модуль `cdc_schema_registry.py`.
3) Файл рассчитан на one-shot запуск (удобно для cron/flock на VM).
"""

import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import oracledb
from confluent_kafka import Producer

try:
    from .cdc_schema_registry import (
        SchemaRuntime,
        build_cdc_event,
        is_cdc_table_enabled,
        load_table_metadata,
        qualified_table_name,
    )
    from .config import Config, load_config_from_env, validate_config
except ImportError:  # pragma: no cover
    from cdc_schema_registry import (
        SchemaRuntime,
        build_cdc_event,
        is_cdc_table_enabled,
        load_table_metadata,
        qualified_table_name,
    )
    from config import Config, load_config_from_env, validate_config


def _log(cfg: Config, message: str) -> None:
    if cfg.verbose:
        print(f"[oracle->kafka:archivelog-sr-cdc] {message}")


def _load_state(cfg: Config) -> Dict[str, Any]:
    """Читает watermark state; при первом запуске возвращает стартовый SCN из конфига."""
    path = Path(cfg.state_file)
    # Шаг 1: если state-файла еще нет, стартуем от START_FROM_SCN.
    if not path.exists():
        return {"last_commit_scn": cfg.start_from_commit_scn}

    try:
        # Шаг 2: читаем JSON state целиком.
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"STATE_FILE is not valid JSON: {path} ({exc})") from exc

    # Шаг 3: проверяем форму state-файла.
    if not isinstance(payload, dict):
        raise RuntimeError(f"STATE_FILE must contain JSON object, got: {type(payload).__name__}")
    if "last_commit_scn" not in payload:
        raise RuntimeError(f"STATE_FILE is missing 'last_commit_scn': {path}")

    try:
        # Шаг 4: нормализуем тип watermark к int, чтобы дальше не ловить сюрпризы.
        payload["last_commit_scn"] = int(payload.get("last_commit_scn"))
    except Exception as exc:
        raise RuntimeError(f"STATE_FILE has invalid 'last_commit_scn': {payload.get('last_commit_scn')!r}") from exc

    # Шаг 5: возвращаем валидированный state.
    return payload


def _fsync_directory(path: Path) -> None:
    """Пытается fsync каталога после os.replace для лучшей crash-consistency."""
    try:
        # Нужен fsync директории, чтобы операция rename была надежно зафиксирована на диске.
        dir_fd = os.open(str(path), os.O_RDONLY)
    except Exception:
        return
    try:
        os.fsync(dir_fd)
    except Exception:
        pass
    finally:
        os.close(dir_fd)


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    """Атомарно пишет JSON: tmp -> flush -> fsync -> os.replace."""
    # Шаг 1: гарантируем существование целевого каталога.
    path.parent.mkdir(parents=True, exist_ok=True)
    # Шаг 2: создаем временный файл в том же каталоге
    # (это важно для атомарного os.replace на одном файловом разделе).
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            # Шаг 3: пишем JSON payload во временный файл.
            json.dump(payload, f, ensure_ascii=False)
            f.write("\n")
            # Шаг 4: принудительно сбрасываем буферы Python и ОС.
            f.flush()
            os.fsync(f.fileno())
        # Шаг 5: атомарно заменяем старый state новым.
        os.replace(str(tmp_path), str(path))
        # Шаг 6: дополнительно fsync каталога после rename.
        _fsync_directory(path.parent)
    except Exception:
        try:
            # При любой ошибке удаляем temp-файл, чтобы не оставлять мусор.
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        raise


def _save_state(cfg: Config, last_commit_scn: int) -> None:
    """Сохраняет watermark после успешного batch атомарной записью."""
    path = Path(cfg.state_file)
    # Собираем компактный state и передаем его в единый атомарный writer.
    _atomic_write_json(
        path,
        {
            "last_commit_scn": int(last_commit_scn),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def _build_kafka_producer(cfg: Config) -> Producer:
    """Создает Kafka Producer под текущий security mode (PLAINTEXT/SSL/SASL_*)."""
    proto = cfg.kafka_security_protocol.upper()
    conf: Dict[str, Any] = {
        "bootstrap.servers": cfg.kafka_broker,
        "client.id": cfg.kafka_client_id,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 10,
        "security.protocol": proto,
    }
    if proto in {"SSL", "SASL_SSL"}:
        if cfg.ssl_cafile:
            conf["ssl.ca.location"] = cfg.ssl_cafile
        conf["ssl.endpoint.identification.algorithm"] = "https" if cfg.ssl_check_hostname else "none"
    if proto in {"SASL_SSL", "SASL_PLAINTEXT"}:
        conf["sasl.mechanism"] = cfg.kafka_sasl_mechanism
        conf["sasl.username"] = cfg.kafka_sasl_username
        conf["sasl.password"] = cfg.kafka_sasl_password
    return Producer(conf)


def _end_logminer(cur: oracledb.Cursor) -> None:
    """Безопасно завершает сессию LogMiner (ошибки глотаем на стороне PL/SQL блока)."""
    cur.execute("""
        BEGIN
          DBMS_LOGMNR.END_LOGMNR;
        EXCEPTION
          WHEN OTHERS THEN NULL;
        END;
    """)


def _get_archived_logs(cur: oracledb.Cursor, from_commit_scn: int, limit_rows: int) -> List[Dict[str, Any]]:
    """Подбирает archived logs под текущий watermark.

    Логика:
    - сначала ищем anchor-лог, перекрывающий from_commit_scn;
    - затем берем "хвост" по sequence, ограниченный ARCHIVE_LOG_LIMIT;
    - fallback для bootstrap (from_commit_scn <= 0): последние N логов.
    """
    def to_int_or_none(v: Any) -> Optional[int]:
        return None if v is None else int(v)

    def fetch_dict_rows(sql_text: str, binds: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Локальный helper: делает словари по именам колонок,
        # чтобы код ниже не был "индексным" и был проще читать/поддерживать.
        prev = cur.rowfactory
        try:
            cur.execute(sql_text, binds)
            cols = [c[0].lower() for c in cur.description]
            cur.rowfactory = lambda *args: dict(zip(cols, args))
            out: List[Dict[str, Any]] = []
            for row in cur.fetchall():
                out.append(
                    {
                        "thread": to_int_or_none(row.get("thread#")),
                        "sequence": to_int_or_none(row.get("sequence#")),
                        "first_change": to_int_or_none(row.get("first_change#")),
                        "next_change": to_int_or_none(row.get("next_change#")),
                        "name": row.get("name"),
                    }
                )
            return out
        finally:
            cur.rowfactory = prev

    sql = """
    WITH anchor AS (
      SELECT thread#, sequence#, first_change#, next_change#, name
      FROM (
        SELECT thread#, sequence#, first_change#, next_change#, name
        FROM v$archived_log
        WHERE name IS NOT NULL
          AND first_change# <= :from_scn
          AND (next_change# > :from_scn OR next_change# IS NULL)
        ORDER BY sequence# DESC
      )
      WHERE ROWNUM = 1
    ),
    tail AS (
      SELECT l.thread#, l.sequence#, l.first_change#, l.next_change#, l.name
      FROM v$archived_log l
      JOIN anchor a ON l.thread# = a.thread# AND l.sequence# >= a.sequence#
      WHERE l.name IS NOT NULL
      ORDER BY l.sequence# ASC
    )
    SELECT thread#, sequence#, first_change#, next_change#, name
    FROM (SELECT thread#, sequence#, first_change#, next_change#, name FROM tail)
    WHERE ROWNUM <= :limit_rows
    """
    rows = fetch_dict_rows(sql, {"from_scn": int(from_commit_scn), "limit_rows": int(limit_rows)})
    if rows:
        return rows

    if int(from_commit_scn) <= 0:
        bootstrap_sql = """
        SELECT thread#, sequence#, first_change#, next_change#, name
        FROM (
          SELECT thread#, sequence#, first_change#, next_change#, name
          FROM v$archived_log
          WHERE name IS NOT NULL
          ORDER BY sequence# DESC
        )
        WHERE ROWNUM <= :limit_rows
        """
        return list(reversed(fetch_dict_rows(bootstrap_sql, {"limit_rows": int(limit_rows)})))

    raise RuntimeError(
        "Cannot locate archived logs for state SCN "
        f"{from_commit_scn}. Required SCN range is not available in v$archived_log."
    )


def _add_logfile(cur: oracledb.Cursor, logfile: str, first: bool) -> None:
    """Добавляет redo-файл в LogMiner (NEW для первого, ADDFILE для остальных)."""
    option = "DBMS_LOGMNR.NEW" if first else "DBMS_LOGMNR.ADDFILE"
    cur.execute(
        f"""
        BEGIN
          DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :logfile, OPTIONS => {option});
        END;
        """,
        {"logfile": logfile},
    )


def _start_logminer(cur: oracledb.Cursor, cfg: Config, log_files: List[str]) -> None:
    """Стартует LogMiner на выбранном наборе archived logs."""
    if not log_files:
        raise RuntimeError("No archived logs found in v$archived_log")
    _end_logminer(cur)
    for i, lf in enumerate(log_files):
        _add_logfile(cur, lf, first=(i == 0))

    options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
    if cfg.use_no_rowid_in_stmt:
        options += " + DBMS_LOGMNR.NO_ROWID_IN_STMT"

    cur.call_timeout = cfg.call_timeout_ms
    cur.execute(f"""
      BEGIN
        DBMS_LOGMNR.START_LOGMNR(OPTIONS => {options});
      END;
    """)


def _build_fetch_rows_query(
    from_commit_scn: int,
    filter_schemas: Sequence[str],
    filter_tables: Sequence[str],
    max_rows_per_batch: int,
) -> Tuple[str, Dict[str, Any]]:
    """Строит SQL для чтения DML из v$logmnr_contents.

    Важные моменты:
    - работаем только с commit_scn IS NOT NULL;
    - max_rows_per_batch ограничивает с добором целого commit_scn (без разрыва commit внутри батча);
    - фильтры схем/таблиц добавляются только если заданы в конфиге.
    """
    base_sql = """
    SELECT
      commit_scn, scn, timestamp, seg_owner, table_name,
      operation, operation_code, sequence# AS redo_sequence,
      rs_id, ssn, sql_redo, sql_undo
    FROM v$logmnr_contents
    WHERE operation_code IN (1, 2, 3)
      AND commit_scn IS NOT NULL
      AND commit_scn > :from_commit_scn
    """
    binds: Dict[str, Any] = {"from_commit_scn": from_commit_scn}

    if filter_schemas:
        ph: List[str] = []
        for i, value in enumerate(filter_schemas):
            key = f"filter_schema_{i}"
            ph.append(f":{key}")
            binds[key] = value
        base_sql += f" AND seg_owner IN ({', '.join(ph)})"

    if filter_tables:
        ph = []
        for i, value in enumerate(filter_tables):
            key = f"filter_table_{i}"
            ph.append(f":{key}")
            binds[key] = value
        base_sql += f" AND table_name IN ({', '.join(ph)})"

    order_by = " ORDER BY commit_scn, redo_sequence, rs_id, ssn"
    if max_rows_per_batch > 0:
        sql = f"""
        WITH base_rows AS ({base_sql}),
        ranked_rows AS (
          SELECT b.*, ROW_NUMBER() OVER (ORDER BY commit_scn, redo_sequence, rs_id, ssn) AS rn
          FROM base_rows b
        ),
        cutoff AS (
          SELECT MAX(commit_scn) AS cutoff_commit_scn
          FROM ranked_rows
          WHERE rn <= :max_rows_per_batch
        )
        SELECT
          r.commit_scn, r.scn, r.timestamp, r.seg_owner, r.table_name,
          r.operation, r.operation_code, r.redo_sequence,
          r.rs_id, r.ssn, r.sql_redo, r.sql_undo
        FROM ranked_rows r
        CROSS JOIN cutoff c
        WHERE c.cutoff_commit_scn IS NOT NULL
          AND r.commit_scn <= c.cutoff_commit_scn
        {order_by}
        """
        binds["max_rows_per_batch"] = int(max_rows_per_batch)
    else:
        sql = f"{base_sql}{order_by}"
    return sql, binds


def _record_to_row(columns: Sequence[str], record: Sequence[Any]) -> Dict[str, Any]:
    """Преобразует tuple-record из Oracle в dict и нормализует timestamp."""
    row = dict(zip(columns, record))
    ts = row.get("timestamp")
    if isinstance(ts, datetime):
        row["timestamp"] = ts.isoformat()
    return row


def _event_key(row: Dict[str, Any]) -> bytes:
    """Формирует детерминированный технический ключ для raw-события."""
    key = (
        f"{row.get('seg_owner','')}.{row.get('table_name','')}|"
        f"{row.get('commit_scn','')}|{row.get('redo_sequence','')}|"
        f"{row.get('rs_id','')}|{row.get('ssn','')}"
    )
    return key.encode("utf-8")


def _sanitize_topic_part(value: str) -> str:
    """Санитизирует часть topic (оставляет alnum, '-', '_' и '.')."""
    chars: List[str] = []
    for ch in str(value).strip().lower():
        chars.append(ch if (ch.isalnum() or ch in {"-", "_", "."}) else "_")
    out = "".join(chars).strip("._-")
    return out or "unknown"


def _resolve_topic(cfg: Config, row: Dict[str, Any]) -> str:
    """Определяет topic назначения: single-topic или topic-per-table."""
    if not cfg.topic_per_table:
        return cfg.kafka_topic
    schema_part = _sanitize_topic_part(str(row.get("seg_owner", "")))
    table_part = _sanitize_topic_part(str(row.get("table_name", "")))
    parts: List[str] = []
    prefix = _sanitize_topic_part(cfg.topic_prefix) if cfg.topic_prefix else ""
    if prefix:
        parts.append(prefix)
    parts.extend([schema_part, table_part])
    return cfg.topic_separator.join(parts)


def _produce_with_backpressure(
    cfg: Config,
    producer: Producer,
    topic: str,
    key: bytes,
    value: bytes,
    callback,
) -> Tuple[int, float]:
    """Публикует сообщение в Kafka с ретраями при BufferError (локальная очередь producer заполнена)."""
    retries = 0
    waited = 0.0
    started = time.monotonic()
    deadline = started + max(float(cfg.produce_retry_timeout_sec), 0.0)

    while True:
        try:
            producer.produce(topic, key=key, value=value, callback=callback)
            return retries, waited
        except BufferError:
            now = time.monotonic()
            if now >= deadline:
                raise RuntimeError(
                    "Kafka local queue remained full until retry timeout "
                    f"({cfg.produce_retry_timeout_sec}s)"
                ) from None
            producer.poll(cfg.produce_retry_poll_sec)
            retries += 1
            waited = now - started


def _produce_raw_event(cfg: Config, producer: Producer, topic: str, row: Dict[str, Any], callback) -> Tuple[int, float]:
    """Путь raw-публикации: отправляем JSON-слепок строки LogMiner как есть."""
    value = json.dumps(row, ensure_ascii=False).encode("utf-8")
    return _produce_with_backpressure(cfg, producer, topic, _event_key(row), value, callback)


def _produce_cdc_event(
    cfg: Config,
    producer: Producer,
    topic: str,
    row: Dict[str, Any],
    table_meta: Dict[str, Any],
    schema_runtime: SchemaRuntime,
    callback,
) -> Tuple[int, float]:
    """Путь CDC-публикации: строим envelope + сериализуем через Schema Registry."""
    key_obj, value_obj = build_cdc_event(row, table_meta, cfg)
    key_bytes = schema_runtime.serialize_key(topic, key_obj)
    value_bytes = schema_runtime.serialize_value(topic, value_obj)
    return _produce_with_backpressure(cfg, producer, topic, key_bytes, value_bytes, callback)


def run_once(cfg: Config) -> Dict[str, Any]:
    """Один one-shot batch.

    Шаги:
    1) читаем state (последний commit_scn),
    2) подбираем archived logs и запускаем LogMiner,
    3) читаем DML, публикуем в Kafka (raw/CDC),
    4) flush + проверка delivery,
    5) обновляем state только при успешной доставке.
    """
    merged_schemas = list(cfg.filter_schemas)
    merged_tables = list(cfg.filter_tables)

    state = _load_state(cfg)
    last_commit_scn = int(state.get("last_commit_scn", cfg.start_from_commit_scn))

    _log(
        cfg,
        (
            "start batch "
            f"(topic_mode={'per_table' if cfg.topic_per_table else 'single'}, "
            f"base_topic={cfg.kafka_topic}, from_commit_scn={last_commit_scn}, "
            f"filter_schemas={','.join(merged_schemas) if merged_schemas else '*'}, "
            f"filter_tables={','.join(merged_tables) if merged_tables else '*'}, "
            f"max_rows_per_batch={cfg.max_rows_per_batch}, "
            f"use_fetchmany={cfg.use_fetchmany}, fetchmany_size={cfg.fetchmany_size}, "
            f"cdc_envelope_enabled={cfg.cdc_envelope_enabled})"
        ),
    )

    producer = _build_kafka_producer(cfg)
    schema_runtime = SchemaRuntime(cfg)

    delivered = 0
    failed = 0
    queue_full_retries = 0
    queue_full_wait_sec = 0.0
    rows_count = 0
    topics_used: Dict[str, int] = {}
    cdc_rows = 0
    raw_rows = 0
    cdc_fallback_raw = 0

    def on_delivery(err, _msg):
        nonlocal delivered, failed
        if err is not None:
            failed += 1
        else:
            delivered += 1

    conn: Optional[oracledb.Connection] = None
    cur: Optional[oracledb.Cursor] = None
    try:
        conn = oracledb.connect(user=cfg.oracle_user, password=cfg.oracle_password, dsn=cfg.oracle_dsn)
        cur = conn.cursor()

        archived_logs = _get_archived_logs(cur, last_commit_scn, cfg.archive_log_limit)
        logs = [str(item["name"]) for item in archived_logs if item.get("name")]
        for item in archived_logs:
            _log(
                cfg,
                (
                    "  logfile: "
                    f"name={item.get('name')} "
                    f"sequence={item.get('sequence')} "
                    f"first_change={item.get('first_change')} "
                    f"next_change={item.get('next_change')}"
                ),
            )

        _start_logminer(cur, cfg, logs)

        sql, binds = _build_fetch_rows_query(
            from_commit_scn=last_commit_scn,
            filter_schemas=merged_schemas,
            filter_tables=merged_tables,
            max_rows_per_batch=cfg.max_rows_per_batch,
        )

        cur.execute(sql, binds)
        columns = [c[0].lower() for c in cur.description]

        first_batch_commit_scn: Optional[int] = None
        last_batch_commit_scn: Optional[int] = None

        def process_record(record: Sequence[Any]) -> None:
            # Обработка одной строки LogMiner:
            # - нормализация строки,
            # - выбор topic,
            # - выбор режима публикации raw/CDC,
            # - backpressure-safe produce.
            #
            # Зачем здесь nonlocal:
            # process_record() вложена в run_once(), а счетчики/агрегаты
            # объявлены уровнем выше (в run_once()).
            # nonlocal позволяет обновлять именно эти внешние переменные,
            # а не создавать новые локальные внутри process_record().
            nonlocal rows_count, queue_full_retries, queue_full_wait_sec
            nonlocal first_batch_commit_scn, last_batch_commit_scn
            nonlocal cdc_rows, raw_rows, cdc_fallback_raw

            row = _record_to_row(columns, record)
            rows_count += 1

            # Фиксируем границы commit_scn внутри текущего батча.
            commit_scn = int(row["commit_scn"])
            if first_batch_commit_scn is None:
                first_batch_commit_scn = commit_scn
            last_batch_commit_scn = commit_scn

            # Для topic-per-table формируется отдельный topic на таблицу,
            # иначе используем общий cfg.kafka_topic.
            topic = _resolve_topic(cfg, row)

            # Ветка отправки:
            # - CDC path (Schema Registry + envelope) если включен и таблица разрешена.
            # - Иначе raw path (JSON-слепок строки LogMiner).
            use_cdc = cfg.cdc_envelope_enabled and is_cdc_table_enabled(cfg, row)
            if use_cdc:
                table_meta = load_table_metadata(conn, str(row.get("seg_owner", "")), str(row.get("table_name", "")))
                try:
                    retries, waited = _produce_cdc_event(cfg, producer, topic, row, table_meta, schema_runtime, on_delivery)
                    cdc_rows += 1
                except Exception as exc:
                    # Для прототипа допускаем fallback в raw, чтобы батч не падал
                    # на неподдержанном SQL-шаблоне.
                    if cfg.cdc_parse_error_mode == "raw":
                        _log(cfg, f"cdc parser fallback -> raw for {qualified_table_name(row)}: {exc}")
                        retries, waited = _produce_raw_event(cfg, producer, topic, row, on_delivery)
                        raw_rows += 1
                        cdc_fallback_raw += 1
                    else:
                        raise
            else:
                retries, waited = _produce_raw_event(cfg, producer, topic, row, on_delivery)
                raw_rows += 1

            queue_full_retries += retries
            queue_full_wait_sec += waited
            topics_used[topic] = topics_used.get(topic, 0) + 1
            # poll(0) запускает обработку delivery callbacks без блокировки.
            producer.poll(0)

            if rows_count <= cfg.log_first_n_events:
                _log(
                    cfg,
                    (
                        "queued event "
                        f"topic={topic} commit_scn={row.get('commit_scn')} "
                        f"schema={row.get('seg_owner')} table={row.get('table_name')} "
                        f"operation={row.get('operation')}"
                    ),
                )

        if cfg.use_fetchmany:
            # Рекомендуемый режим:
            # читаем Oracle чанками (меньше пиковая память, стабильнее на больших батчах).
            _log(cfg, f"fetch mode=fetchmany, fetchmany_size={cfg.fetchmany_size}")
            while True:
                records = cur.fetchmany(cfg.fetchmany_size)
                if not records:
                    break
                for record in records:
                    process_record(record)
        else:
            # Legacy-режим: забираем все строки сразу.
            _log(cfg, "fetch mode=fetchall (compat)")
            for record in cur.fetchall():
                process_record(record)

        _log(cfg, f"rows fetched from v$logmnr_contents: {rows_count}")

        # ВАЖНО: flush возвращает количество сообщений, оставшихся в локальной очереди.
        # Для one-shot сценария это критичный сигнал: если > 0, подтверждать SCN нельзя.
        remaining_after_flush = producer.flush(cfg.kafka_flush_timeout_sec)
        _log(
            cfg,
            (
                "kafka flush done "
                f"(delivered={delivered}, failed={failed}, queued={rows_count}, "
                f"remaining_after_flush={remaining_after_flush}, "
                f"queue_full_retries={queue_full_retries}, "
                f"queue_full_wait_sec={queue_full_wait_sec:.3f})"
            ),
        )
        if failed > 0:
            raise RuntimeError(f"Kafka delivery failures: {failed} of {rows_count}")
        if remaining_after_flush > 0:
            raise RuntimeError(
                "Kafka flush timeout: "
                f"{remaining_after_flush} messages left in producer queue "
                f"after {cfg.kafka_flush_timeout_sec}s"
            )

        # Обновляем state только после успешного flush и при отсутствии failed.
        # Это защищает от потери данных при перезапуске one-shot джобы.
        if rows_count > 0:
            last_commit_scn = max(last_commit_scn, int(last_batch_commit_scn))
            _save_state(cfg, last_commit_scn)
            _log(cfg, f"state updated: last_commit_scn={last_commit_scn}")
        else:
            _log(cfg, "no new rows, state not changed")

        result = {
            "rows": rows_count,
            "delivered": delivered,
            "failed": failed,
            "remaining_after_flush": remaining_after_flush,
            "queue_full_retries": queue_full_retries,
            "queue_full_wait_sec": round(queue_full_wait_sec, 3),
            "last_commit_scn": last_commit_scn,
            "topics_used": topics_used,
            "cdc_rows": cdc_rows,
            "raw_rows": raw_rows,
            "cdc_fallback_raw": cdc_fallback_raw,
            "first_batch_commit_scn": first_batch_commit_scn,
            "last_batch_commit_scn": last_batch_commit_scn,
        }
        _log(cfg, f"batch finished: {result}")
        return result
    finally:
        try:
            if cur is not None:
                try:
                    _end_logminer(cur)
                    _log(cfg, "LogMiner ended")
                except Exception:
                    pass
                cur.close()
        finally:
            if conn is not None:
                conn.close()
                _log(cfg, "Oracle connection closed")
            producer.flush(cfg.kafka_flush_timeout_sec)


def main() -> int:
    """CLI-entrypoint: один запуск -> код 0/1 для cron/monitoring."""
    cfg = load_config_from_env()
    validate_config(cfg)

    _log(
        cfg,
        (
            "start one-shot sr-cdc batch "
            f"(dsn={cfg.oracle_dsn}, topic={cfg.kafka_topic}, "
            f"archive_log_limit={cfg.archive_log_limit}, state_file={cfg.state_file})"
        ),
    )
    try:
        stats = run_once(cfg)
        _log(cfg, f"one-shot finished: {stats}")
        return 0
    except Exception as exc:
        print(f"[oracle->kafka:archivelog-sr-cdc] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
