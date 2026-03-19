#!/usr/bin/env python3
"""Oracle LogMiner -> Kafka prototype with Schema Registry CDC envelope.

Важно:
- Это отдельный ПРОТОТИП, стабильный скрипт не затрагивается.
- SR auth/ssl намеренно не включены (по запросу) — только URL.
"""

import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import oracledb
from confluent_kafka import Producer

from cdc_schema_registry_prototype import (
    SchemaRuntime,
    build_cdc_event,
    is_cdc_table_enabled,
    load_table_metadata,
    qualified_table_name,
    schema_registry_dependencies_available,
)


@dataclass
class Config:
    # Oracle
    oracle_user: str
    oracle_password: str
    oracle_dsn: str
    call_timeout_ms: int = 30000
    archive_log_limit: int = 3
    use_no_rowid_in_stmt: bool = True

    # Kafka
    kafka_broker: str = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"
    kafka_topic: str = "oracle.logminer.raw"
    kafka_security_protocol: str = "PLAINTEXT"
    ssl_cafile: str = ""
    ssl_check_hostname: bool = True
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    kafka_client_id: str = "oracle-logminer-archivelog-sr-prototype"
    kafka_flush_timeout_sec: int = 30
    produce_retry_timeout_sec: int = 60
    produce_retry_poll_sec: float = 0.2
    topic_per_table: bool = False
    topic_prefix: str = ""
    topic_separator: str = "."

    # State
    state_file: str = "./oracle_kafka_state_archivelog_sr_prototype.json"
    start_from_commit_scn: int = 0

    # Filters / batching
    filter_schema: str = ""
    filter_table: str = ""
    filter_schemas: Tuple[str, ...] = ()
    filter_tables: Tuple[str, ...] = ()
    max_rows_per_batch: int = 5000
    use_fetchmany: bool = True
    fetchmany_size: int = 1000

    # Prototype CDC + SR
    cdc_envelope_enabled: bool = False
    cdc_supported_tables: Tuple[str, ...] = ()  # OWNER.TABLE
    cdc_key_mode: str = "technical"  # technical | pk
    cdc_parse_error_mode: str = "raw"  # raw | fail
    schema_registry_url: str = "http://127.0.0.1:18081"
    schema_dir: str = "./schemas"
    schema_auto_register: bool = True
    schema_use_latest_version: bool = False
    schema_normalize: bool = True

    # Logging
    verbose: bool = True
    log_first_n_events: int = 3


def _log(cfg: Config, message: str) -> None:
    if cfg.verbose:
        print(f"[oracle->kafka:archivelog-sr-prototype] {message}")


def _str_to_bool(value: str, default: bool) -> bool:
    raw = value.strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_csv_upper(raw: str) -> List[str]:
    result: List[str] = []
    for item in raw.split(","):
        normalized = item.strip().upper()
        if normalized and normalized not in result:
            result.append(normalized)
    return result


def _merge_name_filters(single_value: str, many_values: Sequence[str]) -> List[str]:
    merged: List[str] = []
    single = str(single_value).strip().upper()
    if single:
        merged.append(single)
    for value in many_values:
        normalized = str(value).strip().upper()
        if normalized and normalized not in merged:
            merged.append(normalized)
    return merged


def load_config_from_env() -> Config:
    broker = os.getenv("KAFKA_BROKER", "").strip()
    if not broker:
        broker = os.getenv("BROKER", "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094").strip()

    return Config(
        oracle_user=os.getenv("ORACLE_USER", "").strip(),
        oracle_password=os.getenv("ORACLE_PASSWORD", "").strip(),
        oracle_dsn=os.getenv("ORACLE_DSN", "").strip(),
        call_timeout_ms=int(os.getenv("CALL_TIMEOUT_MS", "30000")),
        archive_log_limit=int(os.getenv("ARCHIVE_LOG_LIMIT", "3")),
        use_no_rowid_in_stmt=_str_to_bool(os.getenv("USE_NO_ROWID_IN_STMT", "true"), True),
        kafka_broker=broker,
        kafka_topic=os.getenv("KAFKA_TOPIC", "oracle.logminer.raw").strip(),
        kafka_security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper(),
        ssl_cafile=os.getenv("SSL_CAFILE", "").strip(),
        ssl_check_hostname=_str_to_bool(os.getenv("SSL_CHECK_HOSTNAME", "true"), True),
        kafka_sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN").strip(),
        kafka_sasl_username=os.getenv("KAFKA_SASL_USERNAME", "").strip(),
        kafka_sasl_password=os.getenv("KAFKA_SASL_PASSWORD", "").strip(),
        kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "oracle-logminer-archivelog-sr-prototype").strip(),
        kafka_flush_timeout_sec=int(os.getenv("KAFKA_FLUSH_TIMEOUT_SEC", "30")),
        produce_retry_timeout_sec=int(os.getenv("PRODUCE_RETRY_TIMEOUT_SEC", "60")),
        produce_retry_poll_sec=float(os.getenv("PRODUCE_RETRY_POLL_SEC", "0.2")),
        topic_per_table=_str_to_bool(os.getenv("TOPIC_PER_TABLE", "false"), False),
        topic_prefix=os.getenv("TOPIC_PREFIX", "").strip(),
        topic_separator=os.getenv("TOPIC_SEPARATOR", ".").strip() or ".",
        state_file=os.getenv("STATE_FILE", "./oracle_kafka_state_archivelog_sr_prototype.json").strip(),
        start_from_commit_scn=int(os.getenv("START_FROM_SCN", "0") or "0"),
        filter_schema=os.getenv("FILTER_SCHEMA", "").strip(),
        filter_table=os.getenv("FILTER_TABLE", "").strip(),
        filter_schemas=tuple(_parse_csv_upper(os.getenv("FILTER_SCHEMAS", ""))),
        filter_tables=tuple(_parse_csv_upper(os.getenv("FILTER_TABLES", ""))),
        max_rows_per_batch=int(os.getenv("MAX_ROWS_PER_BATCH", "5000")),
        use_fetchmany=_str_to_bool(os.getenv("USE_FETCHMANY", "true"), True),
        fetchmany_size=int(os.getenv("FETCHMANY_SIZE", "1000")),
        cdc_envelope_enabled=_str_to_bool(os.getenv("CDC_ENVELOPE_ENABLED", "false"), False),
        cdc_supported_tables=tuple(_parse_csv_upper(os.getenv("CDC_SUPPORTED_TABLES", ""))),
        cdc_key_mode=os.getenv("CDC_KEY_MODE", "technical").strip().lower(),
        cdc_parse_error_mode=os.getenv("CDC_PARSE_ERROR_MODE", "raw").strip().lower(),
        schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:18081").strip(),
        schema_dir=os.getenv("SCHEMA_DIR", "./schemas").strip(),
        schema_auto_register=_str_to_bool(os.getenv("SCHEMA_AUTO_REGISTER", "true"), True),
        schema_use_latest_version=_str_to_bool(os.getenv("SCHEMA_USE_LATEST_VERSION", "false"), False),
        schema_normalize=_str_to_bool(os.getenv("SCHEMA_NORMALIZE", "true"), True),
        verbose=_str_to_bool(os.getenv("VERBOSE", "true"), True),
        log_first_n_events=int(os.getenv("LOG_FIRST_N_EVENTS", "3")),
    )


def validate_config(cfg: Config) -> None:
    missing: List[str] = []
    if not cfg.oracle_user:
        missing.append("ORACLE_USER")
    if not cfg.oracle_password:
        missing.append("ORACLE_PASSWORD")
    if not cfg.oracle_dsn:
        missing.append("ORACLE_DSN")
    if not cfg.kafka_broker:
        missing.append("KAFKA_BROKER or BROKER")
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    if cfg.fetchmany_size <= 0:
        raise RuntimeError("FETCHMANY_SIZE must be > 0")
    if cfg.cdc_key_mode not in {"pk", "technical"}:
        raise RuntimeError("CDC_KEY_MODE must be one of: pk, technical")
    if cfg.cdc_parse_error_mode not in {"raw", "fail"}:
        raise RuntimeError("CDC_PARSE_ERROR_MODE must be one of: raw, fail")

    if cfg.cdc_envelope_enabled:
        if not schema_registry_dependencies_available():
            raise RuntimeError("Schema Registry dependencies are unavailable in confluent-kafka package")
        if not cfg.schema_registry_url:
            raise RuntimeError("SCHEMA_REGISTRY_URL is required when CDC_ENVELOPE_ENABLED=true")


def _load_state(cfg: Config) -> Dict[str, int]:
    path = Path(cfg.state_file)
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_commit_scn": cfg.start_from_commit_scn}


def _save_state(cfg: Config, last_commit_scn: int) -> None:
    path = Path(cfg.state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump({"last_commit_scn": last_commit_scn, "updated_at": datetime.now(timezone.utc).isoformat()}, f)


def _build_kafka_producer(cfg: Config) -> Producer:
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
    cur.execute("""
        BEGIN
          DBMS_LOGMNR.END_LOGMNR;
        EXCEPTION
          WHEN OTHERS THEN NULL;
        END;
    """)


def _get_archived_logs(cur: oracledb.Cursor, from_commit_scn: int, limit_rows: int) -> List[Dict[str, Any]]:
    def to_int_or_none(v: Any) -> Optional[int]:
        return None if v is None else int(v)

    def fetch_dict_rows(sql_text: str, binds: Dict[str, Any]) -> List[Dict[str, Any]]:
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

    order_by = " ORDER BY commit_scn, sequence#, rs_id, ssn"
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
    row = dict(zip(columns, record))
    ts = row.get("timestamp")
    if isinstance(ts, datetime):
        row["timestamp"] = ts.isoformat()
    return row


def _event_key(row: Dict[str, Any]) -> bytes:
    key = (
        f"{row.get('seg_owner','')}.{row.get('table_name','')}|"
        f"{row.get('commit_scn','')}|{row.get('redo_sequence','')}|"
        f"{row.get('rs_id','')}|{row.get('ssn','')}"
    )
    return key.encode("utf-8")


def _sanitize_topic_part(value: str) -> str:
    chars: List[str] = []
    for ch in str(value).strip().lower():
        chars.append(ch if (ch.isalnum() or ch in {"-", "_", "."}) else "_")
    out = "".join(chars).strip("._-")
    return out or "unknown"


def _resolve_topic(cfg: Config, row: Dict[str, Any]) -> str:
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
    key_obj, value_obj = build_cdc_event(row, table_meta, cfg)
    key_bytes = schema_runtime.serialize_key(topic, key_obj)
    value_bytes = schema_runtime.serialize_value(topic, value_obj)
    return _produce_with_backpressure(cfg, producer, topic, key_bytes, value_bytes, callback)


def run_once(cfg: Config) -> Dict[str, Any]:
    merged_schemas = _merge_name_filters(cfg.filter_schema, cfg.filter_schemas)
    merged_tables = _merge_name_filters(cfg.filter_table, cfg.filter_tables)

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
            nonlocal rows_count, queue_full_retries, queue_full_wait_sec
            nonlocal first_batch_commit_scn, last_batch_commit_scn
            nonlocal cdc_rows, raw_rows, cdc_fallback_raw

            row = _record_to_row(columns, record)
            rows_count += 1

            commit_scn = int(row["commit_scn"])
            if first_batch_commit_scn is None:
                first_batch_commit_scn = commit_scn
            last_batch_commit_scn = commit_scn

            topic = _resolve_topic(cfg, row)

            use_cdc = cfg.cdc_envelope_enabled and is_cdc_table_enabled(cfg, row)
            if use_cdc:
                table_meta = load_table_metadata(conn, str(row.get("seg_owner", "")), str(row.get("table_name", "")))
                try:
                    retries, waited = _produce_cdc_event(cfg, producer, topic, row, table_meta, schema_runtime, on_delivery)
                    cdc_rows += 1
                except Exception as exc:
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
            _log(cfg, f"fetch mode=fetchmany, fetchmany_size={cfg.fetchmany_size}")
            while True:
                records = cur.fetchmany(cfg.fetchmany_size)
                if not records:
                    break
                for record in records:
                    process_record(record)
        else:
            _log(cfg, "fetch mode=fetchall (legacy)")
            for record in cur.fetchall():
                process_record(record)

        _log(cfg, f"rows fetched from v$logmnr_contents: {rows_count}")

        producer.flush(cfg.kafka_flush_timeout_sec)
        _log(
            cfg,
            (
                "kafka flush done "
                f"(delivered={delivered}, failed={failed}, queued={rows_count}, "
                f"queue_full_retries={queue_full_retries}, "
                f"queue_full_wait_sec={queue_full_wait_sec:.3f})"
            ),
        )
        if failed > 0:
            raise RuntimeError(f"Kafka delivery failures: {failed} of {rows_count}")

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
    cfg = load_config_from_env()
    validate_config(cfg)

    _log(
        cfg,
        (
            "start one-shot prototype batch "
            f"(dsn={cfg.oracle_dsn}, topic={cfg.kafka_topic}, "
            f"archive_log_limit={cfg.archive_log_limit}, state_file={cfg.state_file})"
        ),
    )
    try:
        stats = run_once(cfg)
        _log(cfg, f"one-shot finished: {stats}")
        return 0
    except Exception as exc:
        print(f"[oracle->kafka:archivelog-sr-prototype] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
