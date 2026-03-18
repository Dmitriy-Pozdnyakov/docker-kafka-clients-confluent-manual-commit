#!/usr/bin/env python3
"""
Jupyter-friendly Oracle LogMiner -> Kafka через Schema Registry (JSON Schema, без Avro).

Этот вариант подготовлен под сценарий "несколько (например 5) приоритетных таблиц":
- для выбранных таблиц можно подключать table-specific parser;
- для остальных таблиц можно либо отправлять raw envelope, либо пропускать (strict_tracked_only=True).
"""

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import oracledb
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


@dataclass
class Config:
    # Oracle
    oracle_user: str
    oracle_password: str
    oracle_dsn: str
    call_timeout_ms: int = 30000
    archive_log_limit: int = 3

    # Kafka
    kafka_broker: str = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"
    kafka_topic: str = "oracle.logminer.raw"
    kafka_client_id: str = "oracle-logminer-jupyter-sr-json"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    ssl_cafile: str = ""
    ssl_check_hostname: bool = True
    kafka_flush_timeout_sec: int = 30

    # Schema Registry (JSON)
    schema_registry_url: str = "http://127.0.0.1:18081"
    schema_registry_basic_auth_user_info: str = ""  # формат user:password

    # State
    state_file: str = "./oracle_kafka_state_jupyter_sr_json.json"
    start_from_commit_scn: int = 0

    # Loop
    poll_seconds: int = 60

    # Filters
    filter_schemas: Tuple[str, ...] = ()
    filter_tables: Tuple[str, ...] = ()
    max_rows_per_batch: int = 5000

    # Topic strategy
    topic_per_table: bool = False
    topic_prefix: str = ""
    topic_separator: str = "."

    # Tracked tables (до 5 приоритетных таблиц)
    tracked_tables: Tuple[str, ...] = ()  # формат: ("HR.EMPLOYEES", "SALES.ORDERS", ...)
    strict_tracked_only: bool = False  # True: отправлять только tracked tables

    # Logging
    verbose: bool = True
    log_first_n_events: int = 3


# Универсальная схема envelope: подходит и для raw, и для table-specific payload.
# before/after оставлены object|null с additionalProperties=true, чтобы было удобно расширять
# под конкретные таблицы без постоянной миграции общей JSON Schema.
JSON_VALUE_SCHEMA = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "OracleCdcEnvelope",
  "type": "object",
  "additionalProperties": false,
  "required": ["source", "op", "event_id", "before", "after", "raw"],
  "properties": {
    "source": {
      "type": "object",
      "additionalProperties": false,
      "required": ["schema", "table", "scn", "timestamp"],
      "properties": {
        "schema": {"type": "string"},
        "table": {"type": "string"},
        "commit_scn": {"type": ["integer", "null"]},
        "scn": {"type": "integer"},
        "redo_sequence": {"type": ["integer", "null"]},
        "rs_id": {"type": ["string", "null"]},
        "ssn": {"type": ["integer", "null"]},
        "timestamp": {"type": ["string", "null"]}
      }
    },
    "op": {"type": "string"},
    "event_id": {"type": "string"},
    "before": {"type": ["null", "object"], "additionalProperties": true},
    "after": {"type": ["null", "object"], "additionalProperties": true},
    "raw": {
      "type": "object",
      "additionalProperties": false,
      "required": ["sql_redo", "sql_undo"],
      "properties": {
        "sql_redo": {"type": ["string", "null"]},
        "sql_undo": {"type": ["string", "null"]}
      }
    }
  }
}
"""


def _log(cfg: Config, msg: str) -> None:
    if cfg.verbose:
        print(f"[oracle->kafka:jupyter:sr-json] {msg}")


def _normalize_name_list(values: Sequence[str]) -> List[str]:
    normalized: List[str] = []
    for value in values:
        item = str(value).strip().upper()
        if item and item not in normalized:
            normalized.append(item)
    return normalized


def _normalize_table_ids(values: Sequence[str]) -> List[str]:
    result: List[str] = []
    for value in values:
        table_id = str(value).strip().upper()
        if not table_id or "." not in table_id:
            continue
        if table_id not in result:
            result.append(table_id)
    return result


def _load_state(cfg: Config) -> Dict[str, int]:
    path = Path(cfg.state_file)
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    return {"last_commit_scn": cfg.start_from_commit_scn}


def _save_state(cfg: Config, last_commit_scn: int) -> None:
    path = Path(cfg.state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump({"last_commit_scn": last_commit_scn}, fh)


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


def _build_json_serializer(cfg: Config) -> JSONSerializer:
    sr_conf: Dict[str, str] = {"url": cfg.schema_registry_url}
    if cfg.schema_registry_basic_auth_user_info:
        sr_conf["basic.auth.user.info"] = cfg.schema_registry_basic_auth_user_info
    sr_client = SchemaRegistryClient(sr_conf)

    return JSONSerializer(
        schema_str=JSON_VALUE_SCHEMA,
        schema_registry_client=sr_client,
        conf={"auto.register.schemas": True},
    )


def _get_archived_logs(cur: oracledb.Cursor, limit_rows: int) -> List[str]:
    sql = """
    SELECT name
    FROM (
      SELECT name
      FROM v$archived_log
      WHERE name IS NOT NULL
      ORDER BY sequence# DESC
    )
    WHERE ROWNUM <= :limit_rows
    """
    cur.execute(sql, {"limit_rows": limit_rows})
    logs = [row[0] for row in cur.fetchall() if row and row[0]]
    logs.reverse()
    return logs


def _end_logminer(cur: oracledb.Cursor) -> None:
    cur.execute(
        """
        BEGIN
          DBMS_LOGMNR.END_LOGMNR;
        EXCEPTION
          WHEN OTHERS THEN NULL;
        END;
        """
    )


def _add_logfile(cur: oracledb.Cursor, logfile: str, first: bool) -> None:
    option = "DBMS_LOGMNR.NEW" if first else "DBMS_LOGMNR.ADDFILE"
    cur.execute(
        f"""
        BEGIN
          DBMS_LOGMNR.ADD_LOGFILE(
            LOGFILENAME => :logfile,
            OPTIONS     => {option}
          );
        END;
        """,
        {"logfile": logfile},
    )


def _start_logminer(cur: oracledb.Cursor, cfg: Config, log_files: List[str]) -> None:
    if not log_files:
        raise RuntimeError("Не найдено archived logs в v$archived_log")

    _end_logminer(cur)
    for idx, logfile in enumerate(log_files):
        _add_logfile(cur, logfile, first=(idx == 0))

    cur.call_timeout = cfg.call_timeout_ms
    cur.execute(
        """
        BEGIN
          DBMS_LOGMNR.START_LOGMNR(
            OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                     + DBMS_LOGMNR.COMMITTED_DATA_ONLY
                     + DBMS_LOGMNR.NO_ROWID_IN_STMT
          );
        END;
        """
    )


def _fetch_rows(
    cur: oracledb.Cursor,
    from_commit_scn: int,
    filter_schemas: Sequence[str],
    filter_tables: Sequence[str],
    max_rows_per_batch: int,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT
          commit_scn,
          scn,
          timestamp,
          seg_owner,
          table_name,
          operation,
          operation_code,
          sequence# AS redo_sequence,
          rs_id,
          ssn,
          sql_redo,
          sql_undo
        FROM v$logmnr_contents
        WHERE operation_code IN (1, 2, 3)
          AND commit_scn > :from_commit_scn
    """

    binds: Dict[str, Any] = {"from_commit_scn": from_commit_scn}

    if filter_schemas:
        placeholders = []
        for idx, schema in enumerate(filter_schemas):
            key = f"schema_{idx}"
            placeholders.append(f":{key}")
            binds[key] = schema
        sql += f" AND seg_owner IN ({', '.join(placeholders)})"

    if filter_tables:
        placeholders = []
        for idx, table in enumerate(filter_tables):
            key = f"table_{idx}"
            placeholders.append(f":{key}")
            binds[key] = table
        sql += f" AND table_name IN ({', '.join(placeholders)})"

    sql += " ORDER BY commit_scn, sequence#, rs_id, ssn"

    if max_rows_per_batch > 0:
        sql = f"SELECT * FROM ({sql}) WHERE ROWNUM <= :max_rows_per_batch"
        binds["max_rows_per_batch"] = int(max_rows_per_batch)

    cur.execute(sql, binds)
    cols = [col[0].lower() for col in cur.description]
    return [dict(zip(cols, row)) for row in cur]


def _json_default(obj: Any) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    return int(value)


def _to_source(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema": str(row.get("seg_owner") or "UNKNOWN").upper(),
        "table": str(row.get("table_name") or "UNKNOWN").upper(),
        "commit_scn": _as_int(row.get("commit_scn")),
        "scn": int(row.get("scn") or 0),
        "redo_sequence": _as_int(row.get("redo_sequence")),
        "rs_id": row.get("rs_id"),
        "ssn": _as_int(row.get("ssn")),
        "timestamp": _json_default(row.get("timestamp")),
    }


def _table_id(source: Dict[str, Any]) -> str:
    return f"{source.get('schema', 'UNKNOWN')}.{source.get('table', 'UNKNOWN')}"


def _op_code_to_op_name(op_code: int) -> str:
    if op_code == 1:
        return "INSERT"
    if op_code == 2:
        return "DELETE"
    if op_code == 3:
        return "UPDATE"
    return "UNKNOWN"


def _extract_set_pairs_from_update(sql_text: Optional[str]) -> Dict[str, str]:
    """
    Best-effort parser для UPDATE ... SET a=b, c=d ...
    Важно: это шаблон для старта, не full SQL parser.
    """
    if not sql_text:
        return {}

    text = str(sql_text)
    match = re.search(r"\bSET\b(.*?)(\bWHERE\b|$)", text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return {}

    set_part = match.group(1)
    result: Dict[str, str] = {}

    for chunk in set_part.split(","):
        if "=" not in chunk:
            continue
        left, right = chunk.split("=", 1)
        col = left.strip().strip('"').upper()
        val = right.strip()
        if col:
            result[col] = val
    return result


def _default_table_parser(row: Dict[str, Any], source: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Базовый parser для tracked tables:
    - INSERT: только after (best-effort из sql_redo SET/VALUES не извлекаем полностью);
    - UPDATE: before из sql_undo SET, after из sql_redo SET;
    - DELETE: только before.
    """
    op_code = int(row.get("operation_code") or 0)
    sql_redo = row.get("sql_redo")
    sql_undo = row.get("sql_undo")

    if op_code == 1:
        return None, {"_raw_sql_redo": sql_redo}

    if op_code == 2:
        return {"_raw_sql_undo": sql_undo}, None

    if op_code == 3:
        before = _extract_set_pairs_from_update(sql_undo)
        after = _extract_set_pairs_from_update(sql_redo)
        if not before:
            before = {"_raw_sql_undo": sql_undo}
        if not after:
            after = {"_raw_sql_redo": sql_redo}
        return before, after

    return None, None


# Регистр table-specific parsers.
# Ключ: SCHEMA.TABLE в UPPER.
# Значение: функция (row, source) -> (before, after)
TABLE_PARSERS: Dict[str, Callable[[Dict[str, Any], Dict[str, Any]], Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]]] = {}


def _build_payload(row: Dict[str, Any], tracked_set: Sequence[str]) -> Optional[Dict[str, Any]]:
    source = _to_source(row)
    table = _table_id(source)
    op = _op_code_to_op_name(int(row.get("operation_code") or 0))

    event_id = (
        f"{table}|{source.get('commit_scn')}|{source.get('redo_sequence')}|"
        f"{source.get('rs_id')}|{source.get('ssn')}"
    )

    in_tracked = table in tracked_set

    # Для tracked таблиц даем structured before/after через parser.
    if in_tracked:
        parser = TABLE_PARSERS.get(table, _default_table_parser)
        before, after = parser(row, source)
        return {
            "source": source,
            "op": op,
            "event_id": event_id,
            "before": before,
            "after": after,
            "raw": {
                "sql_redo": row.get("sql_redo"),
                "sql_undo": row.get("sql_undo"),
            },
        }

    # Для остальных таблиц — raw envelope без парсинга before/after.
    return {
        "source": source,
        "op": op,
        "event_id": event_id,
        "before": None,
        "after": None,
        "raw": {
            "sql_redo": row.get("sql_redo"),
            "sql_undo": row.get("sql_undo"),
        },
    }


def _event_key(event: Dict[str, Any]) -> bytes:
    return str(event.get("event_id", "")).encode("utf-8", errors="ignore")


def _resolve_topic(cfg: Config, event: Dict[str, Any]) -> str:
    if not cfg.topic_per_table:
        return cfg.kafka_topic

    sep = cfg.topic_separator or "."
    src = event.get("source", {})
    schema_name = str(src.get("schema") or "unknown").strip().lower()
    table_name = str(src.get("table") or "unknown").strip().lower()

    parts = []
    if cfg.topic_prefix:
        parts.extend([p for p in cfg.topic_prefix.split(sep) if p])
    parts.extend([schema_name, table_name])

    return sep.join(parts).replace(" ", "_")


def run_once(cfg: Config) -> Dict[str, Any]:
    started_at = time.time()
    state = _load_state(cfg)
    last_commit_scn = int(state.get("last_commit_scn", cfg.start_from_commit_scn))

    filter_schemas = _normalize_name_list(cfg.filter_schemas)
    filter_tables = _normalize_name_list(cfg.filter_tables)
    tracked_tables = _normalize_table_ids(cfg.tracked_tables)

    _log(
        cfg,
        "run_once started: "
        f"from_commit_scn={last_commit_scn}, base_topic={cfg.kafka_topic}, "
        f"schema_filter={filter_schemas or '-'}, table_filter={filter_tables or '-'}, "
        f"tracked={tracked_tables or '-'}, strict_tracked_only={cfg.strict_tracked_only}, "
        f"max_rows_per_batch={cfg.max_rows_per_batch}",
    )

    producer = _build_kafka_producer(cfg)
    json_serializer = _build_json_serializer(cfg)

    conn = oracledb.connect(user=cfg.oracle_user, password=cfg.oracle_password, dsn=cfg.oracle_dsn)
    conn.call_timeout = cfg.call_timeout_ms

    produced = 0
    skipped = 0
    max_commit_scn_seen = last_commit_scn

    try:
        with conn.cursor() as cur:
            logs = _get_archived_logs(cur, cfg.archive_log_limit)
            _log(cfg, f"archived logs selected ({len(logs)}):")
            for lf in logs:
                _log(cfg, f"  - {lf}")

            _start_logminer(cur, cfg, logs)
            rows = _fetch_rows(
                cur,
                from_commit_scn=last_commit_scn,
                filter_schemas=filter_schemas,
                filter_tables=filter_tables,
                max_rows_per_batch=cfg.max_rows_per_batch,
            )
            _log(cfg, f"rows fetched: {len(rows)}")

            for idx, row in enumerate(rows):
                payload = _build_payload(row, tracked_tables)
                if payload is None:
                    skipped += 1
                    continue

                current_table = _table_id(payload["source"])
                if cfg.strict_tracked_only and tracked_tables and current_table not in tracked_tables:
                    skipped += 1
                    continue

                topic = _resolve_topic(cfg, payload)
                value = json_serializer(payload, SerializationContext(topic, MessageField.VALUE))

                producer.produce(topic=topic, key=_event_key(payload), value=value)
                producer.poll(0)
                produced += 1

                commit_scn = payload.get("source", {}).get("commit_scn")
                if isinstance(commit_scn, int) and commit_scn > max_commit_scn_seen:
                    max_commit_scn_seen = commit_scn

                if idx < cfg.log_first_n_events:
                    _log(
                        cfg,
                        f"sent topic={topic}, op={payload.get('op')}, table={current_table}, "
                        f"commit_scn={commit_scn}, event_id={payload.get('event_id')}",
                    )

            remain = producer.flush(cfg.kafka_flush_timeout_sec)
            if remain > 0:
                raise RuntimeError(f"producer.flush timeout: {remain} messages not delivered")

            if max_commit_scn_seen > last_commit_scn:
                _save_state(cfg, max_commit_scn_seen)
                _log(cfg, f"state updated: last_commit_scn={max_commit_scn_seen}")
            else:
                _log(cfg, "state not changed")

            return {
                "rows": len(rows),
                "produced": produced,
                "skipped": skipped,
                "last_commit_scn_before": last_commit_scn,
                "last_commit_scn_after": max_commit_scn_seen,
                "duration_sec": round(time.time() - started_at, 3),
            }

    finally:
        with conn.cursor() as cur:
            _end_logminer(cur)
        conn.close()


def run_loop(cfg: Config) -> None:
    _log(cfg, f"run_loop started, poll_seconds={cfg.poll_seconds}")
    while True:
        try:
            result = run_once(cfg)
            _log(cfg, f"batch done: {result}")
        except Exception as exc:
            _log(cfg, f"batch failed: {exc}")
        time.sleep(cfg.poll_seconds)
