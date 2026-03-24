#!/usr/bin/env python3
"""Oracle LogMiner -> Kafka RAW runner (one-shot).

Этот скрипт вынесен из CDC producer, чтобы raw-режим жил отдельно:
- публикует в Kafka сырой JSON-слепок строк из v$logmnr_contents;
- не использует CDC envelope и Schema Registry;
- рассчитан на one-shot запуск (cron/flock).
"""

from __future__ import annotations

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
    from .config import Config, load_config_from_env, validate_config
except ImportError:  # pragma: no cover
    from config import Config, load_config_from_env, validate_config


def _log(cfg: Config, message: str) -> None:
    if cfg.verbose:
        print(f"[oracle->kafka:archivelog-raw] {message}")


def _load_state(cfg: Config) -> Dict[str, Any]:
    """Read watermark state. Return START_FROM_SCN on first run."""
    path = Path(cfg.state_file)
    if not path.exists():
        return {"last_commit_scn": cfg.start_from_commit_scn}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"STATE_FILE is not valid JSON: {path} ({exc})") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"STATE_FILE must contain JSON object, got: {type(payload).__name__}")
    if "last_commit_scn" not in payload:
        raise RuntimeError(f"STATE_FILE is missing 'last_commit_scn': {path}")

    try:
        payload["last_commit_scn"] = int(payload.get("last_commit_scn"))
    except Exception as exc:
        raise RuntimeError(f"STATE_FILE has invalid 'last_commit_scn': {payload.get('last_commit_scn')!r}") from exc

    return payload


def _fsync_directory(path: Path) -> None:
    """Try to fsync dir after os.replace for better crash-consistency."""
    try:
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
    """Atomically write JSON: tmp -> flush -> fsync -> os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(str(tmp_path), str(path))
        _fsync_directory(path.parent)
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        raise


def _save_state(cfg: Config, last_commit_scn: int) -> None:
    """Persist watermark after successful batch."""
    path = Path(cfg.state_file)
    _atomic_write_json(
        path,
        {
            "last_commit_scn": int(last_commit_scn),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def _build_kafka_producer(cfg: Config) -> Producer:
    """Create Kafka Producer for PLAINTEXT/SSL/SASL_* modes."""
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
    """Safely end LogMiner session."""
    cur.execute(
        """
        BEGIN
          DBMS_LOGMNR.END_LOGMNR;
        EXCEPTION
          WHEN OTHERS THEN NULL;
        END;
        """
    )


def _get_archived_logs(cur: oracledb.Cursor, from_commit_scn: int, limit_rows: int) -> List[Dict[str, Any]]:
    """Locate archived logs for current watermark."""

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
    """Add redo log file to LogMiner."""
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
    """Start LogMiner on selected archived logs."""
    if not log_files:
        raise RuntimeError("No archived logs found in v$archived_log")
    _end_logminer(cur)
    for i, lf in enumerate(log_files):
        _add_logfile(cur, lf, first=(i == 0))

    options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
    if cfg.use_no_rowid_in_stmt:
        options += " + DBMS_LOGMNR.NO_ROWID_IN_STMT"

    cur.call_timeout = cfg.call_timeout_ms
    cur.execute(
        f"""
        BEGIN
          DBMS_LOGMNR.START_LOGMNR(OPTIONS => {options});
        END;
        """
    )


def _build_fetch_rows_query(
    from_commit_scn: int,
    filter_schemas: Sequence[str],
    filter_tables: Sequence[str],
    max_rows_per_batch: int,
) -> Tuple[str, Dict[str, Any]]:
    """Build SQL for reading DML rows from v$logmnr_contents."""
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
        placeholders: List[str] = []
        for i, value in enumerate(filter_schemas):
            key = f"filter_schema_{i}"
            placeholders.append(f":{key}")
            binds[key] = value
        base_sql += f" AND seg_owner IN ({', '.join(placeholders)})"

    if filter_tables:
        placeholders = []
        for i, value in enumerate(filter_tables):
            key = f"filter_table_{i}"
            placeholders.append(f":{key}")
            binds[key] = value
        base_sql += f" AND table_name IN ({', '.join(placeholders)})"

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
    """Convert Oracle tuple-record to dict and normalize timestamp."""
    row = dict(zip(columns, record))
    ts = row.get("timestamp")
    if isinstance(ts, datetime):
        row["timestamp"] = ts.isoformat()
    return row


def _event_key(row: Dict[str, Any]) -> bytes:
    """Build deterministic technical key for raw event."""
    key = (
        f"{row.get('seg_owner','')}.{row.get('table_name','')}|"
        f"{row.get('commit_scn','')}|{row.get('redo_sequence','')}|"
        f"{row.get('rs_id','')}|{row.get('ssn','')}"
    )
    return key.encode("utf-8")


def _sanitize_topic_part(value: str) -> str:
    """Sanitize topic segment (allow alnum, '-', '_' and '.')."""
    chars: List[str] = []
    for ch in str(value).strip().lower():
        chars.append(ch if (ch.isalnum() or ch in {"-", "_", "."}) else "_")
    out = "".join(chars).strip("._-")
    return out or "unknown"


def _resolve_topic(cfg: Config, row: Dict[str, Any]) -> str:
    """Resolve target topic in single-topic or topic-per-table mode."""
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
    """Produce with retries when local queue is full (BufferError)."""
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
    """Publish raw JSON snapshot of LogMiner row."""
    value = json.dumps(row, ensure_ascii=False).encode("utf-8")
    return _produce_with_backpressure(cfg, producer, topic, _event_key(row), value, callback)


def run_once(cfg: Config) -> Dict[str, Any]:
    """Run one raw batch.

    Steps:
    1) read state watermark,
    2) start LogMiner on archived logs,
    3) read DML rows and publish raw events,
    4) flush and verify delivery,
    5) persist updated state.
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
            f"use_fetchmany={cfg.use_fetchmany}, fetchmany_size={cfg.fetchmany_size})"
        ),
    )

    producer = _build_kafka_producer(cfg)

    delivered = 0
    failed = 0
    queue_full_retries = 0
    queue_full_wait_sec = 0.0
    rows_count = 0
    raw_rows = 0
    topics_used: Dict[str, int] = {}

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
            nonlocal first_batch_commit_scn, last_batch_commit_scn, raw_rows

            row = _record_to_row(columns, record)
            rows_count += 1

            commit_scn = int(row["commit_scn"])
            if first_batch_commit_scn is None:
                first_batch_commit_scn = commit_scn
            last_batch_commit_scn = commit_scn

            topic = _resolve_topic(cfg, row)
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
                        "queued raw event "
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
            _log(cfg, "fetch mode=fetchall (compat)")
            for record in cur.fetchall():
                process_record(record)

        _log(cfg, f"rows fetched from v$logmnr_contents: {rows_count}")

        remaining_after_flush = producer.flush(cfg.kafka_flush_timeout_sec)
        _log(
            cfg,
            (
                "kafka flush done "
                f"(delivered={delivered}, failed={failed}, queued={raw_rows}, "
                f"remaining_after_flush={remaining_after_flush}, "
                f"queue_full_retries={queue_full_retries}, "
                f"queue_full_wait_sec={queue_full_wait_sec:.3f})"
            ),
        )
        if failed > 0:
            raise RuntimeError(f"Kafka delivery failures: {failed} of {raw_rows}")
        if remaining_after_flush > 0:
            raise RuntimeError(
                "Kafka flush timeout: "
                f"{remaining_after_flush} messages left in producer queue "
                f"after {cfg.kafka_flush_timeout_sec}s"
            )

        if rows_count > 0:
            last_commit_scn = max(last_commit_scn, int(last_batch_commit_scn))
            _save_state(cfg, last_commit_scn)
            _log(cfg, f"state updated: last_commit_scn={last_commit_scn}")
        else:
            _log(cfg, "no new rows, state not changed")

        result = {
            "rows": rows_count,
            "raw_rows": raw_rows,
            "delivered": delivered,
            "failed": failed,
            "remaining_after_flush": remaining_after_flush,
            "queue_full_retries": queue_full_retries,
            "queue_full_wait_sec": round(queue_full_wait_sec, 3),
            "last_commit_scn": last_commit_scn,
            "topics_used": topics_used,
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
    """CLI entrypoint: one run -> exit code 0/1 for cron/monitoring."""
    cfg = load_config_from_env()
    validate_config(cfg)

    _log(
        cfg,
        (
            "start one-shot raw batch "
            f"(dsn={cfg.oracle_dsn}, topic={cfg.kafka_topic}, "
            f"archive_log_limit={cfg.archive_log_limit}, state_file={cfg.state_file})"
        ),
    )
    try:
        stats = run_once(cfg)
        _log(cfg, f"one-shot finished: {stats}")
        return 0
    except Exception as exc:
        print(f"[oracle->kafka:archivelog-raw] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
