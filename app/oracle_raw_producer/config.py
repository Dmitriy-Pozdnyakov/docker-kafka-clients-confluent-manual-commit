"""Config layer for Oracle LogMiner RAW producer runner."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class Config:
    # =========================
    # Oracle connection
    # =========================
    oracle_user: str
    oracle_password: str
    oracle_dsn: str
    call_timeout_ms: int = 30000
    archive_log_limit: int = 3
    use_no_rowid_in_stmt: bool = True

    # =========================
    # Kafka connection
    # =========================
    kafka_broker: str = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"
    kafka_topic: str = "oracle.logminer.raw"
    kafka_security_protocol: str = "PLAINTEXT"
    ssl_cafile: str = ""
    ssl_check_hostname: bool = True
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    kafka_client_id: str = "oracle-logminer-archivelog-raw"
    kafka_flush_timeout_sec: int = 30
    produce_retry_timeout_sec: int = 60
    produce_retry_poll_sec: float = 0.2
    topic_per_table: bool = False
    topic_prefix: str = ""
    topic_separator: str = "."

    # =========================
    # State management
    # =========================
    state_file: str = "./oracle_kafka_state_archivelog_raw.json"
    start_from_commit_scn: int = 0

    # =========================
    # Filters / batching
    # =========================
    filter_schemas: Tuple[str, ...] = ()
    filter_tables: Tuple[str, ...] = ()
    max_rows_per_batch: int = 5000
    use_fetchmany: bool = True
    fetchmany_size: int = 1000

    # =========================
    # Logging
    # =========================
    verbose: bool = True
    log_first_n_events: int = 3


def str_to_bool(value: str, default: bool) -> bool:
    """Normalize bool parameters from env (true/false, yes/no, 1/0)."""
    raw = value.strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def parse_csv_upper(raw: str) -> List[str]:
    """Convert CSV string to unique UPPER-case values preserving order."""
    result: List[str] = []
    for item in raw.split(","):
        normalized = item.strip().upper()
        if normalized and normalized not in result:
            result.append(normalized)
    return result


def load_config_from_env() -> Config:
    """Build runtime config from env with safe defaults."""
    broker = os.getenv("KAFKA_BROKER", "").strip()
    if not broker:
        broker = os.getenv("BROKER", "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094").strip()

    return Config(
        oracle_user=os.getenv("ORACLE_USER", "").strip(),
        oracle_password=os.getenv("ORACLE_PASSWORD", "").strip(),
        oracle_dsn=os.getenv("ORACLE_DSN", "").strip(),
        call_timeout_ms=int(os.getenv("CALL_TIMEOUT_MS", "30000")),
        archive_log_limit=int(os.getenv("ARCHIVE_LOG_LIMIT", "3")),
        use_no_rowid_in_stmt=str_to_bool(os.getenv("USE_NO_ROWID_IN_STMT", "true"), True),
        kafka_broker=broker,
        kafka_topic=os.getenv("KAFKA_TOPIC", "oracle.logminer.raw").strip(),
        kafka_security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper(),
        ssl_cafile=os.getenv("SSL_CAFILE", "").strip(),
        ssl_check_hostname=str_to_bool(os.getenv("SSL_CHECK_HOSTNAME", "true"), True),
        kafka_sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN").strip(),
        kafka_sasl_username=os.getenv("KAFKA_SASL_USERNAME", "").strip(),
        kafka_sasl_password=os.getenv("KAFKA_SASL_PASSWORD", "").strip(),
        kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "oracle-logminer-archivelog-raw").strip(),
        kafka_flush_timeout_sec=int(os.getenv("KAFKA_FLUSH_TIMEOUT_SEC", "30")),
        produce_retry_timeout_sec=int(os.getenv("PRODUCE_RETRY_TIMEOUT_SEC", "60")),
        produce_retry_poll_sec=float(os.getenv("PRODUCE_RETRY_POLL_SEC", "0.2")),
        topic_per_table=str_to_bool(os.getenv("TOPIC_PER_TABLE", "false"), False),
        topic_prefix=os.getenv("TOPIC_PREFIX", "").strip(),
        topic_separator=os.getenv("TOPIC_SEPARATOR", ".").strip() or ".",
        state_file=os.getenv("STATE_FILE", "./oracle_kafka_state_archivelog_raw.json").strip(),
        start_from_commit_scn=int(os.getenv("START_FROM_SCN", "0") or "0"),
        filter_schemas=tuple(parse_csv_upper(os.getenv("FILTER_SCHEMAS", ""))),
        filter_tables=tuple(parse_csv_upper(os.getenv("FILTER_TABLES", ""))),
        max_rows_per_batch=int(os.getenv("MAX_ROWS_PER_BATCH", "5000")),
        use_fetchmany=str_to_bool(os.getenv("USE_FETCHMANY", "true"), True),
        fetchmany_size=int(os.getenv("FETCHMANY_SIZE", "1000")),
        verbose=str_to_bool(os.getenv("VERBOSE", "true"), True),
        log_first_n_events=int(os.getenv("LOG_FIRST_N_EVENTS", "3")),
    )


def validate_config(cfg: Config) -> None:
    """Validate required fields and key options."""
    missing: List[str] = []
    if not cfg.oracle_user:
        missing.append("ORACLE_USER")
    if not cfg.oracle_password:
        missing.append("ORACLE_PASSWORD")
    if not cfg.oracle_dsn:
        missing.append("ORACLE_DSN")
    if not cfg.kafka_broker:
        missing.append("KAFKA_BROKER or BROKER")
    if not cfg.kafka_topic:
        missing.append("KAFKA_TOPIC")
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    if cfg.fetchmany_size <= 0:
        raise RuntimeError("FETCHMANY_SIZE must be > 0")
