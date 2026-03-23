"""Config layer for Oracle LogMiner CDC + Schema Registry runner.

Содержит:
1) dataclass `Config`;
2) загрузку конфига из env;
3) валидацию обязательных параметров;
4) парсинг CSV-фильтров.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Tuple

try:
    from .cdc_schema_registry import schema_registry_dependencies_available
except ImportError:  # pragma: no cover
    from cdc_schema_registry import schema_registry_dependencies_available


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
    kafka_client_id: str = "oracle-logminer-archivelog-sr-cdc"
    kafka_flush_timeout_sec: int = 30
    produce_retry_timeout_sec: int = 60
    produce_retry_poll_sec: float = 0.2
    topic_per_table: bool = False
    topic_prefix: str = ""
    topic_separator: str = "."

    # =========================
    # State management
    # =========================
    state_file: str = "./oracle_kafka_state_archivelog_sr_cdc.json"
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
    # Prototype CDC + Schema Registry
    # =========================
    cdc_envelope_enabled: bool = False
    cdc_supported_tables: Tuple[str, ...] = ()
    cdc_key_mode: str = "technical"
    cdc_parse_error_mode: str = "raw"
    cdc_sql_parser_backend: str = "auto"
    schema_registry_url: str = "http://127.0.0.1:18081"
    schema_dir: str = "schemas"
    schema_auto_register: bool = True
    schema_use_latest_version: bool = False
    schema_normalize: bool = True

    # =========================
    # Logging
    # =========================
    verbose: bool = True
    log_first_n_events: int = 3


def str_to_bool(value: str, default: bool) -> bool:
    """Нормализует bool-параметры из env (true/false, yes/no, 1/0)."""
    raw = value.strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def parse_csv_upper(raw: str) -> List[str]:
    """Преобразует CSV-строку в уникальный список UPPER-значений."""
    result: List[str] = []
    for item in raw.split(","):
        normalized = item.strip().upper()
        if normalized and normalized not in result:
            result.append(normalized)
    return result


def load_config_from_env() -> Config:
    """Собирает runtime-конфиг из env с безопасными дефолтами."""
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
        kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "oracle-logminer-archivelog-sr-cdc").strip(),
        kafka_flush_timeout_sec=int(os.getenv("KAFKA_FLUSH_TIMEOUT_SEC", "30")),
        produce_retry_timeout_sec=int(os.getenv("PRODUCE_RETRY_TIMEOUT_SEC", "60")),
        produce_retry_poll_sec=float(os.getenv("PRODUCE_RETRY_POLL_SEC", "0.2")),
        topic_per_table=str_to_bool(os.getenv("TOPIC_PER_TABLE", "false"), False),
        topic_prefix=os.getenv("TOPIC_PREFIX", "").strip(),
        topic_separator=os.getenv("TOPIC_SEPARATOR", ".").strip() or ".",
        state_file=os.getenv("STATE_FILE", "./oracle_kafka_state_archivelog_sr_cdc.json").strip(),
        start_from_commit_scn=int(os.getenv("START_FROM_SCN", "0") or "0"),
        filter_schemas=tuple(parse_csv_upper(os.getenv("FILTER_SCHEMAS", ""))),
        filter_tables=tuple(parse_csv_upper(os.getenv("FILTER_TABLES", ""))),
        max_rows_per_batch=int(os.getenv("MAX_ROWS_PER_BATCH", "5000")),
        use_fetchmany=str_to_bool(os.getenv("USE_FETCHMANY", "true"), True),
        fetchmany_size=int(os.getenv("FETCHMANY_SIZE", "1000")),
        cdc_envelope_enabled=str_to_bool(os.getenv("CDC_ENVELOPE_ENABLED", "false"), False),
        cdc_supported_tables=tuple(parse_csv_upper(os.getenv("CDC_SUPPORTED_TABLES", ""))),
        cdc_key_mode=os.getenv("CDC_KEY_MODE", "technical").strip().lower(),
        cdc_parse_error_mode=os.getenv("CDC_PARSE_ERROR_MODE", "raw").strip().lower(),
        cdc_sql_parser_backend=os.getenv("CDC_SQL_PARSER_BACKEND", "auto").strip().lower(),
        schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:18081").strip(),
        schema_dir=os.getenv("SCHEMA_DIR", "schemas").strip(),
        schema_auto_register=str_to_bool(os.getenv("SCHEMA_AUTO_REGISTER", "true"), True),
        schema_use_latest_version=str_to_bool(os.getenv("SCHEMA_USE_LATEST_VERSION", "false"), False),
        schema_normalize=str_to_bool(os.getenv("SCHEMA_NORMALIZE", "true"), True),
        verbose=str_to_bool(os.getenv("VERBOSE", "true"), True),
        log_first_n_events=int(os.getenv("LOG_FIRST_N_EVENTS", "3")),
    )


def validate_config(cfg: Config) -> None:
    """Проверяет обязательные поля и совместимость ключевых опций."""
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
    if cfg.cdc_sql_parser_backend not in {"auto", "auto_legacy_first", "legacy_first", "legacy", "sqlglot"}:
        raise RuntimeError(
            "CDC_SQL_PARSER_BACKEND must be one of: "
            "auto, auto_legacy_first, legacy_first, legacy, sqlglot"
        )

    if cfg.cdc_envelope_enabled:
        if not schema_registry_dependencies_available():
            raise RuntimeError("Schema Registry dependencies are unavailable in confluent-kafka package")
        if not cfg.schema_registry_url:
            raise RuntimeError("SCHEMA_REGISTRY_URL is required when CDC_ENVELOPE_ENABLED=true")
