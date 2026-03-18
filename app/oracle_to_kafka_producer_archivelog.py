#!/usr/bin/env python3
"""
Oracle LogMiner (archived logs) -> Kafka, Docker/env-friendly entrypoint.

Структура намеренно синхронизирована с jupyter-вариантом:
- конфиг через dataclass `Config`;
- один батч через `run_once(config)`;
- entrypoint `main()` работает в one-shot режиме (для cron/docker compose run);
- подробные комментарии по шагам.
"""

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import oracledb
from confluent_kafka import Producer


@dataclass
class Config:
    # =========================
    # Oracle connection
    # =========================
    oracle_user: str  # пользователь Oracle
    oracle_password: str  # пароль Oracle
    oracle_dsn: str  # DSN Oracle, например "host:1521/service_name"
    call_timeout_ms: int = 30000  # timeout Oracle-запросов в миллисекундах
    archive_log_limit: int = 3  # сколько последних archived logs подключать к LogMiner
    use_no_rowid_in_stmt: bool = True  # добавлять ли NO_ROWID_IN_STMT в options LogMiner

    # =========================
    # Kafka connection
    # =========================
    kafka_broker: str = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"  # bootstrap servers
    kafka_topic: str = "oracle.logminer.raw"  # целевой topic
    kafka_security_protocol: str = "PLAINTEXT"  # PLAINTEXT / SSL / SASL_SSL / SASL_PLAINTEXT
    ssl_cafile: str = ""  # путь до CA сертификата (для SSL/SASL_SSL)
    ssl_check_hostname: bool = True  # проверка hostname в TLS-сертификате
    kafka_sasl_mechanism: str = "PLAIN"  # SASL механизм
    kafka_sasl_username: str = ""  # SASL username
    kafka_sasl_password: str = ""  # SASL password
    kafka_client_id: str = "oracle-logminer-archivelog-producer"  # client.id producer
    kafka_flush_timeout_sec: int = 30  # timeout flush producer
    produce_retry_timeout_sec: int = 60  # максимум ожидания при queue full на 1 сообщение
    produce_retry_poll_sec: float = 0.2  # интервал poll при queue full
    topic_per_table: bool = False  # писать ли в отдельный topic на каждую таблицу
    topic_prefix: str = ""  # префикс для topic per table, например "oracle.cdc"
    topic_separator: str = "."  # разделитель частей topic name

    # =========================
    # State management
    # =========================
    state_file: str = "./oracle_kafka_state_archivelog.json"  # state-файл с last_commit_scn
    start_from_commit_scn: int = 0  # стартовый commit_scn если state-файл отсутствует

    # =========================
    # Loop mode
    # =========================
    poll_seconds: int = 60  # legacy параметр: в one-shot режиме контейнера не используется

    # =========================
    # Data filters
    # =========================
    filter_schema: str = ""  # одиночный фильтр схемы (legacy)
    filter_table: str = ""  # одиночный фильтр таблицы (legacy)
    filter_schemas: Tuple[str, ...] = ()  # список схем
    filter_tables: Tuple[str, ...] = ()  # список таблиц
    max_rows_per_batch: int = 5000  # максимум строк за один батч (0 = без лимита)
    use_fetchmany: bool = True  # читать данные из курсора чанками (memory-friendly)
    fetchmany_size: int = 1000  # размер чанка при use_fetchmany=true

    # =========================
    # Logging
    # =========================
    verbose: bool = True  # включить подробные логи
    log_first_n_events: int = 3  # сколько первых событий батча логировать детально

def _log(cfg: Config, message: str) -> None:
    # Единый формат логов: печатаем только при verbose.
    if cfg.verbose:
        print(f"[oracle->kafka:archivelog] {message}")


def _parse_comma_separated_upper_list(raw: str) -> List[str]:
    # Разбираем строку "A,B,C" -> ["A", "B", "C"] c trim + upper + dedupe.
    values: List[str] = []
    for item in raw.split(","):
        normalized = item.strip().upper()
        if normalized and normalized not in values:
            values.append(normalized)
    return values


def _merge_name_filters(single_value: str, many_values: Sequence[str]) -> List[str]:
    # Объединяем legacy одиночное значение и список, удаляя дубли.
    merged: List[str] = []
    single = str(single_value).strip().upper()
    if single:
        merged.append(single)
    for value in many_values:
        normalized = str(value).strip().upper()
        if normalized and normalized not in merged:
            merged.append(normalized)
    return merged


def _str_to_bool(value: str, default: bool) -> bool:
    raw = value.strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def load_config_from_env() -> Config:
    # Загружаем все настройки из env для docker-режима.
    broker = os.getenv("KAFKA_BROKER", "").strip()
    if not broker:
        broker = os.getenv("BROKER", "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094").strip()

    start_from_commit_scn = int(os.getenv("START_FROM_SCN", "0") or "0")
    filter_schemas = tuple(_parse_comma_separated_upper_list(os.getenv("FILTER_SCHEMAS", "")))
    filter_tables = tuple(_parse_comma_separated_upper_list(os.getenv("FILTER_TABLES", "")))

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
        kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "oracle-logminer-archivelog-producer").strip(),
        kafka_flush_timeout_sec=int(os.getenv("KAFKA_FLUSH_TIMEOUT_SEC", "30")),
        produce_retry_timeout_sec=int(os.getenv("PRODUCE_RETRY_TIMEOUT_SEC", "60")),
        produce_retry_poll_sec=float(os.getenv("PRODUCE_RETRY_POLL_SEC", "0.2")),
        topic_per_table=_str_to_bool(os.getenv("TOPIC_PER_TABLE", "false"), False),
        topic_prefix=os.getenv("TOPIC_PREFIX", "").strip(),
        topic_separator=os.getenv("TOPIC_SEPARATOR", ".").strip() or ".",
        state_file=os.getenv("STATE_FILE", "./oracle_kafka_state_archivelog.json").strip(),
        start_from_commit_scn=start_from_commit_scn,
        poll_seconds=int(os.getenv("POLL_SECONDS", "60")),
        filter_schema=os.getenv("FILTER_SCHEMA", "").strip(),
        filter_table=os.getenv("FILTER_TABLE", "").strip(),
        filter_schemas=filter_schemas,
        filter_tables=filter_tables,
        max_rows_per_batch=int(os.getenv("MAX_ROWS_PER_BATCH", "5000")),
        use_fetchmany=_str_to_bool(os.getenv("USE_FETCHMANY", "true"), True),
        fetchmany_size=int(os.getenv("FETCHMANY_SIZE", "1000")),
        verbose=_str_to_bool(os.getenv("VERBOSE", "true"), True),
        log_first_n_events=int(os.getenv("LOG_FIRST_N_EVENTS", "3")),
    )


def validate_config(cfg: Config) -> None:
    # Валидируем обязательные поля до запуска.
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


def _load_state(cfg: Config) -> Dict[str, int]:
    # Загружаем last_commit_scn из state-файла, если он есть.
    path = Path(cfg.state_file)
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_commit_scn": cfg.start_from_commit_scn}


def _save_state(cfg: Config, last_commit_scn: int) -> None:
    # Сохраняем новый watermark после успешного батча.
    path = Path(cfg.state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "last_commit_scn": last_commit_scn,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            f,
        )


def _build_kafka_producer(cfg: Config) -> Producer:
    # Собираем конфиг Producer из Config.
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
        conf["ssl.endpoint.identification.algorithm"] = (
            "https" if cfg.ssl_check_hostname else "none"
        )

    if proto in {"SASL_SSL", "SASL_PLAINTEXT"}:
        conf["sasl.mechanism"] = cfg.kafka_sasl_mechanism
        conf["sasl.username"] = cfg.kafka_sasl_username
        conf["sasl.password"] = cfg.kafka_sasl_password

    return Producer(conf)


def _end_logminer(cur: oracledb.Cursor) -> None:
    # Безопасно завершаем текущую сессию LogMiner.
    cur.execute(
        """
        BEGIN
          DBMS_LOGMNR.END_LOGMNR;
        EXCEPTION
          WHEN OTHERS THEN NULL;
        END;
        """
    )


def _get_archived_logs(
    cur: oracledb.Cursor,
    from_commit_scn: int,
    limit_rows: int,
) -> List[Dict[str, Any]]:
    def to_int_or_none(value: Any) -> Optional[int]:
        # Безопасно приводим Oracle-значение к int:
        # - NULL в БД остается None;
        # - непустое значение приводим к int.
        return None if value is None else int(value)

    def fetch_dict_rows(sql_text: str, binds: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Используем rowfactory, чтобы получать строки сразу как dict по именам колонок.
        # Это делает код заметно читаемее и убирает зависимость от row[0], row[1], ...
        previous_rowfactory = cur.rowfactory
        try:
            cur.execute(sql_text, binds)
            columns = [col[0].lower() for col in cur.description]
            cur.rowfactory = lambda *args: dict(zip(columns, args))
            rows = cur.fetchall()
            normalized: List[Dict[str, Any]] = []
            for row in rows:
                normalized.append(
                    {
                        "thread": to_int_or_none(row.get("thread#")),
                        "sequence": to_int_or_none(row.get("sequence#")),
                        "first_change": to_int_or_none(row.get("first_change#")),
                        "next_change": to_int_or_none(row.get("next_change#")),
                        "name": row.get("name"),
                    }
                )
            return normalized
        finally:
            cur.rowfactory = previous_rowfactory

    # Выбираем archived logs "от watermark", а не просто "последние N":
    # 1) anchor: лог, который покрывает from_commit_scn;
    # 2) tail: последовательный хвост логов после anchor по sequence#.
    #
    # Почему это важно:
    # - если брать только "последние N", можно пропустить нужный диапазон SCN;
    # - anchor+tail дает непрерывную цепочку от текущей точки state.
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
      SELECT
        l.thread#,
        l.sequence#,
        l.first_change#,
        l.next_change#,
        l.name
      FROM v$archived_log l
      JOIN anchor a
        ON l.thread# = a.thread#
       AND l.sequence# >= a.sequence#
      WHERE l.name IS NOT NULL
      ORDER BY l.sequence# ASC
    )
    SELECT thread#, sequence#, first_change#, next_change#, name
    FROM (
      SELECT thread#, sequence#, first_change#, next_change#, name
      FROM tail
    )
    WHERE ROWNUM <= :limit_rows
    """

    rows = fetch_dict_rows(
        sql,
        {
            "from_scn": int(from_commit_scn),
            "limit_rows": int(limit_rows),
        },
    )
    if rows:
        return rows

    # Отдельный fallback только для самого первого запуска:
    # если from_commit_scn <= 0, нет "якоря" и можно взять последние N как bootstrap.
    # Для нормального прод-режима from_commit_scn должен быть валидным и попадать в archived logs.
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
        bootstrap_rows = fetch_dict_rows(bootstrap_sql, {"limit_rows": int(limit_rows)})
        # Приводим к хронологическому порядку.
        return list(reversed(bootstrap_rows))

    # Если anchor не найден при from_commit_scn > 0:
    # это значит, что нужный SCN диапазон уже отсутствует в archived logs
    # (например, логи удалены из-за retention), и продолжать небезопасно.
    raise RuntimeError(
        "Cannot locate archived logs for state SCN "
        f"{from_commit_scn}. Required SCN range is not available in v$archived_log."
    )


def _add_logfile(cur: oracledb.Cursor, logfile: str, first: bool) -> None:
    # Первый файл добавляется как NEW, остальные как ADDFILE.
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
    # Стартуем LogMiner на выбранном наборе archived logs.
    if not log_files:
        raise RuntimeError("No archived logs found in v$archived_log")

    _end_logminer(cur)
    for idx, logfile in enumerate(log_files):
        _add_logfile(cur, logfile, first=(idx == 0))

    options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
    if cfg.use_no_rowid_in_stmt:
        options += " + DBMS_LOGMNR.NO_ROWID_IN_STMT"

    cur.call_timeout = cfg.call_timeout_ms
    cur.execute(
        f"""
        BEGIN
          DBMS_LOGMNR.START_LOGMNR(
            OPTIONS => {options}
          );
        END;
        """
    )


def _build_fetch_rows_query(
    from_commit_scn: int,
    filter_schemas: Sequence[str],
    filter_tables: Sequence[str],
    max_rows_per_batch: int,
) -> Tuple[str, Dict[str, Any]]:
    # Эта функция только строит SQL + bind-параметры.
    # Выполнение запроса вынесено в run_once(), чтобы можно было:
    # - читать курсор потоково (fetchmany),
    # - публиковать в Kafka "на лету", без накопления всего батча в памяти.
    # Базовый SQL: только DML после watermark commit_scn.
    base_sql = """
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
      AND commit_scn IS NOT NULL
      AND commit_scn > :from_commit_scn
    """
    binds: Dict[str, Any] = {"from_commit_scn": from_commit_scn}

    if filter_schemas:
        schema_placeholders = []
        for idx, schema_name in enumerate(filter_schemas):
            bind_name = f"filter_schema_{idx}"
            schema_placeholders.append(f":{bind_name}")
            binds[bind_name] = schema_name
        base_sql += f" AND seg_owner IN ({', '.join(schema_placeholders)})"

    if filter_tables:
        table_placeholders = []
        for idx, table_name in enumerate(filter_tables):
            bind_name = f"filter_table_{idx}"
            table_placeholders.append(f":{bind_name}")
            binds[bind_name] = table_name
        base_sql += f" AND table_name IN ({', '.join(table_placeholders)})"

    order_by = " ORDER BY commit_scn, sequence#, rs_id, ssn"

    if max_rows_per_batch > 0:
        # Важный момент для надежности:
        # MAX_ROWS_PER_BATCH не должен "резать" один commit на две итерации.
        #
        # Логика:
        # 1) нумеруем строки в стабильном порядке;
        # 2) находим commit_scn строки на позиции max_rows_per_batch (через MAX(commit_scn) по rn<=N);
        # 3) возвращаем все строки с commit_scn <= cutoff_commit_scn.
        #
        # Итог: размер батча может быть чуть больше N, но commit всегда полный.
        #
        # Зачем это нужно:
        # - если порезать commit посередине, в следующем батче можно получить
        #   неконсистентный срез данных и сложнее делать дедупликацию/resume.
        sql = f"""
        WITH base_rows AS (
          {base_sql}
        ),
        ranked_rows AS (
          SELECT
            b.*,
            ROW_NUMBER() OVER (ORDER BY commit_scn, redo_sequence, rs_id, ssn) AS rn
          FROM base_rows b
        ),
        cutoff AS (
          SELECT MAX(commit_scn) AS cutoff_commit_scn
          FROM ranked_rows
          WHERE rn <= :max_rows_per_batch
        )
        SELECT
          r.commit_scn,
          r.scn,
          r.timestamp,
          r.seg_owner,
          r.table_name,
          r.operation,
          r.operation_code,
          r.redo_sequence,
          r.rs_id,
          r.ssn,
          r.sql_redo,
          r.sql_undo
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
    # Преобразуем строку Oracle в словарь и нормализуем timestamp для JSON.
    row = dict(zip(columns, record))
    ts = row.get("timestamp")
    if isinstance(ts, datetime):
        row["timestamp"] = ts.isoformat()
    return row


def _event_key(row: Dict[str, Any]) -> bytes:
    # Детерминированный key события для Kafka.
    key = (
        f"{row.get('seg_owner','')}.{row.get('table_name','')}|"
        f"{row.get('commit_scn','')}|{row.get('redo_sequence','')}|"
        f"{row.get('rs_id','')}|{row.get('ssn','')}"
    )
    return key.encode("utf-8")


def _sanitize_topic_part(value: str) -> str:
    # Нормализуем часть topic name:
    # - lower-case;
    # - все небезопасные символы -> "_".
    safe_chars = []
    for ch in str(value).strip().lower():
        if ch.isalnum() or ch in {"-", "_", "."}:
            safe_chars.append(ch)
        else:
            safe_chars.append("_")
    result = "".join(safe_chars).strip("._-")
    return result or "unknown"


def _resolve_topic(cfg: Config, row: Dict[str, Any]) -> str:
    # Выбираем topic:
    # - обычный режим: cfg.kafka_topic;
    # - topic per table: [prefix.]schema.table
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
    # Безопасная отправка: queue full -> poll + retry до дедлайна.
    retries = 0
    waited_sec = 0.0
    started = time.monotonic()
    deadline = started + max(float(cfg.produce_retry_timeout_sec), 0.0)

    while True:
        try:
            producer.produce(topic, key=key, value=value, callback=callback)
            return retries, waited_sec
        except BufferError:
            now = time.monotonic()
            if now >= deadline:
                raise RuntimeError(
                    "Kafka local queue remained full until retry timeout "
                    f"({cfg.produce_retry_timeout_sec}s)"
                ) from None
            producer.poll(cfg.produce_retry_poll_sec)
            retries += 1
            waited_sec = now - started


def run_once(cfg: Config) -> Dict[str, Any]:
    # Объединяем legacy одиночные фильтры со списками.
    merged_schemas = _merge_name_filters(cfg.filter_schema, cfg.filter_schemas)
    merged_tables = _merge_name_filters(cfg.filter_table, cfg.filter_tables)

    # 1) Загружаем состояние.
    state = _load_state(cfg)
    last_commit_scn = int(state.get("last_commit_scn", cfg.start_from_commit_scn))
    from_commit_scn = last_commit_scn
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

    # 2) Инициализация producer и счетчиков.
    # Счетчики держим здесь, чтобы собрать:
    # - delivery-статистику Kafka,
    # - метрики backpressure,
    # - SCN-прогресс батча.
    producer = _build_kafka_producer(cfg)
    delivered = 0
    failed = 0
    queue_full_retries = 0
    queue_full_wait_sec = 0.0
    topics_used: Dict[str, int] = {}
    timings: Dict[str, float] = {}
    t0 = time.monotonic()

    def on_delivery(err, _msg):
        nonlocal delivered, failed
        if err is not None:
            failed += 1
        else:
            delivered += 1

    conn: Optional[oracledb.Connection] = None
    cur: Optional[oracledb.Cursor] = None
    logs: List[str] = []
    try:
        # 3) Connect to Oracle + check access.
        t_connect = time.monotonic()
        conn = oracledb.connect(user=cfg.oracle_user, password=cfg.oracle_password, dsn=cfg.oracle_dsn)
        timings["oracle_connect_sec"] = time.monotonic() - t_connect
        _log(cfg, f"connected to Oracle DSN={cfg.oracle_dsn}")

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM v$logmnr_contents WHERE 1 = 0")
        _ = cur.fetchone()
        _log(cfg, "access check to v$logmnr_contents passed")

        # 4) Выбираем archived logs от state SCN и стартуем LogMiner.
        t_logs = time.monotonic()
        archived_logs = _get_archived_logs(
            cur=cur,
            from_commit_scn=last_commit_scn,
            limit_rows=cfg.archive_log_limit,
        )
        logs = [str(item["name"]) for item in archived_logs if item.get("name")]
        timings["select_archived_logs_sec"] = time.monotonic() - t_logs
        _log(cfg, f"archived logs selected: {len(logs)}")
        for item in archived_logs:
            _log(
                cfg,
                (
                    "  logfile: "
                    f"name={item.get('name')} "
                    f"thread={item.get('thread')} "
                    f"sequence={item.get('sequence')} "
                    f"first_change={item.get('first_change')} "
                    f"next_change={item.get('next_change')}"
                ),
            )

        t_logminer = time.monotonic()
        _start_logminer(cur, cfg, logs)
        timings["start_logminer_sec"] = time.monotonic() - t_logminer
        _log(cfg, "LogMiner started")

        # 5) Готовим SQL для чтения строк из LogMiner.
        # Важно: только подготовка SQL; само чтение будет потоковым ниже.
        t_prepare_fetch = time.monotonic()
        sql, binds = _build_fetch_rows_query(
            from_commit_scn=last_commit_scn,
            filter_schemas=merged_schemas,
            filter_tables=merged_tables,
            max_rows_per_batch=cfg.max_rows_per_batch,
        )
        timings["prepare_fetch_cursor_sec"] = time.monotonic() - t_prepare_fetch

        # 6) Читаем и публикуем потоково.
        #
        # use_fetchmany=true:
        #   читаем курсор чанками, не держим весь батч в памяти.
        # use_fetchmany=false:
        #   читаем cur.fetchall() (legacy-поведение).
        t_fetch_total = 0.0
        t_publish_total = 0.0
        rows_count = 0
        first_batch_commit_scn: Optional[int] = None
        last_batch_commit_scn: Optional[int] = None

        cur.execute(sql, binds)
        columns = [c[0].lower() for c in cur.description]

        if cfg.use_fetchmany:
            _log(cfg, f"fetch mode=fetchmany, fetchmany_size={cfg.fetchmany_size}")
            while True:
                # Каждый fetchmany возвращает ограниченный кусок строк,
                # что стабилизирует память при больших батчах/широких SQL_REDO.
                t_fetch_chunk = time.monotonic()
                records = cur.fetchmany(cfg.fetchmany_size)
                t_fetch_total += time.monotonic() - t_fetch_chunk
                if not records:
                    break

                for record in records:
                    # Нормализуем строку Oracle -> dict и сразу публикуем.
                    # Таким образом, в Python не накапливается весь батч целиком.
                    row = _record_to_row(columns, record)
                    rows_count += 1
                    commit_scn = int(row["commit_scn"])
                    if first_batch_commit_scn is None:
                        first_batch_commit_scn = commit_scn
                    last_batch_commit_scn = commit_scn

                    t_publish_one = time.monotonic()
                    value = json.dumps(row, ensure_ascii=False).encode("utf-8")
                    topic = _resolve_topic(cfg, row)
                    retries, waited = _produce_with_backpressure(
                        cfg=cfg,
                        producer=producer,
                        topic=topic,
                        key=_event_key(row),
                        value=value,
                        callback=on_delivery,
                    )
                    t_publish_total += time.monotonic() - t_publish_one

                    queue_full_retries += retries
                    queue_full_wait_sec += waited
                    topics_used[topic] = topics_used.get(topic, 0) + 1
                    # producer.poll(0) обслуживает callback delivery без блокировки.
                    producer.poll(0)

                    if rows_count <= cfg.log_first_n_events:
                        _log(
                            cfg,
                            (
                                "queued event "
                                f"topic={topic} "
                                f"commit_scn={row.get('commit_scn')} "
                                f"schema={row.get('seg_owner')} table={row.get('table_name')} "
                                f"operation={row.get('operation')}"
                            ),
                        )
        else:
            _log(cfg, "fetch mode=fetchall (legacy)")
            # Legacy-режим оставлен как feature-flag для быстрого отката поведения.
            t_fetch_all = time.monotonic()
            records = cur.fetchall()
            t_fetch_total += time.monotonic() - t_fetch_all

            for record in records:
                row = _record_to_row(columns, record)
                rows_count += 1
                commit_scn = int(row["commit_scn"])
                if first_batch_commit_scn is None:
                    first_batch_commit_scn = commit_scn
                last_batch_commit_scn = commit_scn

                t_publish_one = time.monotonic()
                value = json.dumps(row, ensure_ascii=False).encode("utf-8")
                topic = _resolve_topic(cfg, row)
                retries, waited = _produce_with_backpressure(
                    cfg=cfg,
                    producer=producer,
                    topic=topic,
                    key=_event_key(row),
                    value=value,
                    callback=on_delivery,
                )
                t_publish_total += time.monotonic() - t_publish_one

                queue_full_retries += retries
                queue_full_wait_sec += waited
                topics_used[topic] = topics_used.get(topic, 0) + 1
                # producer.poll(0) обслуживает callback delivery без блокировки.
                producer.poll(0)

                if rows_count <= cfg.log_first_n_events:
                    _log(
                        cfg,
                        (
                            "queued event "
                            f"topic={topic} "
                            f"commit_scn={row.get('commit_scn')} "
                            f"schema={row.get('seg_owner')} table={row.get('table_name')} "
                            f"operation={row.get('operation')}"
                        ),
                    )

        timings["fetch_rows_sec"] = t_fetch_total
        timings["publish_rows_sec"] = t_publish_total
        _log(cfg, f"rows fetched from v$logmnr_contents: {rows_count}")

        # 7) Flush и проверка ошибок delivery.
        # После flush считаем батч успешным только если failed == 0.
        # Это важно для one-shot режима: иначе cron увидит ложный успех.
        t_flush = time.monotonic()
        producer.flush(cfg.kafka_flush_timeout_sec)
        timings["flush_sec"] = time.monotonic() - t_flush
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

        # 8) Обновляем state watermark.
        # State пишем только после успешного flush и нулевого failed.
        # Это защищает от "подтверждения прогресса" при частично доставленных сообщениях.
        if rows_count > 0:
            # commit_scn гарантированно NOT NULL по условию выборки в _build_fetch_rows_query.
            # Данные отсортированы по commit_scn ASC, поэтому last_batch_commit_scn — максимум.
            new_commit_scn = int(last_batch_commit_scn)
            last_commit_scn = max(last_commit_scn, new_commit_scn)
            _save_state(cfg, last_commit_scn)
            _log(cfg, f"state updated: last_commit_scn={last_commit_scn}")
        else:
            _log(cfg, "no new rows, state not changed")

        # 9) Сводные SCN-метрики батча.
        # Нужны для быстрого ответа на вопросы:
        # - какой диапазон SCN мы обработали;
        # - есть ли прогресс state;
        # - насколько отстаем от хвоста выбранного окна archived logs.
        #
        # from_commit_scn:
        #   SCN из state ДО запуска батча (точка старта).
        # to_commit_scn:
        #   SCN в state ПОСЛЕ батча (точка окончания).
        # processed_scn_span:
        #   Насколько продвинулись за батч: to - from.
        # logs_min_first_change / logs_max_next_change:
        #   Границы SCN-окна по выбранным archived logs.
        # lag_to_logs_tail_scn:
        #   Приблизительное отставание от хвоста выбранного окна:
        #   logs_max_next_change - to_commit_scn (если next_change доступен).
        # Фактические границы commit_scn в самом батче (по реальным строкам).
        # Значения уже рассчитаны в потоковой обработке выше.

        # Границы SCN по выбранным archived logs.
        log_first_changes = [int(item["first_change"]) for item in archived_logs if item.get("first_change") is not None]
        log_next_changes = [int(item["next_change"]) for item in archived_logs if item.get("next_change") is not None]
        logs_min_first_change = min(log_first_changes) if log_first_changes else None
        logs_max_next_change = max(log_next_changes) if log_next_changes else None

        # Основные итоговые индикаторы прогресса.
        processed_scn_span = max(0, int(last_commit_scn) - int(from_commit_scn))
        lag_to_logs_tail_scn = (
            max(0, int(logs_max_next_change) - int(last_commit_scn))
            if logs_max_next_change is not None
            else None
        )
        state_advanced = int(last_commit_scn) > int(from_commit_scn)

        # Единый объект метрик для лога и итогового result.
        scn_metrics = {
            "from_commit_scn": int(from_commit_scn),
            "first_batch_commit_scn": first_batch_commit_scn,
            "last_batch_commit_scn": last_batch_commit_scn,
            "to_commit_scn": int(last_commit_scn),
            "processed_scn_span": processed_scn_span,
            "logs_min_first_change": logs_min_first_change,
            "logs_max_next_change": logs_max_next_change,
            "lag_to_logs_tail_scn": lag_to_logs_tail_scn,
            "state_advanced": state_advanced,
        }
        # Короткая строка для оперативного просмотра в cron/docker логах.
        # Она специально компактная, чтобы легко читать глазами и парсить grep'ом.
        _log(
            cfg,
            (
                "scn summary "
                f"(from={scn_metrics['from_commit_scn']}, "
                f"to={scn_metrics['to_commit_scn']}, "
                f"span={scn_metrics['processed_scn_span']}, "
                f"lag_to_logs_tail={scn_metrics['lag_to_logs_tail_scn']}, "
                f"state_advanced={scn_metrics['state_advanced']})"
            ),
        )

        # 10) Формируем итоговый result для логов/интеграций.
        timings["total_sec"] = time.monotonic() - t0
        result = {
            "rows": rows_count,
            "delivered": delivered,
            "failed": failed,
            "queue_full_retries": queue_full_retries,
            "queue_full_wait_sec": round(queue_full_wait_sec, 3),
            "last_commit_scn": last_commit_scn,
            "logs_used": logs,
            "topics_used": topics_used,
            "scn_metrics": scn_metrics,
            "timings_sec": {k: round(v, 3) for k, v in timings.items()},
        }
        _log(cfg, f"batch finished: {result}")
        return result
    finally:
        # 11) Гарантированный cleanup.
        # Выполняем даже при исключениях, чтобы не оставлять висящие ресурсы.
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
            "start one-shot batch "
            f"(dsn={cfg.oracle_dsn}, topic={cfg.kafka_topic}, "
            f"archive_log_limit={cfg.archive_log_limit}, state_file={cfg.state_file})"
        ),
    )
    try:
        stats = run_once(cfg)
        _log(cfg, f"one-shot finished: {stats}")
        return 0
    except Exception as exc:
        print(f"[oracle->kafka:archivelog] ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
