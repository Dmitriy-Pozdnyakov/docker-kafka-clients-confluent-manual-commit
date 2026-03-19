"""Schema Registry + CDC envelope utilities for Oracle LogMiner SR-CDC runner.

Вынесено в отдельный модуль, чтобы основной runtime-скрипт
оставался компактным и читабельным.

Структура модуля:
1) `SchemaRuntime`:
   - лениво создает serializer key/value для каждого topic;
   - читает JSON Schema из локальной папки;
   - сериализует key/value в wire-format Confluent SR.
2) Метаданные таблиц:
   - `load_table_metadata()` вытягивает колонки и PK;
   - используется для режима ключа `cdc_key_mode=pk`.
3) Прототипный parser SQL_REDO/SQL_UNDO:
   - поддержка INSERT/UPDATE/DELETE только в базовых шаблонах;
   - для сложных SQL runtime-скрипт может сделать fallback в raw-mode.
4) `build_cdc_event()`:
   - собирает итоговые key/value для CDC envelope.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import oracledb

try:
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.json_schema import JSONSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext
except Exception:  # pragma: no cover
    SchemaRegistryClient = None
    JSONSerializer = None
    SerializationContext = None
    MessageField = None

# Простые алиасы для читаемости сигнатур.
RowDict = Dict[str, Any]
TableMeta = Dict[str, Any]


def schema_registry_dependencies_available() -> bool:
    """Проверяет, доступен ли стек Schema Registry в установленном confluent-kafka."""
    return all(
        item is not None
        for item in (SchemaRegistryClient, JSONSerializer, SerializationContext, MessageField)
    )


class SchemaRuntime:
    """Lazy-загрузка сериализаторов по topic из локальных schema-файлов.

    Для topic `oracle.cdc.hr.employees` ожидаем файлы:
    - ./schemas/oracle.cdc.hr.employees.key.json
    - ./schemas/oracle.cdc.hr.employees.value.json

    Это прототип: только URL, без SSL/auth к SR.
    """

    def __init__(self, cfg: Any):
        # Ленивые кэши сериализаторов по topic.
        # Это снижает overhead: схема читается/инициализируется один раз на topic.
        self.cfg = cfg
        self._key_serializers: Dict[str, Any] = {}
        self._value_serializers: Dict[str, Any] = {}

        if not cfg.cdc_envelope_enabled:
            self.client = None
            return

        if not schema_registry_dependencies_available():
            raise RuntimeError("Schema Registry dependencies are unavailable")

        self.client = SchemaRegistryClient({"url": cfg.schema_registry_url})

    def _schema_file_path(self, topic: str, kind: str) -> Path:
        return Path(self.cfg.schema_dir) / f"{topic}.{kind}.json"

    def _load_schema_text(self, topic: str, kind: str) -> str:
        path = self._schema_file_path(topic, kind)
        if not path.exists():
            raise RuntimeError(f"Schema file not found: {path}")
        return path.read_text(encoding="utf-8")

    def _json_serializer(self, schema_str: str) -> Any:
        return JSONSerializer(
            schema_str=schema_str,
            schema_registry_client=self.client,
            to_dict=lambda obj, _ctx: obj,
            conf={
                "auto.register.schemas": self.cfg.schema_auto_register,
                "use.latest.version": self.cfg.schema_use_latest_version,
                "normalize.schemas": self.cfg.schema_normalize,
            },
        )

    def key_serializer(self, topic: str) -> Any:
        if topic not in self._key_serializers:
            self._key_serializers[topic] = self._json_serializer(self._load_schema_text(topic, "key"))
        return self._key_serializers[topic]

    def value_serializer(self, topic: str) -> Any:
        if topic not in self._value_serializers:
            self._value_serializers[topic] = self._json_serializer(self._load_schema_text(topic, "value"))
        return self._value_serializers[topic]

    def serialize_key(self, topic: str, key_obj: RowDict) -> bytes:
        if not self.cfg.cdc_envelope_enabled:
            raise RuntimeError("serialize_key called while CDC_ENVELOPE_ENABLED=false")
        serializer = self.key_serializer(topic)
        return serializer(key_obj, SerializationContext(topic, MessageField.KEY))

    def serialize_value(self, topic: str, value_obj: RowDict) -> bytes:
        if not self.cfg.cdc_envelope_enabled:
            raise RuntimeError("serialize_value called while CDC_ENVELOPE_ENABLED=false")
        serializer = self.value_serializer(topic)
        return serializer(value_obj, SerializationContext(topic, MessageField.VALUE))


def qualified_table_name(row: RowDict) -> str:
    """Возвращает OWNER.TABLE в upper-case."""
    return f"{str(row.get('seg_owner', '')).upper()}.{str(row.get('table_name', '')).upper()}"


def is_cdc_table_enabled(cfg: Any, row: RowDict) -> bool:
    """Фильтрует таблицы для CDC path по списку CDC_SUPPORTED_TABLES."""
    if not cfg.cdc_supported_tables:
        return True
    return qualified_table_name(row) in cfg.cdc_supported_tables


_TABLE_META_CACHE: Dict[str, TableMeta] = {}


def load_table_metadata(conn: oracledb.Connection, owner: str, table: str) -> TableMeta:
    """Грузит метаданные таблицы (колонки + PK) с кэшированием.

    Эти метаданные используются для построения CDC key/value,
    прежде всего для режима ключа `cdc_key_mode=pk`.
    """
    cache_key = f"{owner.upper()}.{table.upper()}"
    if cache_key in _TABLE_META_CACHE:
        return _TABLE_META_CACHE[cache_key]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, nullable, column_id
            FROM all_tab_columns
            WHERE owner = :owner AND table_name = :table
            ORDER BY column_id
            """,
            {"owner": owner.upper(), "table": table.upper()},
        )
        columns = [
            {
                "name": str(name).upper(),
                "oracle_type": str(data_type).upper(),
                "nullable": str(nullable).upper() == "Y",
                "column_id": int(column_id),
            }
            for name, data_type, nullable, column_id in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT c.column_name, c.position
            FROM all_constraints a
            JOIN all_cons_columns c
              ON a.owner = c.owner
             AND a.constraint_name = c.constraint_name
             AND a.table_name = c.table_name
            WHERE a.owner = :owner
              AND a.table_name = :table
              AND a.constraint_type = 'P'
            ORDER BY c.position
            """,
            {"owner": owner.upper(), "table": table.upper()},
        )
        pk_columns = [str(column_name).upper() for column_name, _position in cur.fetchall()]

    result = {
        "owner": owner.upper(),
        "table": table.upper(),
        "columns": columns,
        "pk_columns": pk_columns,
    }
    _TABLE_META_CACHE[cache_key] = result
    return result


# -------------------------
# Prototype DML parser layer
# -------------------------
# Ограниченный и консервативный парсер.
# Для unsupported SQL вызываем fallback или fail в зависимости от CDC_PARSE_ERROR_MODE.
#
# Ниже три ключевых regex:
# 1) INSERT: вытаскиваем "(col1, col2, ...)" и "(val1, val2, ...)".
# 2) UPDATE: делим SQL на "set_clause" и "where_clause".
# 3) ASSIGNMENT: разбираем элемент вида COL=VALUE или "COL"=VALUE.

# Пример ожидаемого SQL:
# insert into HR.EMP (ID, NAME) values (1, 'Ann')
# groups:
# - columns -> "ID, NAME"
# - values  -> "1, 'Ann'"
_INSERT_VALUES_RE = re.compile(
    r"insert\s+into\s+.+?\((?P<columns>.+?)\)\s+values\s*\((?P<values>.+?)\)",
    flags=re.IGNORECASE | re.DOTALL,
)

# Пример ожидаемого SQL:
# update HR.EMP set NAME='Bob', SAL=100 where ID=1
# groups:
# - set_clause   -> "NAME='Bob', SAL=100"
# - where_clause -> "ID=1"
_UPDATE_SET_RE = re.compile(
    r"update\s+.+?\s+set\s+(?P<set_clause>.+?)\s+where\s+(?P<where_clause>.+?)\s*;?\s*$",
    flags=re.IGNORECASE | re.DOTALL,
)

# Пример ожидаемого элемента:
# "NAME"='Bob'  -> column=NAME, value='Bob'
# SAL=100       -> column=SAL,  value=100
_ASSIGNMENT_RE = re.compile(r'"?(?P<column>[A-Za-z0-9_#$]+)"?\s*=\s*(?P<value>.+)$', flags=re.DOTALL)


def _split_csv_sql(text: str) -> List[str]:
    """Разбивает SQL-список по запятым с учетом строковых литералов."""
    items: List[str] = []
    buf: List[str] = []
    in_string = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "'":
            buf.append(ch)
            if in_string and i + 1 < len(text) and text[i + 1] == "'":
                buf.append(text[i + 1])
                i += 2
                continue
            in_string = not in_string
        elif ch == "," and not in_string:
            items.append("".join(buf).strip())
            buf = []
            i += 1
            continue
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        items.append(tail)
    return items


def _sql_literal_to_python(token: str) -> Any:
    """Грубое преобразование SQL-литерала в python-значение для прототипа парсера."""
    token = token.strip()
    if not token:
        return None
    upper = token.upper()
    if upper == "NULL":
        return None
    if upper.startswith("TO_DATE(") or upper.startswith("TO_TIMESTAMP("):
        return token
    if token.startswith("'") and token.endswith("'"):
        return token[1:-1].replace("''", "'")
    if re.fullmatch(r"[-+]?\d+", token):
        try:
            return int(token)
        except Exception:
            return token
    if re.fullmatch(r"[-+]?\d+\.\d+", token):
        try:
            return float(token)
        except Exception:
            return token
    return token


def _parse_assignment_map(clause: str) -> RowDict:
    """Парсит выражения вида `COL=VALUE, ...` в dict."""
    result: RowDict = {}
    for item in _split_csv_sql(clause):
        match = _ASSIGNMENT_RE.match(item.strip())
        if not match:
            continue
        col = match.group("column").strip().upper()
        result[col] = _sql_literal_to_python(match.group("value"))
    return result


def _parse_insert_sql(sql_redo: str) -> RowDict:
    """Прототипный parser INSERT ... VALUES (...)."""
    match = _INSERT_VALUES_RE.search(sql_redo.strip())
    if not match:
        raise RuntimeError("Unsupported INSERT SQL_REDO pattern for prototype parser")
    columns = [part.strip().strip('"').upper() for part in _split_csv_sql(match.group("columns"))]
    values = [_sql_literal_to_python(part) for part in _split_csv_sql(match.group("values"))]
    if len(columns) != len(values):
        raise RuntimeError("INSERT parser mismatch: columns/value counts differ")
    return dict(zip(columns, values))


def _parse_update_sql(sql_redo: str, sql_undo: str) -> Tuple[RowDict, RowDict]:
    """Прототипный parser UPDATE: строит `before` из undo и `after` из redo."""
    redo_match = _UPDATE_SET_RE.search(sql_redo.strip())
    undo_match = _UPDATE_SET_RE.search(sql_undo.strip())
    if not redo_match or not undo_match:
        raise RuntimeError("Unsupported UPDATE SQL pattern for prototype parser")

    before_row: RowDict = {}
    after_row: RowDict = {}

    before_row.update(_parse_assignment_map(undo_match.group("set_clause")))
    before_row.update(_parse_assignment_map(undo_match.group("where_clause")))

    after_row.update(before_row)
    after_row.update(_parse_assignment_map(redo_match.group("set_clause")))
    after_row.update(_parse_assignment_map(redo_match.group("where_clause")))

    return before_row, after_row


def _parse_delete_sql(sql_undo: str) -> RowDict:
    # В прототипе для DELETE ожидаем, что sql_undo будет INSERT ... VALUES (...)
    return _parse_insert_sql(sql_undo)


def _build_source_block(row: RowDict) -> RowDict:
    """Формирует source-блок CDC envelope из служебных полей LogMiner."""
    ts_ms: Optional[int] = None
    ts_value = row.get("timestamp")
    if isinstance(ts_value, str):
        try:
            ts_ms = int(datetime.fromisoformat(ts_value).timestamp() * 1000)
        except Exception:
            ts_ms = None

    return {
        "schema": str(row.get("seg_owner", "")).upper(),
        "table": str(row.get("table_name", "")).upper(),
        "commit_scn": int(row.get("commit_scn") or 0),
        "redo_sequence": int(row.get("redo_sequence") or 0),
        "rs_id": str(row.get("rs_id") or ""),
        "ssn": int(row.get("ssn") or 0),
        "ts_ms": ts_ms,
    }


def _build_pk_key(row_image: RowDict, table_meta: TableMeta) -> RowDict:
    """Собирает key объект по PK колонкам таблицы."""
    pk_columns = [str(col).upper() for col in table_meta.get("pk_columns", [])]
    if not pk_columns:
        raise RuntimeError(
            f"No PK metadata found for {table_meta.get('owner')}.{table_meta.get('table')}"
        )
    key_obj: RowDict = {}
    for pk_col in pk_columns:
        if pk_col not in row_image:
            raise RuntimeError(f"PK column {pk_col} not found in parsed row image")
        key_obj[pk_col] = row_image[pk_col]
    return key_obj


def _build_technical_key_object(row: RowDict) -> RowDict:
    """Технический fallback-key (schema/table + позиция в redo stream)."""
    return {
        "schema": str(row.get("seg_owner", "")).upper(),
        "table": str(row.get("table_name", "")).upper(),
        "commit_scn": int(row.get("commit_scn") or 0),
        "redo_sequence": int(row.get("redo_sequence") or 0),
        "rs_id": str(row.get("rs_id") or ""),
        "ssn": int(row.get("ssn") or 0),
    }


def build_cdc_event(row: RowDict, table_meta: TableMeta, cfg: Any) -> Tuple[RowDict, RowDict]:
    """Строит CDC key/value для SR serialization.

    Поддерживает INSERT/UPDATE/DELETE в рамках прототипного парсера.
    Для сложных SQL/DDL этот parser может не подойти — тогда runtime-скрипт
    может сделать fallback в raw-режим (если включено).
    """
    operation = str(row.get("operation") or "").strip().upper()
    sql_redo = str(row.get("sql_redo") or "")
    sql_undo = str(row.get("sql_undo") or "")

    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    op_code: str

    # Приводим Oracle operation к CDC op-кодам:
    # INSERT -> c (create), UPDATE -> u (update), DELETE -> d (delete)
    if operation == "INSERT":
        op_code = "c"
        after = _parse_insert_sql(sql_redo)
    elif operation == "UPDATE":
        op_code = "u"
        before, after = _parse_update_sql(sql_redo, sql_undo)
    elif operation == "DELETE":
        op_code = "d"
        before = _parse_delete_sql(sql_undo)
    else:
        raise RuntimeError(f"Unsupported operation for CDC envelope: {operation!r}")

    key_basis = after or before or {}
    key_obj = (
        _build_pk_key(key_basis, table_meta)
        if cfg.cdc_key_mode == "pk"
        else _build_technical_key_object(row)
    )

    value_obj = {
        "op": op_code,
        "source": _build_source_block(row),
        "before": before,
        "after": after,
    }
    return key_obj, value_obj
