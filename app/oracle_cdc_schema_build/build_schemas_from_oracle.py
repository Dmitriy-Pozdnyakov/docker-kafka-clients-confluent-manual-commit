#!/usr/bin/env python3
"""Build JSON Schema files from Oracle metadata (ALL_TAB_COLUMNS + PK).

Назначение:
1) Считать структуру таблиц из Oracle.
2) Сгенерировать `<topic>.key.json` и `<topic>.value.json`.
3) (Опционально) зарегистрировать схемы в Schema Registry.

Это отдельный pre-deploy шаг (CI/CD), а не runtime-логика продюсера.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import oracledb
except Exception:  # pragma: no cover
    oracledb = None


def _parse_csv(raw: str) -> List[str]:
    """Парсит CSV-строку в уникальный список значений (с сохранением порядка)."""
    values: List[str] = []
    for item in raw.split(","):
        normalized = item.strip()
        if normalized and normalized not in values:
            values.append(normalized)
    return values


def _sanitize_topic_part(value: str) -> str:
    """Нормализует часть topic (lowercase + безопасные символы)."""
    chars: List[str] = []
    for ch in value.strip().lower():
        chars.append(ch if (ch.isalnum() or ch in {"-", "_", "."}) else "_")
    out = "".join(chars).strip("._-")
    return out or "unknown"


def _table_to_topic(table_qualified: str, topic_prefix: str, sep: str) -> str:
    """Преобразует SCHEMA.TABLE в имя topic с заданным префиксом."""
    parts = table_qualified.split(".")
    if len(parts) != 2:
        raise ValueError(f"Expected SCHEMA.TABLE format, got: {table_qualified!r}")
    schema_name = _sanitize_topic_part(parts[0])
    table_name = _sanitize_topic_part(parts[1])
    topic_base = sep.join([schema_name, table_name])
    prefix = _sanitize_topic_part(topic_prefix)
    return sep.join([prefix, topic_base]) if prefix else topic_base


def _oracle_type_to_json_type(oracle_type: str, nullable: bool) -> Dict[str, Any]:
    """Маппинг Oracle типа к JSON Schema type.

    Это практичный, но не идеальный маппинг.
    При необходимости можно адаптировать под конкретные требования downstream.
    """
    t = oracle_type.upper().strip()
    if t in {
        "NUMBER",
        "INTEGER",
        "FLOAT",
        "BINARY_FLOAT",
        "BINARY_DOUBLE",
        "DECIMAL",
        "NUMERIC",
    }:
        base: Any = "number"
    elif t in {
        "SMALLINT",
    }:
        base = "integer"
    elif t in {
        "DATE",
        "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP WITH LOCAL TIME ZONE",
    }:
        base = "string"
    elif t in {
        "CHAR",
        "NCHAR",
        "VARCHAR2",
        "NVARCHAR2",
        "CLOB",
        "NCLOB",
        "LONG",
        "ROWID",
        "UROWID",
        "XMLTYPE",
    }:
        base = "string"
    elif t in {"RAW", "LONG RAW", "BLOB"}:
        # Обычно upstream кодирует такие значения (hex/base64) строкой.
        base = "string"
    else:
        # Безопасный fallback.
        base = "string"

    return {"type": [base, "null"]} if nullable else {"type": base}


def _load_table_metadata(conn: oracledb.Connection, owner: str, table: str) -> Dict[str, Any]:
    """Читает метаданные таблицы из Oracle: колонки + PK."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, nullable, column_id
            FROM all_tab_columns
            WHERE owner = :owner AND table_name = :table_name
            ORDER BY column_id
            """,
            {"owner": owner.upper(), "table_name": table.upper()},
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
              AND a.table_name = :table_name
              AND a.constraint_type = 'P'
            ORDER BY c.position
            """,
            {"owner": owner.upper(), "table_name": table.upper()},
        )
        pk_columns = [str(column_name).upper() for column_name, _position in cur.fetchall()]

    return {
        "owner": owner.upper(),
        "table": table.upper(),
        "columns": columns,
        "pk_columns": pk_columns,
    }


def _build_row_object_schema(table_meta: Dict[str, Any], include_required: bool = True) -> Dict[str, Any]:
    """Строит JSON Schema объект строки таблицы по списку колонок."""
    properties: Dict[str, Any] = {}
    required: List[str] = []
    for col in table_meta["columns"]:
        name = col["name"]
        properties[name] = _oracle_type_to_json_type(col["oracle_type"], col["nullable"])
        if not col["nullable"]:
            required.append(name)

    payload: Dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    # include_required=False используется для partial row-images (например UPDATE),
    # где в before/after допустим неполный набор колонок.
    if include_required and required:
        payload["required"] = required
    return payload


def _build_source_object_schema() -> Dict[str, Any]:
    """Строит source-блок CDC envelope."""
    return {
        "type": "object",
        "properties": {
            "schema": {"type": "string"},
            "table": {"type": "string"},
            "commit_scn": {"type": "integer"},
            "redo_sequence": {"type": "integer"},
            "rs_id": {"type": "string"},
            "ssn": {"type": "integer"},
            "ts_ms": {"type": ["integer", "null"]},
        },
        "required": ["schema", "table", "commit_scn", "redo_sequence", "rs_id", "ssn"],
        "additionalProperties": False,
    }


def _build_event_branch_schema(
    op_code: str,
    source_schema: Dict[str, Any],
    before_schema: Dict[str, Any],
    after_schema: Dict[str, Any],
) -> Dict[str, Any]:
    """Строит одну op-ветку schema (c/u/d) для root oneOf."""
    return {
        "type": "object",
        "properties": {
            "op": {"type": "string", "enum": [op_code]},
            "source": source_schema,
            "before": before_schema,
            "after": after_schema,
        },
        "required": ["op", "source", "before", "after"],
        "additionalProperties": False,
    }


def _build_key_schema(topic: str, table_meta: Dict[str, Any], key_mode: str) -> Dict[str, Any]:
    """Строит key schema.

    Режимы:
    - technical: служебный ключ по позиции события в redo потоке;
    - pk: ключ по первичному ключу таблицы.
    """
    if key_mode == "technical":
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": f"{topic}-key",
            "type": "object",
            "properties": {
                "schema": {"type": "string"},
                "table": {"type": "string"},
                "commit_scn": {"type": "integer"},
                "redo_sequence": {"type": "integer"},
                "rs_id": {"type": "string"},
                "ssn": {"type": "integer"},
            },
            "required": ["schema", "table", "commit_scn", "redo_sequence", "rs_id", "ssn"],
            "additionalProperties": False,
        }

    pk_columns = table_meta["pk_columns"]
    if not pk_columns:
        raise RuntimeError(
            f"No PK columns found for {table_meta['owner']}.{table_meta['table']} (key_mode=pk)"
        )

    col_map = {c["name"]: c for c in table_meta["columns"]}
    properties: Dict[str, Any] = {}
    for pk in pk_columns:
        col = col_map.get(pk)
        if col is None:
            raise RuntimeError(f"PK column {pk} not found in ALL_TAB_COLUMNS")
        # Для key null обычно не допускаем.
        properties[pk] = _oracle_type_to_json_type(col["oracle_type"], False)

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": f"{topic}-key",
        "type": "object",
        "properties": properties,
        "required": pk_columns,
        "additionalProperties": False,
    }


def _build_value_schema(topic: str, table_meta: Dict[str, Any]) -> Dict[str, Any]:
    """Строит value schema (CDC envelope: op/source/before/after)."""
    strict_row_schema = _build_row_object_schema(table_meta, include_required=True)
    partial_row_schema = _build_row_object_schema(table_meta, include_required=False)
    source_schema = _build_source_object_schema()

    # Контракт по операциям:
    # - c (INSERT): before=null, after=strict row image;
    # - u (UPDATE): before/after partial row images;
    # - d (DELETE): before=strict row image, after=null.
    insert_branch = _build_event_branch_schema(
        op_code="c",
        source_schema=source_schema,
        before_schema={"type": "null"},
        after_schema=strict_row_schema,
    )
    update_branch = _build_event_branch_schema(
        op_code="u",
        source_schema=source_schema,
        before_schema=partial_row_schema,
        after_schema=partial_row_schema,
    )
    delete_branch = _build_event_branch_schema(
        op_code="d",
        source_schema=source_schema,
        before_schema=strict_row_schema,
        after_schema={"type": "null"},
    )

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": f"{topic}-value",
        "type": "object",
        "oneOf": [insert_branch, update_branch, delete_branch],
    }


def _write_json(path: Path, payload: Dict[str, Any], overwrite: bool) -> str:
    """Записывает JSON в файл. Возвращает 'write' или 'skip'."""
    if path.exists() and not overwrite:
        return "skip"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return "write"


def _sr_request_json(
    sr_url: str,
    path: str,
    method: str,
    auth: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Выполняет JSON-запрос к SR и возвращает JSON-ответ (или `{}`)."""
    # Шаг 1: собираем полный endpoint (base URL + относительный path).
    endpoint = sr_url.rstrip("/") + path
    # Шаг 2: если есть payload, сериализуем его в JSON body.
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None

    # Шаг 3: формируем HTTP request + необходимые заголовки.
    req = urllib.request.Request(endpoint, data=body, method=method.upper())
    if body is not None:
        req.add_header("Content-Type", "application/vnd.schemaregistry.v1+json")
    if auth:
        # SR Basic auth ожидает base64(user:password).
        token = base64.b64encode(auth.encode("utf-8")).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")

    # Шаг 4: выполняем запрос и парсим JSON-ответ.
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _schema_payload_to_sr_text(schema_payload: Dict[str, Any]) -> str:
    """Преобразует schema payload в строковый JSON, как ожидает SR поле `schema`."""
    return json.dumps(schema_payload, ensure_ascii=False)


def _schema_text_matches_payload(sr_schema_text: str, local_schema_payload: Dict[str, Any]) -> bool:
    """Сравнивает схему из SR (`schema`-строка) с локальным payload словарем."""
    try:
        # Нормальный путь: сравниваем как dict-объекты (порядок ключей не важен).
        sr_obj = json.loads(sr_schema_text)
    except Exception:
        # Fallback: если SR вернул неожиданную строку, сравниваем как есть.
        return sr_schema_text.strip() == _schema_payload_to_sr_text(local_schema_payload).strip()
    return sr_obj == local_schema_payload


def _post_schema_to_sr(sr_url: str, subject: str, schema_payload: Dict[str, Any], auth: str) -> Dict[str, Any]:
    """Регистрирует одну схему в SR через `/subjects/<subject>/versions`."""
    # Subject может содержать точки/спецсимволы, URL-экранируем обязательно.
    subject_q = urllib.parse.quote(subject, safe="")
    return _sr_request_json(
        sr_url=sr_url,
        path=f"/subjects/{subject_q}/versions",
        method="POST",
        auth=auth,
        payload={
            "schemaType": "JSON",
            "schema": _schema_payload_to_sr_text(schema_payload),
        },
    )


def _get_latest_subject_schema(sr_url: str, subject: str, auth: str) -> Dict[str, Any]:
    """Читает latest schema для subject из SR."""
    subject_q = urllib.parse.quote(subject, safe="")
    return _sr_request_json(
        sr_url=sr_url,
        path=f"/subjects/{subject_q}/versions/latest",
        method="GET",
        auth=auth,
        payload=None,
    )


def _register_schema_idempotent(
    sr_url: str,
    subject: str,
    schema_payload: Dict[str, Any],
    auth: str,
) -> Dict[str, Any]:
    """Idempotent registration:
    - успешный POST -> `status=registered`;
    - HTTP 409 + identical latest schema -> `status=already-registered`;
    - HTTP 409 + different latest schema -> ошибка.
    """
    try:
        # Шаг 1: обычная попытка регистрации новой версии схемы.
        result = _post_schema_to_sr(sr_url, subject, schema_payload, auth)
        return {
            "status": "registered",
            "id": result.get("id"),
        }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        if exc.code != 409:
            # Любая ошибка кроме "конфликт совместимости" пробрасывается как runtime-ошибка.
            raise RuntimeError(f"Schema Registry HTTP error for subject={subject}: {exc.code} {body}") from exc

        # Шаг 2 (409): читаем latest subject schema, чтобы отличить
        # "схема уже зарегистрирована" от реального конфликта.
        try:
            latest = _get_latest_subject_schema(sr_url, subject, auth)
        except urllib.error.HTTPError as latest_exc:
            latest_body = latest_exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(
                f"Schema Registry 409 for subject={subject}, and latest schema read failed: "
                f"{latest_exc.code} {latest_body}"
            ) from latest_exc

        # Шаг 3: сравниваем latest SR schema с локальной схемой.
        latest_schema_text = str(latest.get("schema") or "")
        if _schema_text_matches_payload(latest_schema_text, schema_payload):
            # Идемпотентный случай: локальная схема уже фактически применена.
            return {
                "status": "already-registered",
                "id": latest.get("id"),
                "version": latest.get("version"),
            }

        # Иначе это реальная несовместимость/конфликт эволюции схем.
        raise RuntimeError(
            f"Schema Registry rejected schema for subject={subject} with HTTP 409 "
            "(latest subject schema differs from local payload)."
        ) from exc


def _register_schemas(
    sr_url: str,
    topic: str,
    key_schema: Dict[str, Any],
    value_schema: Dict[str, Any],
    auth: str,
) -> None:
    """Регистрирует пару схем (`-key` и `-value`) для конкретного topic."""
    # Для одного topic всегда работаем парой subject-ов.
    key_subject = f"{topic}-key"
    value_subject = f"{topic}-value"

    # Каждая регистрация сама idempotent и безопасна для повторного запуска.
    key_result = _register_schema_idempotent(sr_url, key_subject, key_schema, auth)
    value_result = _register_schema_idempotent(sr_url, value_subject, value_schema, auth)
    print(f"[oracle-schema-build] SR {key_result['status']} {key_subject}: {key_result}")
    print(f"[oracle-schema-build] SR {value_result['status']} {value_subject}: {value_result}")


def _resolve_tables(args_tables: str, args_tables_file: str) -> List[str]:
    """Разрешает список таблиц из `--tables` или `--tables-file`."""
    if args_tables:
        return _parse_csv(args_tables)
    if args_tables_file:
        path = Path(args_tables_file)
        rows: List[str] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                rows.append(stripped)
        return rows
    raise ValueError("Provide --tables or --tables-file")


def main() -> int:
    """CLI entrypoint.

    Поток выполнения:
    1) читаем аргументы;
    2) подключаемся к Oracle;
    3) генерируем key/value schema файлы;
    4) опционально регистрируем схемы в SR.
    """
    parser = argparse.ArgumentParser(description="Build JSON schemas from Oracle metadata.")
    parser.add_argument("--oracle-user", default=os.getenv("ORACLE_USER", ""))
    parser.add_argument("--oracle-password", default=os.getenv("ORACLE_PASSWORD", ""))
    parser.add_argument("--oracle-dsn", default=os.getenv("ORACLE_DSN", ""))
    parser.add_argument("--tables", default="", help="CSV list SCHEMA.TABLE")
    parser.add_argument("--tables-file", default="", help="File with SCHEMA.TABLE per line")
    parser.add_argument("--topic-prefix", default="oracle.cdc")
    parser.add_argument("--topic-separator", default=".")
    parser.add_argument("--schema-dir", default="schemas")
    parser.add_argument("--key-mode", default="technical", choices=["technical", "pk"])
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--register-sr", action="store_true")
    parser.add_argument("--sr-url", default=os.getenv("SCHEMA_REGISTRY_URL", ""))
    parser.add_argument(
        "--sr-auth",
        default=os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", ""),
        help="user:password for Basic auth (optional)",
    )
    args = parser.parse_args()

    if not args.oracle_user or not args.oracle_password or not args.oracle_dsn:
        raise SystemExit("Oracle credentials are required: --oracle-user/--oracle-password/--oracle-dsn")
    if args.register_sr and not args.sr_url:
        raise SystemExit("--register-sr requires --sr-url (or SCHEMA_REGISTRY_URL)")
    if oracledb is None:
        raise SystemExit("Python package 'oracledb' is required. Install dependencies in container/venv first.")

    tables = _resolve_tables(args.tables, args.tables_file)
    schema_dir = Path(args.schema_dir)
    print(
        f"[oracle-schema-build] start tables={len(tables)} schema_dir={schema_dir} "
        f"key_mode={args.key_mode} register_sr={args.register_sr}"
    )

    written = 0
    skipped = 0
    conn = oracledb.connect(user=args.oracle_user, password=args.oracle_password, dsn=args.oracle_dsn)
    try:
        for table_qualified in tables:
            owner, table = table_qualified.split(".", 1)
            topic = _table_to_topic(table_qualified, args.topic_prefix, args.topic_separator)
            meta = _load_table_metadata(conn, owner, table)

            key_schema = _build_key_schema(topic, meta, args.key_mode)
            value_schema = _build_value_schema(topic, meta)

            key_path = schema_dir / f"{topic}.key.json"
            value_path = schema_dir / f"{topic}.value.json"
            for payload, path in ((key_schema, key_path), (value_schema, value_path)):
                state = _write_json(path, payload, args.overwrite)
                if state == "write":
                    written += 1
                    print(f"[oracle-schema-build] write  {path}")
                else:
                    skipped += 1
                    print(f"[oracle-schema-build] skip   {path} (exists)")

            if args.register_sr:
                _register_schemas(args.sr_url, topic, key_schema, value_schema, args.sr_auth)

    finally:
        conn.close()

    print(f"[oracle-schema-build] done written={written} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
