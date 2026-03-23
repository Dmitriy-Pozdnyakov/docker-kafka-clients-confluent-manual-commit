"""Facade for Oracle LogMiner SQL parser backends.

Роли этого файла:
1) выбрать backend (`auto | auto_legacy_first | legacy_first | legacy | sqlglot`);
2) обеспечить fallback-логику в `auto`;
3) выполнить общую type coercion row-images по metadata таблицы.

Сами backend-парсеры разнесены по файлам:
- `cdc_sql_parser_legacy.py`
- `cdc_sql_parser_sqlglot.py`
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

try:
    from .cdc_sql_parser_legacy import parse_operation_images_legacy
    from .cdc_sql_parser_sqlglot import (
        parse_operation_images_sqlglot,
        sqlglot_dependencies_available,
    )
except ImportError:  # pragma: no cover
    from cdc_sql_parser_legacy import parse_operation_images_legacy
    from cdc_sql_parser_sqlglot import (
        parse_operation_images_sqlglot,
        sqlglot_dependencies_available,
    )

RowDict = Dict[str, Any]
TableMeta = Dict[str, Any]

_INT_TOKEN_RE = re.compile(r"[-+]?\d+")
_FLOAT_TOKEN_RE = re.compile(r"[-+]?\d+\.\d+")
_NUMERIC_ORACLE_TYPE_PREFIXES = (
    "NUMBER",
    "FLOAT",
    "BINARY_FLOAT",
    "BINARY_DOUBLE",
    "DECIMAL",
    "INTEGER",
    "INT",
    "SMALLINT",
)


def _normalize_column_name(raw: str) -> str:
    # Приводим имена к единому виду, чтобы сравнение колонок
    # не зависело от кавычек/регистра из SQL_REDO/SQL_UNDO.
    return str(raw).strip().strip('"').upper()


def _parse_numeric_token(token: str) -> Any:
    # Мягкий coercion: если токен не "чистое" число, возвращаем строку как есть.
    if _INT_TOKEN_RE.fullmatch(token):
        try:
            return int(token)
        except Exception:
            return token
    if _FLOAT_TOKEN_RE.fullmatch(token):
        try:
            return float(token)
        except Exception:
            return token
    return token


def _is_numeric_oracle_type(oracle_type: str) -> bool:
    # Проверяем именно prefix, т.к. Oracle type часто приходит с суффиксами,
    # например NUMBER(10,2) или FLOAT(126).
    normalized = str(oracle_type or "").upper()
    return any(normalized.startswith(prefix) for prefix in _NUMERIC_ORACLE_TYPE_PREFIXES)


def _coerce_value_by_oracle_type(value: Any, oracle_type: str) -> Any:
    """Приводит parsed literal к ожидаемому python-типу по Oracle data type."""
    if value is None:
        return None
    if not _is_numeric_oracle_type(oracle_type):
        # Для нечисловых типов ничего не трогаем: даты/строки/RAW остаются как распарсил backend.
        return value
    if isinstance(value, (int, float)):
        return value
    if not isinstance(value, str):
        return value
    # Часто parser отдает строку (например из quoted literal), здесь пробуем
    # безопасно до-привести к int/float.
    return _parse_numeric_token(value.strip())


def _coerce_row_image_types(row_image: Optional[RowDict], table_meta: TableMeta) -> Optional[RowDict]:
    """Приводит типы значений parsed row-image на основе metadata таблицы."""
    if row_image is None:
        return None

    type_by_col = {
        _normalize_column_name(str(col.get("name", ""))): str(col.get("oracle_type", "")).upper()
        for col in table_meta.get("columns", [])
    }
    out: RowDict = {}
    for col_name, col_value in row_image.items():
        col_key = _normalize_column_name(str(col_name))
        out[col_key] = _coerce_value_by_oracle_type(col_value, type_by_col.get(col_key, ""))
    return out


def _parse_with_backend(
    backend: str,
    operation: str,
    sql_redo: str,
    sql_undo: str,
) -> Tuple[str, Optional[RowDict], Optional[RowDict]]:
    if backend == "legacy":
        return parse_operation_images_legacy(operation, sql_redo, sql_undo)
    if backend == "sqlglot":
        return parse_operation_images_sqlglot(operation, sql_redo, sql_undo)
    raise RuntimeError(f"Unsupported parser backend: {backend!r}")


def _resolve_backends(parser_backend: str) -> Tuple[str, ...]:
    backend = str(parser_backend or "auto").strip().lower()
    if backend == "legacy_first":
        # Исторический alias для обратной совместимости с env/config.
        backend = "auto_legacy_first"

    if backend not in {"auto", "auto_legacy_first", "legacy", "sqlglot"}:
        raise RuntimeError(f"Unsupported parser backend: {parser_backend!r}")
    if backend == "legacy":
        return ("legacy",)
    if backend == "sqlglot":
        if not sqlglot_dependencies_available():
            raise RuntimeError("Parser backend 'sqlglot' requested, but sqlglot dependency is not installed")
        return ("sqlglot",)
    if backend == "auto_legacy_first":
        # Предпочитаем legacy и подключаем sqlglot только как fallback.
        return ("legacy", "sqlglot") if sqlglot_dependencies_available() else ("legacy",)
    # auto
    return ("sqlglot", "legacy") if sqlglot_dependencies_available() else ("legacy",)


def _parse_operation_images(
    operation: str,
    sql_redo: str,
    sql_undo: str,
    parser_backend: str,
) -> Tuple[str, Optional[RowDict], Optional[RowDict]]:
    """Возвращает `(op_code, before, after)` до type coercion."""
    selected_backends = _resolve_backends(parser_backend)
    raw_backend = str(parser_backend or "auto").strip().lower()
    if raw_backend == "legacy_first":
        raw_backend = "auto_legacy_first"
    # explicit_backend=True означает "fallback запрещен":
    # если пользователь явно выбрал backend, не скрываем ошибку.
    explicit_backend = raw_backend not in {"auto", "auto_legacy_first"}

    last_error: Optional[Exception] = None
    for backend in selected_backends:
        try:
            return _parse_with_backend(backend, operation, sql_redo, sql_undo)
        except Exception as exc:
            last_error = exc
            if explicit_backend:
                raise
            # Для auto-режима пробуем следующий backend (fallback).

    if last_error is not None:
        raise last_error
    raise RuntimeError("No parser backend available")


def parse_cdc_row_images(
    operation: str,
    sql_redo: str,
    sql_undo: str,
    table_meta: TableMeta,
    parser_backend: str = "auto",
) -> Tuple[str, Optional[RowDict], Optional[RowDict]]:
    """Парсит DML и возвращает `(op_code, before, after)` для CDC envelope."""
    op_code, before, after = _parse_operation_images(
        operation=operation,
        sql_redo=sql_redo,
        sql_undo=sql_undo,
        parser_backend=parser_backend,
    )

    # Общая пост-обработка одинакова для любого backend:
    # нормализуем parsed row images под metadata колонок.
    before = _coerce_row_image_types(before, table_meta)
    after = _coerce_row_image_types(after, table_meta)
    return op_code, before, after
