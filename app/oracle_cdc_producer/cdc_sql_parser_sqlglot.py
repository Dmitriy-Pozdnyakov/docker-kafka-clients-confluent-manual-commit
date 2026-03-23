"""sqlglot backend for Oracle LogMiner SQL_REDO/SQL_UNDO parser.

Задача backend:
1) разобрать SQL в AST (Oracle dialect);
2) извлечь `before/after` для INSERT/UPDATE/DELETE;
3) вернуть сырые row-images без type coercion.

Важно:
- dependency optional: если `sqlglot` не установлен, backend недоступен;
- сообщения ошибок нормализованы под текущий pipeline.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

try:  # optional dependency
    import sqlglot
    from sqlglot import exp as _sg_exp
except Exception:  # pragma: no cover
    sqlglot = None
    _sg_exp = None

RowDict = Dict[str, Any]

# Числовые literal helper-ы (используются для мягкого coercion строковых токенов).
_INT_TOKEN_RE = re.compile(r"[-+]?\d+")
_FLOAT_TOKEN_RE = re.compile(r"[-+]?\d+\.\d+")


def sqlglot_dependencies_available() -> bool:
    """Возвращает True, если optional dependency `sqlglot` доступна."""
    return sqlglot is not None and _sg_exp is not None


def _require_available() -> None:
    """Fail-fast guard для путей, где sqlglot обязателен."""
    if not sqlglot_dependencies_available():
        raise RuntimeError("Parser backend 'sqlglot' requested, but sqlglot dependency is not installed")


def _normalize_column_name(raw: str) -> str:
    """Нормализует имя колонки к UPPER без кавычек."""
    return str(raw).strip().strip('"').upper()


def _parse_numeric_token(token: str) -> Any:
    """Пытается преобразовать строковый токен в int/float, иначе возвращает как есть."""
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


def _sql_literal_to_python(token: str) -> Any:
    """Преобразует SQL literal в python-представление для CDC row-image."""
    token = token.strip()
    if not token:
        return None
    upper = token.upper()
    if upper == "NULL":
        return None
    if upper.startswith("TO_DATE(") or upper.startswith("TO_TIMESTAMP("):
        return token
    if token.startswith("'") and token.endswith("'"):
        inner = token[1:-1].replace("''", "'")
        return _parse_numeric_token(inner)
    return _parse_numeric_token(token)


def _parse_one(sql_text: str, stmt_kind: str):
    """Парсит SQL в AST конкретного stmt_kind, нормализуя текст ошибки."""
    _require_available()
    try:
        return sqlglot.parse_one(sql_text, read="oracle")
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"sqlglot failed to parse {stmt_kind} SQL: {exc}") from exc


def _to_sql(expr: Any) -> str:
    """Безопасно сериализует AST node обратно в SQL-фрагмент."""
    if expr is None:
        return ""
    try:
        return str(expr.sql(dialect="oracle"))
    except Exception:
        return str(expr)


def _column_name(expr: Any) -> Optional[str]:
    """Извлекает и нормализует имя колонки из Column/Identifier узла."""
    if expr is None or not sqlglot_dependencies_available():
        return None
    if isinstance(expr, _sg_exp.Column):
        name = getattr(expr, "name", None)
        if name:
            return _normalize_column_name(name)
        this_arg = expr.args.get("this")
        if this_arg is not None:
            raw = getattr(this_arg, "this", this_arg)
            return _normalize_column_name(raw)
    if isinstance(expr, _sg_exp.Identifier):
        return _normalize_column_name(getattr(expr, "this", ""))
    return None


def _expr_to_python(expr: Any) -> Any:
    """Преобразует AST expression в python value для payload."""
    _require_available()
    if expr is None:
        return None
    if isinstance(expr, _sg_exp.Paren):
        return _expr_to_python(expr.args.get("this"))
    if isinstance(expr, _sg_exp.Null):
        return None
    if isinstance(expr, _sg_exp.Literal):
        raw = str(getattr(expr, "this", "")).strip()
        return _parse_numeric_token(raw)

    token = _to_sql(expr).strip()
    return _sql_literal_to_python(token)


def _assignment_pair(expr: Any) -> Optional[Tuple[str, Any]]:
    """Пытается извлечь пару `COLUMN -> value` из assignment/equality узла."""
    if expr is None or not hasattr(expr, "args"):
        return None
    left = expr.args.get("this")
    right = expr.args.get("expression")
    col = _column_name(left)
    if not col or right is None:
        return None
    return col, _expr_to_python(right)


def _collect_where_pairs(expr: Any, out: RowDict) -> None:
    """Рекурсивно собирает простые equality-пары из WHERE дерева."""
    if expr is None or not sqlglot_dependencies_available():
        return
    # Спускаемся до "плоских" сравнений вида COL = LITERAL.
    if isinstance(expr, _sg_exp.Where):
        _collect_where_pairs(expr.args.get("this"), out)
        return
    if isinstance(expr, _sg_exp.Paren):
        _collect_where_pairs(expr.args.get("this"), out)
        return
    if isinstance(expr, _sg_exp.And):
        _collect_where_pairs(expr.args.get("this"), out)
        _collect_where_pairs(expr.args.get("expression"), out)
        return

    # Leaf-case: извлекаем пару, если узел похож на assignment/equality.
    pair = _assignment_pair(expr)
    if pair:
        out[pair[0]] = pair[1]


def _extract_insert_columns(insert_ast: Any) -> List[str]:
    """Извлекает список колонок из INSERT AST."""
    target = insert_ast.args.get("this")
    schema_expr = target if isinstance(target, _sg_exp.Schema) else insert_ast.find(_sg_exp.Schema)
    if schema_expr is None:
        raise RuntimeError("Unsupported INSERT SQL_REDO pattern for prototype parser")

    columns: List[str] = []
    for col_expr in schema_expr.expressions or []:
        col_name = _column_name(col_expr)
        if col_name:
            columns.append(col_name)
    if not columns:
        raise RuntimeError("Unsupported INSERT SQL_REDO pattern for prototype parser")
    return columns


def _extract_insert_values(insert_ast: Any) -> List[Any]:
    """Извлекает один VALUES-row из INSERT AST и конвертирует значения."""
    values_expr = insert_ast.find(_sg_exp.Values)
    if values_expr is None or not values_expr.expressions:
        raise RuntimeError("INSERT parser mismatch: VALUES clause is not parenthesized")

    if len(values_expr.expressions) != 1:
        raise RuntimeError("INSERT parser mismatch: columns/value counts differ")

    row_expr = values_expr.expressions[0]
    value_exprs = list(getattr(row_expr, "expressions", []) or [])
    if not value_exprs:
        raise RuntimeError("INSERT parser mismatch: columns/value counts differ")
    return [_expr_to_python(expr) for expr in value_exprs]


def _parse_insert_sql(sql_redo: str) -> RowDict:
    """Парсит INSERT SQL_REDO и возвращает row-image `column -> value`."""
    ast = _parse_one(sql_redo, "INSERT")
    if not isinstance(ast, _sg_exp.Insert):
        raise RuntimeError("Unsupported INSERT SQL_REDO pattern for prototype parser")

    columns = _extract_insert_columns(ast)
    values = _extract_insert_values(ast)
    if len(columns) != len(values):
        raise RuntimeError("INSERT parser mismatch: columns/value counts differ")
    return dict(zip(columns, values))


def _collect_update_set_pairs(update_ast: Any) -> RowDict:
    """Собирает пары из секции SET у UPDATE."""
    out: RowDict = {}
    for expr in update_ast.args.get("expressions") or []:
        pair = _assignment_pair(expr)
        if pair:
            out[pair[0]] = pair[1]
    return out


def _parse_update_sql(sql_redo: str, sql_undo: str) -> Tuple[RowDict, RowDict]:
    """Строит before/after для UPDATE из redo и undo AST."""
    redo_ast = _parse_one(sql_redo, "UPDATE")
    undo_ast = _parse_one(sql_undo, "UPDATE")
    if not isinstance(redo_ast, _sg_exp.Update) or not isinstance(undo_ast, _sg_exp.Update):
        raise RuntimeError("Unsupported UPDATE SQL pattern for prototype parser")

    # before = undo SET + undo WHERE.
    before_row = _collect_update_set_pairs(undo_ast)
    _collect_where_pairs(undo_ast.args.get("where"), before_row)

    # after = before + redo SET/WHERE.
    after_row = dict(before_row)
    after_row.update(_collect_update_set_pairs(redo_ast))
    _collect_where_pairs(redo_ast.args.get("where"), after_row)
    return before_row, after_row


def _parse_delete_sql(sql_undo: str) -> RowDict:
    """Для DELETE восстанавливает row image из SQL_UNDO (ожидаем INSERT ... VALUES ...)."""
    return _parse_insert_sql(sql_undo)


def parse_operation_images_sqlglot(
    operation: str,
    sql_redo: str,
    sql_undo: str,
) -> Tuple[str, Optional[RowDict], Optional[RowDict]]:
    """sqlglot backend dispatcher: возвращает `(op_code, before, after)`."""
    op = str(operation or "").strip().upper()
    if op == "INSERT":
        return "c", None, _parse_insert_sql(sql_redo)
    if op == "UPDATE":
        before, after = _parse_update_sql(sql_redo, sql_undo)
        return "u", before, after
    if op == "DELETE":
        return "d", _parse_delete_sql(sql_undo), None
    raise RuntimeError(f"Unsupported operation for CDC envelope: {op!r}")
