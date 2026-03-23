"""Legacy parser backend for Oracle LogMiner SQL_REDO/SQL_UNDO.

Задача модуля:
1) разобрать простые INSERT/UPDATE/DELETE SQL от LogMiner;
2) вернуть сырые row-images (`before`/`after`) без type coercion;
3) не зависеть от внешних библиотек.

Важно:
- это консервативный backend;
- если шаблон SQL не поддержан, выбрасывается RuntimeError;
- нормализация типов выполняется на уровне фасада `cdc_sql_parser.py`.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

RowDict = Dict[str, Any]

# Регулярки для "быстрого пути" по простым SQL-шаблонам LogMiner.
_INSERT_HEAD_RE = re.compile(
    r"insert\s+into\s+.+?\((?P<columns>.+?)\)\s+values\s*",
    flags=re.IGNORECASE | re.DOTALL,
)
_UPDATE_SET_RE = re.compile(
    r"update\s+.+?\s+set\s+(?P<set_clause>.+?)\s+where\s+(?P<where_clause>.+?)\s*;?\s*$",
    flags=re.IGNORECASE | re.DOTALL,
)
_ASSIGNMENT_RE = re.compile(r'"?(?P<column>[A-Za-z0-9_#$]+)"?\s*=\s*(?P<value>.+)$', flags=re.DOTALL)

_INT_TOKEN_RE = re.compile(r"[-+]?\d+")
_FLOAT_TOKEN_RE = re.compile(r"[-+]?\d+\.\d+")


def _normalize_column_name(raw: str) -> str:
    """Нормализует имя колонки к UPPER без кавычек."""
    return str(raw).strip().strip('"').upper()


def _append_buf_if_not_empty(buf: List[str], out: List[str]) -> None:
    """Сливает буфер в выходной список, если токен непустой."""
    token = "".join(buf).strip()
    if token:
        out.append(token)


def _is_and_separator(text: str, index: int) -> bool:
    """Проверяет, что в позиции index находится top-level разделитель AND."""
    if text[index : index + 3].upper() != "AND":
        return False
    prev_ok = index == 0 or text[index - 1].isspace()
    next_ok = index + 3 >= len(text) or text[index + 3].isspace()
    return prev_ok and next_ok


def _split_top_level_sql(text: str, split_on_and: bool) -> List[str]:
    """Делит SQL по top-level разделителям (с учетом строк и скобок)."""
    items: List[str] = []
    buf: List[str] = []
    in_string = False
    paren_depth = 0

    # Простой state-machine сканер:
    # - in_string не дает резать внутри литералов;
    # - paren_depth не дает резать внутри скобок/функций.
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "'":
            buf.append(ch)
            if in_string and i + 1 < n and text[i + 1] == "'":
                buf.append(text[i + 1])
                i += 2
                continue
            in_string = not in_string
            i += 1
            continue

        if not in_string:
            if ch == "(":
                paren_depth += 1
                buf.append(ch)
                i += 1
                continue
            if ch == ")":
                if paren_depth > 0:
                    paren_depth -= 1
                buf.append(ch)
                i += 1
                continue
            if paren_depth == 0:
                if ch == ",":
                    _append_buf_if_not_empty(buf, items)
                    buf = []
                    i += 1
                    continue
                if split_on_and and _is_and_separator(text, i):
                    _append_buf_if_not_empty(buf, items)
                    buf = []
                    i += 3
                    continue

        # Обычный символ текущего токена.
        buf.append(ch)
        i += 1

    _append_buf_if_not_empty(buf, items)
    return items


def _split_csv_sql(text: str) -> List[str]:
    """Делит CSV-подобный SQL список по top-level запятым."""
    return _split_top_level_sql(text, split_on_and=False)


def _split_assignment_items(text: str) -> List[str]:
    """Делит assignment-clause по top-level AND и запятым."""
    return _split_top_level_sql(text, split_on_and=True)


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
    """Преобразует SQL literal в Python-значение (ограниченный набор для CDC прототипа)."""
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


def _parse_assignment_map(clause: str) -> RowDict:
    """Разбирает `col=value` выражения в словарь `COLUMN -> value`."""
    result: RowDict = {}
    for item in _split_assignment_items(clause):
        match = _ASSIGNMENT_RE.match(item.strip())
        if not match:
            continue
        col = _normalize_column_name(match.group("column"))
        result[col] = _sql_literal_to_python(match.group("value"))
    return result


def _extract_parenthesized_content(text: str, open_paren_idx: int) -> str:
    """Возвращает содержимое скобок `( ... )`, корректно проходя строки и вложенность."""
    if open_paren_idx < 0 or open_paren_idx >= len(text) or text[open_paren_idx] != "(":
        raise RuntimeError("Parenthesized SQL parser: expected '(' at open_paren_idx")

    in_string = False
    depth = 0
    buf: List[str] = []
    i = open_paren_idx
    while i < len(text):
        ch = text[i]
        if ch == "'":
            if in_string and i + 1 < len(text) and text[i + 1] == "'":
                if depth > 0:
                    buf.append(ch)
                    buf.append(text[i + 1])
                i += 2
                continue
            in_string = not in_string
            if depth > 0:
                buf.append(ch)
            i += 1
            continue

        if not in_string and ch == "(":
            depth += 1
            if depth > 1:
                buf.append(ch)
            i += 1
            continue

        if not in_string and ch == ")":
            depth -= 1
            if depth == 0:
                return "".join(buf).strip()
            if depth < 0:
                break
            buf.append(ch)
            i += 1
            continue

        if depth > 0:
            buf.append(ch)
        i += 1

    raise RuntimeError("Parenthesized SQL parser: unmatched parentheses")


def _parse_insert_sql(sql_redo: str) -> RowDict:
    """Парсит INSERT ... VALUES (...) и возвращает map `column -> value`."""
    sql_text = sql_redo.strip()
    match = _INSERT_HEAD_RE.search(sql_text)
    if not match:
        raise RuntimeError("Unsupported INSERT SQL_REDO pattern for prototype parser")

    # 1) Извлекаем список колонок.
    columns = [_normalize_column_name(part) for part in _split_csv_sql(match.group("columns"))]
    # 2) Извлекаем VALUES(...) и переводим literal в python-представление.
    values_part = sql_text[match.end() :].lstrip()
    if not values_part.startswith("("):
        raise RuntimeError("INSERT parser mismatch: VALUES clause is not parenthesized")
    values_raw = _extract_parenthesized_content(values_part, 0)
    values = [_sql_literal_to_python(part) for part in _split_csv_sql(values_raw)]
    if len(columns) != len(values):
        raise RuntimeError("INSERT parser mismatch: columns/value counts differ")
    return dict(zip(columns, values))


def _parse_update_sql(sql_redo: str, sql_undo: str) -> Tuple[RowDict, RowDict]:
    """Строит `before` из undo и `after` из redo (SET + WHERE)."""
    redo_match = _UPDATE_SET_RE.search(sql_redo.strip())
    undo_match = _UPDATE_SET_RE.search(sql_undo.strip())
    if not redo_match or not undo_match:
        raise RuntimeError("Unsupported UPDATE SQL pattern for prototype parser")

    # before = значения "до изменений" (берем из undo SET + undo WHERE).
    before_row: RowDict = {}
    before_row.update(_parse_assignment_map(undo_match.group("set_clause")))
    before_row.update(_parse_assignment_map(undo_match.group("where_clause")))

    # after = before + актуальные значения из redo.
    after_row = dict(before_row)
    after_row.update(_parse_assignment_map(redo_match.group("set_clause")))
    after_row.update(_parse_assignment_map(redo_match.group("where_clause")))
    return before_row, after_row


def _parse_delete_sql(sql_undo: str) -> RowDict:
    """Для DELETE восстанавливает row image из SQL_UNDO (ожидаем INSERT ... VALUES ...)."""
    return _parse_insert_sql(sql_undo)


def parse_operation_images_legacy(
    operation: str,
    sql_redo: str,
    sql_undo: str,
) -> Tuple[str, Optional[RowDict], Optional[RowDict]]:
    """Legacy backend dispatcher: возвращает `(op_code, before, after)`."""
    op = str(operation or "").strip().upper()
    if op == "INSERT":
        return "c", None, _parse_insert_sql(sql_redo)
    if op == "UPDATE":
        before, after = _parse_update_sql(sql_redo, sql_undo)
        return "u", before, after
    if op == "DELETE":
        return "d", _parse_delete_sql(sql_undo), None
    raise RuntimeError(f"Unsupported operation for CDC envelope: {op!r}")
