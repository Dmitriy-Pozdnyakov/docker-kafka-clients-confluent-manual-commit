#!/usr/bin/env python3
"""Stress + error-focused verification for cdc_sql_parser.

Скрипт проверяет две вещи:
1) валидные SQL действительно парсятся в корректные row-images;
2) невалидные SQL предсказуемо падают с ошибкой (а не "тихо проходят").
"""

from __future__ import annotations

import argparse
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    from .cdc_sql_parser import parse_cdc_row_images, sqlglot_dependencies_available
except ImportError:  # pragma: no cover
    from cdc_sql_parser import parse_cdc_row_images, sqlglot_dependencies_available

RowDict = Dict[str, Any]
TableMeta = Dict[str, Any]


EMPLOYEES_META: TableMeta = {
    "schema": "HR",
    "table": "EMPLOYEES",
    "columns": [
        {"name": "EMPLOYEE_ID", "oracle_type": "NUMBER"},
        {"name": "FIRST_NAME", "oracle_type": "VARCHAR2"},
        {"name": "LAST_NAME", "oracle_type": "VARCHAR2"},
        {"name": "EMAIL", "oracle_type": "VARCHAR2"},
        {"name": "HIRE_DATE", "oracle_type": "DATE"},
        {"name": "SALARY", "oracle_type": "NUMBER"},
        {"name": "DEPARTMENT_ID", "oracle_type": "NUMBER"},
        {"name": "CREATED_AT", "oracle_type": "TIMESTAMP"},
    ],
}


@dataclass
class Case:
    operation: str
    sql_redo: str
    sql_undo: str
    employee_id: int
    salary_before: Optional[int]
    salary_after: Optional[int]
    expect_error: bool = False
    note: str = ""


def _q(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _make_insert_case(employee_id: int, salary: int, last_name: str, note: str = "") -> Case:
    email = f"user{employee_id}@local.test"
    sql_redo = (
        'insert into "HR"."EMPLOYEES"'
        '("EMPLOYEE_ID","FIRST_NAME","LAST_NAME","EMAIL","HIRE_DATE","SALARY","DEPARTMENT_ID","CREATED_AT") '
        "values "
        f"({_q(str(employee_id))},{_q('Nina')},{_q(last_name)},{_q(email)},"
        f"TO_DATE('20-MAR-26', 'DD-MON-RR'),{_q(str(salary))},{_q(str(employee_id))},"
        "TO_TIMESTAMP('20-MAR-26 10.40.23.317357 PM'));"
    )
    return Case(
        operation="INSERT",
        sql_redo=sql_redo,
        sql_undo="",
        employee_id=employee_id,
        salary_before=None,
        salary_after=salary,
        note=note,
    )


def _make_update_case(
    employee_id: int,
    salary_before: int,
    salary_after: int,
    last_name_before: str,
    last_name_after: str,
    note: str = "",
) -> Case:
    email = f"user{employee_id}@local.test"
    sql_redo = (
        'update "HR"."EMPLOYEES" '
        f"set \"SALARY\"={_q(str(salary_after))},\"LAST_NAME\"={_q(last_name_after)} "
        f"where \"EMPLOYEE_ID\"={_q(str(employee_id))} and \"EMAIL\"={_q(email)};"
    )
    sql_undo = (
        'update "HR"."EMPLOYEES" '
        f"set \"SALARY\"={_q(str(salary_before))},\"LAST_NAME\"={_q(last_name_before)} "
        f"where \"EMPLOYEE_ID\"={_q(str(employee_id))} and \"EMAIL\"={_q(email)};"
    )
    return Case(
        operation="UPDATE",
        sql_redo=sql_redo,
        sql_undo=sql_undo,
        employee_id=employee_id,
        salary_before=salary_before,
        salary_after=salary_after,
        note=note,
    )


def _make_delete_case(employee_id: int, salary: int, last_name: str, note: str = "") -> Case:
    email = f"user{employee_id}@local.test"
    sql_redo = f'delete from "HR"."EMPLOYEES" where "EMPLOYEE_ID"={_q(str(employee_id))};'
    sql_undo = (
        'insert into "HR"."EMPLOYEES"'
        '("EMPLOYEE_ID","FIRST_NAME","LAST_NAME","EMAIL","HIRE_DATE","SALARY","DEPARTMENT_ID","CREATED_AT") '
        "values "
        f"({_q(str(employee_id))},{_q('Nina')},{_q(last_name)},{_q(email)},"
        f"TO_DATE('20-MAR-26', 'DD-MON-RR'),{_q(str(salary))},{_q(str(employee_id))},"
        "TO_TIMESTAMP('20-MAR-26 10.40.23.317357 PM'));"
    )
    return Case(
        operation="DELETE",
        sql_redo=sql_redo,
        sql_undo=sql_undo,
        employee_id=employee_id,
        salary_before=salary,
        salary_after=None,
        note=note,
    )


def _validate_parse_result(case: Case, op_code: str, before: Optional[RowDict], after: Optional[RowDict]) -> None:
    if case.operation == "INSERT":
        assert op_code == "c"
        assert before is None
        assert isinstance(after, dict)
        assert after["EMPLOYEE_ID"] == case.employee_id
        assert after["SALARY"] == case.salary_after
        assert isinstance(after["SALARY"], int)
        return

    if case.operation == "UPDATE":
        assert op_code == "u"
        assert isinstance(before, dict)
        assert isinstance(after, dict)
        assert before["EMPLOYEE_ID"] == case.employee_id
        assert after["EMPLOYEE_ID"] == case.employee_id
        assert before["SALARY"] == case.salary_before
        assert after["SALARY"] == case.salary_after
        assert isinstance(before["SALARY"], int)
        assert isinstance(after["SALARY"], int)
        return

    if case.operation == "DELETE":
        assert op_code == "d"
        assert isinstance(before, dict)
        assert after is None
        assert before["EMPLOYEE_ID"] == case.employee_id
        assert before["SALARY"] == case.salary_before
        assert isinstance(before["SALARY"], int)
        return

    raise AssertionError(f"Unknown operation in validator: {case.operation!r}")


def _run_cases(cases: List[Case], backend: str) -> Tuple[int, int, List[str]]:
    passed = 0
    failed = 0
    failures: List[str] = []

    for idx, case in enumerate(cases, start=1):
        label = case.note or f"{case.operation}#{idx}"
        if case.expect_error:
            try:
                parse_cdc_row_images(
                    case.operation,
                    case.sql_redo,
                    case.sql_undo,
                    EMPLOYEES_META,
                    parser_backend=backend,
                )
                failed += 1
                failures.append(f"{label}: expected error, but parser succeeded")
            except Exception:
                passed += 1
            continue

        try:
            op_code, before, after = parse_cdc_row_images(
                case.operation,
                case.sql_redo,
                case.sql_undo,
                EMPLOYEES_META,
                parser_backend=backend,
            )
            _validate_parse_result(case, op_code, before, after)
            passed += 1
        except Exception as exc:
            failed += 1
            failures.append(f"{label}: unexpected failure: {exc}")

    return passed, failed, failures


def _build_fixed_positive_cases() -> List[Case]:
    return [
        _make_insert_case(1, 3100, "Stream", note="insert/basic"),
        _make_insert_case(2, 3200, "O'Neil", note="insert/escaped-quote"),
        _make_insert_case(3, 3300, "Smith, Jr", note="insert/comma-in-string"),
        _make_update_case(11, 4100, 4200, "Before", "After", note="update/basic"),
        _make_update_case(12, 5000, 5050, "O'Neil", "Mc'Dowell", note="update/escaped-quote"),
        _make_delete_case(21, 2900, "Gone", note="delete/basic"),
    ]


def _build_fixed_negative_cases() -> List[Case]:
    bad_insert = _make_insert_case(101, 3000, "Broken", note="invalid/insert-values-missing")
    bad_insert.sql_redo = bad_insert.sql_redo.replace("values", "vals", 1)
    bad_insert.expect_error = True

    bad_insert_counts = _make_insert_case(102, 3000, "Broken", note="invalid/insert-count-mismatch")
    bad_insert_counts.sql_redo = bad_insert_counts.sql_redo.replace(
        '"CREATED_AT")',
        '"CREATED_AT","EXTRA_COL")',
        1,
    )
    bad_insert_counts.expect_error = True

    bad_update = _make_update_case(103, 3300, 3400, "A", "B", note="invalid/update-no-where")
    bad_update.sql_redo = bad_update.sql_redo.replace(" where ", " ", 1)
    bad_update.expect_error = True

    bad_delete = _make_delete_case(104, 3500, "BadUndo", note="invalid/delete-undo-not-insert")
    bad_delete.sql_undo = 'select 1 from dual'
    bad_delete.expect_error = True

    bad_op = _make_insert_case(105, 3600, "WrongOp", note="invalid/unsupported-operation")
    bad_op.operation = "MERGE"
    bad_op.expect_error = True

    return [bad_insert, bad_insert_counts, bad_update, bad_delete, bad_op]


def _build_random_case(rng: random.Random, i: int) -> Case:
    op = rng.choice(["INSERT", "UPDATE", "DELETE"])
    employee_id = 1000 + i
    salary_before = rng.randint(1000, 9000)
    salary_after = salary_before + rng.randint(-500, 500)
    if salary_after < 0:
        salary_after = 0

    last_names = ["Stream", "O'Neil", "Smith, Jr", "Alpha", "Beta"]
    last_before = rng.choice(last_names)
    last_after = rng.choice(last_names)

    if op == "INSERT":
        return _make_insert_case(employee_id, salary_after, last_after, note=f"random/insert/{i}")
    if op == "UPDATE":
        return _make_update_case(
            employee_id,
            salary_before,
            salary_after,
            last_before,
            last_after,
            note=f"random/update/{i}",
        )
    return _make_delete_case(employee_id, salary_before, last_before, note=f"random/delete/{i}")


def _corrupt_case(case: Case, rng: random.Random) -> Case:
    # Делаем контролируемую "поломку", чтобы parser обязан был выбросить ошибку.
    if case.operation == "INSERT":
        case.sql_redo = case.sql_redo.replace("values", "vals", 1)
    elif case.operation == "UPDATE":
        case.sql_redo = case.sql_redo.replace(" where ", " ", 1)
    elif case.operation == "DELETE":
        case.sql_undo = "select 1 from dual"
    else:
        case.operation = "MERGE"
    case.expect_error = True
    case.note = f"{case.note}/corrupted"
    return case


def run_stress(iterations: int, invalid_ratio: float, seed: int, backend: str) -> int:
    rng = random.Random(seed)
    started = time.perf_counter()

    fixed_positive = _build_fixed_positive_cases()
    fixed_negative = _build_fixed_negative_cases()

    random_cases: List[Case] = []
    for i in range(iterations):
        case = _build_random_case(rng, i)
        if rng.random() < invalid_ratio:
            case = _corrupt_case(case, rng)
        random_cases.append(case)

    p1, f1, e1 = _run_cases(fixed_positive, backend=backend)
    p2, f2, e2 = _run_cases(fixed_negative, backend=backend)
    p3, f3, e3 = _run_cases(random_cases, backend=backend)

    total_passed = p1 + p2 + p3
    total_failed = f1 + f2 + f3
    total_cases = len(fixed_positive) + len(fixed_negative) + len(random_cases)
    elapsed = max(time.perf_counter() - started, 1e-9)
    rate = total_cases / elapsed

    print("=== CDC SQL Parser Stress Report ===")
    print(f"backend={backend}, sqlglot_available={sqlglot_dependencies_available()}")
    print(f"seed={seed}, iterations={iterations}, invalid_ratio={invalid_ratio:.3f}")
    print(f"cases_total={total_cases}, passed={total_passed}, failed={total_failed}")
    print(f"duration_sec={elapsed:.3f}, throughput_cases_per_sec={rate:.1f}")
    print(f"fixed_positive: passed={p1}, failed={f1}")
    print(f"fixed_negative: passed={p2}, failed={f2}")
    print(f"random_mix:     passed={p3}, failed={f3}")

    if total_failed:
        print("\n--- Failures (first 20) ---")
        for item in (e1 + e2 + e3)[:20]:
            print(f"- {item}")
        return 1

    print("Result: OK (parser behavior matches expectations for valid/invalid SQL).")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stress + error-focused verification for cdc_sql_parser",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=10000,
        help="Количество случайных кейсов (по умолчанию: 10000)",
    )
    parser.add_argument(
        "--invalid-ratio",
        type=float,
        default=0.25,
        help="Доля намеренно испорченных SQL в random-прогоне (по умолчанию: 0.25)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed для воспроизводимости",
    )
    parser.add_argument(
        "--backend",
        choices=("auto", "auto_legacy_first", "legacy_first", "legacy", "sqlglot"),
        default="auto",
        help=(
            "backend parser: "
            "auto | auto_legacy_first | legacy_first | legacy | sqlglot "
            "(по умолчанию: auto)"
        ),
    )
    args = parser.parse_args()

    if args.iterations <= 0:
        raise SystemExit("--iterations must be > 0")
    if not (0.0 <= args.invalid_ratio <= 1.0):
        raise SystemExit("--invalid-ratio must be in [0.0, 1.0]")
    if args.backend == "sqlglot" and not sqlglot_dependencies_available():
        raise SystemExit("backend=sqlglot requested, but sqlglot is not installed")

    return run_stress(args.iterations, args.invalid_ratio, args.seed, backend=args.backend)


if __name__ == "__main__":
    raise SystemExit(main())
