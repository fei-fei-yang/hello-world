from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from .models import ColumnMetadata, TableSnapshot


@dataclass(frozen=True)
class SchemaCompareResult:
    is_match: bool
    missing_columns_in_target: list[str]
    missing_columns_in_source: list[str]
    column_definition_mismatches: list[dict[str, Any]]
    column_order_match: bool
    source_column_order: list[str]
    target_column_order: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DataCompareResult:
    is_match: bool
    mode: str
    compared_columns: list[str]
    key_columns: list[str]
    skipped_columns: list[str]
    source_row_count: int
    target_row_count: int
    missing_rows_in_target_count: int
    extra_rows_in_target_count: int
    changed_rows_count: int
    duplicate_key_count_source: int
    duplicate_key_count_target: int
    missing_rows_in_target_samples: list[Any]
    extra_rows_in_target_samples: list[Any]
    changed_rows_samples: list[dict[str, Any]]
    duplicate_key_samples_source: list[dict[str, Any]]
    duplicate_key_samples_target: list[dict[str, Any]]
    note: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def compare_schema(source_columns: list[ColumnMetadata], target_columns: list[ColumnMetadata]) -> SchemaCompareResult:
    source_by_name = {column.name: column for column in source_columns}
    target_by_name = {column.name: column for column in target_columns}

    source_names = [column.name for column in source_columns]
    target_names = [column.name for column in target_columns]

    missing_in_target = [name for name in source_names if name not in target_by_name]
    missing_in_source = [name for name in target_names if name not in source_by_name]

    common_names = [name for name in source_names if name in target_by_name]
    column_definition_mismatches: list[dict[str, Any]] = []
    for name in common_names:
        source_column = source_by_name[name]
        target_column = target_by_name[name]
        field_diffs: dict[str, dict[str, Any]] = {}
        for field in (
            "data_type",
            "column_type",
            "is_nullable",
            "column_default",
            "extra",
            "character_set_name",
            "collation_name",
            "column_key",
        ):
            source_value = getattr(source_column, field)
            target_value = getattr(target_column, field)
            if source_value != target_value:
                field_diffs[field] = {"source": source_value, "target": target_value}

        if field_diffs:
            column_definition_mismatches.append({"column": name, "differences": field_diffs})

    source_common_order = [name for name in source_names if name in common_names]
    target_common_order = [name for name in target_names if name in common_names]
    column_order_match = source_common_order == target_common_order

    is_match = not (
        missing_in_target
        or missing_in_source
        or column_definition_mismatches
        or not column_order_match
    )
    return SchemaCompareResult(
        is_match=is_match,
        missing_columns_in_target=missing_in_target,
        missing_columns_in_source=missing_in_source,
        column_definition_mismatches=column_definition_mismatches,
        column_order_match=column_order_match,
        source_column_order=source_names,
        target_column_order=target_names,
    )


def compare_tables(source: TableSnapshot, target: TableSnapshot, max_samples: int = 100) -> dict[str, Any]:
    schema_result = compare_schema(source.columns, target.columns)
    source_column_names = [column.name for column in source.columns]
    target_column_names = [column.name for column in target.columns]
    compared_columns = [column for column in source_column_names if column in target_column_names]
    skipped_columns = sorted(set(source_column_names).symmetric_difference(set(target_column_names)))

    data_result = compare_data(
        source_rows=source.rows,
        target_rows=target.rows,
        source_primary_key=source.primary_key,
        target_primary_key=target.primary_key,
        compared_columns=compared_columns,
        skipped_columns=skipped_columns,
        max_samples=max_samples,
    )

    overall_match = schema_result.is_match and data_result.is_match
    return {
        "overall_match": overall_match,
        "structure": schema_result.to_dict(),
        "data": data_result.to_dict(),
    }


def compare_data(
    source_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    source_primary_key: list[str],
    target_primary_key: list[str],
    compared_columns: list[str],
    skipped_columns: list[str],
    max_samples: int,
) -> DataCompareResult:
    if not compared_columns:
        return DataCompareResult(
            is_match=False,
            mode="unavailable",
            compared_columns=[],
            key_columns=[],
            skipped_columns=skipped_columns,
            source_row_count=len(source_rows),
            target_row_count=len(target_rows),
            missing_rows_in_target_count=0,
            extra_rows_in_target_count=0,
            changed_rows_count=0,
            duplicate_key_count_source=0,
            duplicate_key_count_target=0,
            missing_rows_in_target_samples=[],
            extra_rows_in_target_samples=[],
            changed_rows_samples=[],
            duplicate_key_samples_source=[],
            duplicate_key_samples_target=[],
            note="No common columns found between source and target (case-sensitive comparison).",
        )

    key_columns: list[str] = []
    if source_primary_key and source_primary_key == target_primary_key and all(
        column in compared_columns for column in source_primary_key
    ):
        key_columns = source_primary_key
        return _compare_data_by_key(
            source_rows=source_rows,
            target_rows=target_rows,
            compared_columns=compared_columns,
            key_columns=key_columns,
            skipped_columns=skipped_columns,
            max_samples=max_samples,
        )

    return _compare_data_by_row_multiset(
        source_rows=source_rows,
        target_rows=target_rows,
        compared_columns=compared_columns,
        skipped_columns=skipped_columns,
        max_samples=max_samples,
    )


def _compare_data_by_key(
    source_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    compared_columns: list[str],
    key_columns: list[str],
    skipped_columns: list[str],
    max_samples: int,
) -> DataCompareResult:
    source_map, source_duplicate_count, source_duplicates = _build_keyed_row_map(
        rows=source_rows,
        compared_columns=compared_columns,
        key_columns=key_columns,
        max_samples=max_samples,
    )
    target_map, target_duplicate_count, target_duplicates = _build_keyed_row_map(
        rows=target_rows,
        compared_columns=compared_columns,
        key_columns=key_columns,
        max_samples=max_samples,
    )

    missing_rows_in_target_samples: list[dict[str, Any]] = []
    extra_rows_in_target_samples: list[dict[str, Any]] = []
    changed_rows_samples: list[dict[str, Any]] = []

    missing_rows_in_target_count = 0
    extra_rows_in_target_count = 0
    changed_rows_count = 0

    for key in source_map:
        if key not in target_map:
            missing_rows_in_target_count += 1
            if len(missing_rows_in_target_samples) < max_samples:
                missing_rows_in_target_samples.append(
                    {"key": _key_to_dict(key_columns, key), "row": source_map[key]}
                )

    for key in target_map:
        if key not in source_map:
            extra_rows_in_target_count += 1
            if len(extra_rows_in_target_samples) < max_samples:
                extra_rows_in_target_samples.append(
                    {"key": _key_to_dict(key_columns, key), "row": target_map[key]}
                )

    for key in source_map:
        if key not in target_map:
            continue

        source_row = source_map[key]
        target_row = target_map[key]
        differences = []
        for column in compared_columns:
            source_value = source_row.get(column)
            target_value = target_row.get(column)
            if source_value != target_value:
                differences.append({"column": column, "source": source_value, "target": target_value})

        if differences:
            changed_rows_count += 1
            if len(changed_rows_samples) < max_samples:
                changed_rows_samples.append(
                    {
                        "key": _key_to_dict(key_columns, key),
                        "differences": differences,
                    }
                )

    is_match = not (
        missing_rows_in_target_count
        or extra_rows_in_target_count
        or changed_rows_count
        or source_duplicate_count
        or target_duplicate_count
    )
    return DataCompareResult(
        is_match=is_match,
        mode="primary_key",
        compared_columns=compared_columns,
        key_columns=key_columns,
        skipped_columns=skipped_columns,
        source_row_count=len(source_rows),
        target_row_count=len(target_rows),
        missing_rows_in_target_count=missing_rows_in_target_count,
        extra_rows_in_target_count=extra_rows_in_target_count,
        changed_rows_count=changed_rows_count,
        duplicate_key_count_source=source_duplicate_count,
        duplicate_key_count_target=target_duplicate_count,
        missing_rows_in_target_samples=missing_rows_in_target_samples,
        extra_rows_in_target_samples=extra_rows_in_target_samples,
        changed_rows_samples=changed_rows_samples,
        duplicate_key_samples_source=source_duplicates,
        duplicate_key_samples_target=target_duplicates,
        note="Compared by aligned primary key columns. String comparison is case-sensitive.",
    )


def _compare_data_by_row_multiset(
    source_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    compared_columns: list[str],
    skipped_columns: list[str],
    max_samples: int,
) -> DataCompareResult:
    source_counter: Counter[tuple[Any, ...]] = Counter()
    target_counter: Counter[tuple[Any, ...]] = Counter()
    source_signature_samples: dict[tuple[Any, ...], dict[str, Any]] = {}
    target_signature_samples: dict[tuple[Any, ...], dict[str, Any]] = {}

    for row in source_rows:
        narrowed_row = _narrow_row(row, compared_columns)
        signature = _row_signature(narrowed_row, compared_columns)
        source_counter[signature] += 1
        source_signature_samples.setdefault(signature, narrowed_row)

    for row in target_rows:
        narrowed_row = _narrow_row(row, compared_columns)
        signature = _row_signature(narrowed_row, compared_columns)
        target_counter[signature] += 1
        target_signature_samples.setdefault(signature, narrowed_row)

    missing_rows = source_counter - target_counter
    extra_rows = target_counter - source_counter

    missing_rows_in_target_count = sum(missing_rows.values())
    extra_rows_in_target_count = sum(extra_rows.values())

    missing_rows_in_target_samples = []
    extra_rows_in_target_samples = []

    for signature, count in missing_rows.items():
        if len(missing_rows_in_target_samples) >= max_samples:
            break
        missing_rows_in_target_samples.append({"row": source_signature_samples[signature], "count": count})

    for signature, count in extra_rows.items():
        if len(extra_rows_in_target_samples) >= max_samples:
            break
        extra_rows_in_target_samples.append({"row": target_signature_samples[signature], "count": count})

    is_match = missing_rows_in_target_count == 0 and extra_rows_in_target_count == 0
    return DataCompareResult(
        is_match=is_match,
        mode="row_multiset",
        compared_columns=compared_columns,
        key_columns=[],
        skipped_columns=skipped_columns,
        source_row_count=len(source_rows),
        target_row_count=len(target_rows),
        missing_rows_in_target_count=missing_rows_in_target_count,
        extra_rows_in_target_count=extra_rows_in_target_count,
        changed_rows_count=0,
        duplicate_key_count_source=0,
        duplicate_key_count_target=0,
        missing_rows_in_target_samples=missing_rows_in_target_samples,
        extra_rows_in_target_samples=extra_rows_in_target_samples,
        changed_rows_samples=[],
        duplicate_key_samples_source=[],
        duplicate_key_samples_target=[],
        note=(
            "Primary keys are not aligned, so comparison falls back to row multiset mode. "
            "String comparison is case-sensitive."
        ),
    )


def _build_keyed_row_map(
    rows: list[dict[str, Any]],
    compared_columns: list[str],
    key_columns: list[str],
    max_samples: int,
) -> tuple[dict[tuple[Any, ...], dict[str, Any]], int, list[dict[str, Any]]]:
    row_map: dict[tuple[Any, ...], dict[str, Any]] = {}
    duplicate_samples: list[dict[str, Any]] = []
    duplicate_count = 0

    for row in rows:
        narrowed_row = _narrow_row(row, compared_columns)
        key = _build_key(narrowed_row, key_columns)
        if key in row_map:
            duplicate_count += 1
            if len(duplicate_samples) < max_samples:
                duplicate_samples.append({"key": _key_to_dict(key_columns, key), "row": narrowed_row})
            continue
        row_map[key] = narrowed_row

    return row_map, duplicate_count, duplicate_samples


def _build_key(row: dict[str, Any], key_columns: list[str]) -> tuple[Any, ...]:
    return tuple(_normalize_for_signature(row.get(column)) for column in key_columns)


def _key_to_dict(key_columns: list[str], key: tuple[Any, ...]) -> dict[str, Any]:
    return {column: key[index] for index, column in enumerate(key_columns)}


def _narrow_row(row: dict[str, Any], columns: list[str]) -> dict[str, Any]:
    return {column: row.get(column) for column in columns}


def _row_signature(row: dict[str, Any], columns: list[str]) -> tuple[Any, ...]:
    return tuple(_normalize_for_signature(row.get(column)) for column in columns)


def _normalize_for_signature(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple((key, _normalize_for_signature(val)) for key, val in sorted(value.items()))
    if isinstance(value, list):
        return tuple(_normalize_for_signature(item) for item in value)
    if isinstance(value, bytes):
        return ("__bytes__", value.hex())
    if isinstance(value, Decimal):
        return ("__decimal__", str(value))
    if isinstance(value, (datetime, date, time)):
        return ("__datetime__", value.isoformat())
    return value
