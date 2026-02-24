from __future__ import annotations

import unittest

from migration_compare.comparator import compare_data, compare_schema
from migration_compare.models import ColumnMetadata


def build_column(name: str, ordinal: int, col_type: str = "varchar(32)") -> ColumnMetadata:
    return ColumnMetadata(
        name=name,
        ordinal_position=ordinal,
        data_type="varchar",
        column_type=col_type,
        is_nullable="NO",
        column_default=None,
        extra="",
        character_set_name="utf8mb4",
        collation_name="utf8mb4_bin",
        column_key="",
    )


class ComparatorTestCase(unittest.TestCase):
    def test_schema_compare_is_case_sensitive_for_column_names(self) -> None:
        source_columns = [build_column("Name", 1)]
        target_columns = [build_column("name", 1)]

        result = compare_schema(source_columns, target_columns)

        self.assertFalse(result.is_match)
        self.assertEqual(result.missing_columns_in_target, ["Name"])
        self.assertEqual(result.missing_columns_in_source, ["name"])

    def test_data_compare_detects_case_difference_in_string_values(self) -> None:
        source_rows = [{"id": 1, "name": "Alice"}]
        target_rows = [{"id": 1, "name": "alice"}]

        result = compare_data(
            source_rows=source_rows,
            target_rows=target_rows,
            source_primary_key=["id"],
            target_primary_key=["id"],
            compared_columns=["id", "name"],
            skipped_columns=[],
            max_samples=10,
        )

        self.assertFalse(result.is_match)
        self.assertEqual(result.mode, "primary_key")
        self.assertEqual(result.changed_rows_count, 1)
        self.assertEqual(result.changed_rows_samples[0]["differences"][0]["column"], "name")

    def test_data_compare_falls_back_to_multiset_without_aligned_primary_key(self) -> None:
        source_rows = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
        target_rows = [{"id": 1, "name": "A"}, {"id": 3, "name": "C"}]

        result = compare_data(
            source_rows=source_rows,
            target_rows=target_rows,
            source_primary_key=[],
            target_primary_key=[],
            compared_columns=["id", "name"],
            skipped_columns=[],
            max_samples=10,
        )

        self.assertFalse(result.is_match)
        self.assertEqual(result.mode, "row_multiset")
        self.assertEqual(result.missing_rows_in_target_count, 1)
        self.assertEqual(result.extra_rows_in_target_count, 1)


if __name__ == "__main__":
    unittest.main()
