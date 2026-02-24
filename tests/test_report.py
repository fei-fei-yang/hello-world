from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from migration_compare.report import write_report


class ReportWriterTestCase(unittest.TestCase):
    def test_report_is_localized_to_chinese_and_contains_raw_payload(self) -> None:
        report = {
            "generated_at": "2026-02-24T08:00:00+00:00",
            "source": {"host": "10.0.0.1", "port": 3306, "database": "db_src", "table": "orders"},
            "target": {"host": "10.0.0.2", "port": 3306, "database": "db_tgt", "table": "orders"},
            "summary": {"overall_match": False, "structure_match": True, "data_match": False},
            "comparison": {
                "overall_match": False,
                "structure": {
                    "is_match": True,
                    "missing_columns_in_target": [],
                    "missing_columns_in_source": [],
                    "column_definition_mismatches": [],
                    "column_order_match": True,
                    "source_column_order": ["id", "name"],
                    "target_column_order": ["id", "name"],
                },
                "data": {
                    "is_match": False,
                    "mode": "primary_key",
                    "compared_columns": ["id", "name"],
                    "key_columns": ["id"],
                    "skipped_columns": [],
                    "source_row_count": 1,
                    "target_row_count": 1,
                    "missing_rows_in_target_count": 0,
                    "extra_rows_in_target_count": 0,
                    "changed_rows_count": 1,
                    "duplicate_key_count_source": 0,
                    "duplicate_key_count_target": 0,
                    "missing_rows_in_target_samples": [],
                    "extra_rows_in_target_samples": [],
                    "changed_rows_samples": [
                        {
                            "key": {"id": 1},
                            "differences": [{"column": "name", "source": "Alice", "target": "alice"}],
                        }
                    ],
                    "duplicate_key_samples_source": [],
                    "duplicate_key_samples_target": [],
                    "note": "Compared by aligned primary key columns. String comparison is case-sensitive.",
                },
            },
            "raw_query_result": {
                "source": {
                    "database": "db_src",
                    "table": "orders",
                    "primary_key": ["id"],
                    "columns": [{"name": "id", "column_type": "bigint"}],
                    "rows": [{"id": 1, "name": "Alice"}],
                },
                "target": {
                    "database": "db_tgt",
                    "table": "orders",
                    "primary_key": ["id"],
                    "columns": [{"name": "id", "column_type": "bigint"}],
                    "rows": [{"id": 1, "name": "alice"}],
                },
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            json_path, markdown_path = write_report(report, output_dir)

            json_data = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertIn("报告信息", json_data)
            self.assertIn("对比总结", json_data)
            self.assertIn("源端原始查询结果", json_data)
            self.assertIn("目标端原始查询结果", json_data)
            self.assertEqual(json_data["源端原始查询结果"]["原始数据"][0]["name"], "Alice")
            self.assertEqual(json_data["目标端原始查询结果"]["原始数据"][0]["name"], "alice")

            markdown_text = markdown_path.read_text(encoding="utf-8")
            self.assertIn("## 源端原始查询结果", markdown_text)
            self.assertIn("## 目标端原始查询结果", markdown_text)
            self.assertIn("字符串比较区分大小写", markdown_text)


if __name__ == "__main__":
    unittest.main()
