from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from migration_compare.models import CompareConfig
from migration_compare.web import create_app
from pymysql import err as pymysql_err


class WebAppTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_output_dir = Path(self._temp_dir.name)
        self.last_config: CompareConfig | None = None

        def fake_compare_runner(config: CompareConfig) -> dict[str, Any]:
            self.last_config = config
            return {
                "generated_at": "2026-02-24T00:00:00+00:00",
                "source": {
                    "host": config.source.connection.host,
                    "port": config.source.connection.port,
                    "database": config.source.table.database,
                    "table": config.source.table.table,
                },
                "target": {
                    "host": config.target.connection.host,
                    "port": config.target.connection.port,
                    "database": config.target.table.database,
                    "table": config.target.table.table,
                },
                "summary": {
                    "overall_match": False,
                    "structure_match": True,
                    "data_match": False,
                },
                "comparison": {
                    "structure": {
                        "missing_columns_in_target": [],
                        "missing_columns_in_source": ["extra_col"],
                        "column_order_match": True,
                        "column_definition_mismatches": [],
                    },
                    "data": {
                        "mode": "primary_key",
                        "compared_columns": ["id", "name"],
                        "skipped_columns": [],
                        "key_columns": ["id"],
                        "source_row_count": 2,
                        "target_row_count": 2,
                        "missing_rows_in_target_count": 0,
                        "extra_rows_in_target_count": 0,
                        "changed_rows_count": 1,
                        "duplicate_key_count_source": 0,
                        "duplicate_key_count_target": 0,
                        "note": "Case-sensitive compare.",
                        "missing_rows_in_target_samples": [],
                        "extra_rows_in_target_samples": [],
                        "changed_rows_samples": [{"key": {"id": 1}}],
                    },
                },
                "raw_query_result": {
                    "source": {
                        "database": "db_src",
                        "table": "orders",
                        "primary_key": ["id"],
                        "columns": [
                            {"name": "id", "column_type": "bigint"},
                            {"name": "name", "column_type": "varchar(64)"},
                        ],
                        "rows": [
                            {"id": 1, "name": "Alice-1"},
                            {"id": 2, "name": "Alice-2"},
                            {"id": 3, "name": "Alice-3"},
                        ],
                    },
                    "target": {
                        "database": "db_tgt",
                        "table": "orders",
                        "primary_key": ["id"],
                        "columns": [
                            {"name": "id", "column_type": "bigint"},
                            {"name": "name", "column_type": "varchar(64)"},
                        ],
                        "rows": [
                            {"id": 1, "name": "alice-1"},
                            {"id": 2, "name": "alice-2"},
                            {"id": 3, "name": "alice-3"},
                        ],
                    },
                },
            }

        def fake_report_writer(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
            output_dir.mkdir(parents=True, exist_ok=True)
            json_path = output_dir / "fake_report.json"
            markdown_path = output_dir / "fake_report.md"
            json_path.write_text(json.dumps(report), encoding="utf-8")
            markdown_path.write_text("# report", encoding="utf-8")
            return json_path, markdown_path

        self.app = create_app(compare_runner=fake_compare_runner, report_writer=fake_report_writer)
        self.app.testing = True
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_index_page_loads(self) -> None:
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        body = response.data.decode("utf-8")
        self.assertIn("MySQL 迁移对比平台", body)
        self.assertIn("开始对比", body)

    def test_compare_validation_error_for_missing_fields(self) -> None:
        response = self.client.post("/compare", data={"source_host": ""})
        self.assertEqual(response.status_code, 400)
        self.assertIn("缺少必填字段", response.data.decode("utf-8"))

    def test_compare_result_page(self) -> None:
        form_data = self._valid_form_data()
        response = self.client.post("/compare", data=form_data)
        self.assertEqual(response.status_code, 200)
        body = response.data.decode("utf-8")
        self.assertIn("对比结果", body)
        self.assertIn("表结构差异", body)
        self.assertIn("源端表结构", body)
        self.assertIn("目标端表结构", body)
        self.assertIn("源端数据（表格）", body)
        self.assertIn("目标端数据（表格）", body)
        self.assertIn("<table", body)
        self.assertIn("共 3 行，当前第 1 / 1 页", body)
        self.assertIsNotNone(self.last_config)
        self.assertEqual(self.last_config.max_report_samples, 20)

    def test_download_route_removed(self) -> None:
        response = self.client.get("/download/any/json")
        self.assertEqual(response.status_code, 404)

    def test_result_endpoint_supports_raw_data_pagination(self) -> None:
        response = self.client.post("/compare", data=self._valid_form_data())
        self.assertEqual(response.status_code, 200)
        report_registry = self.app.config["REPORT_REGISTRY"]
        report_id = next(iter(report_registry))

        page_response = self.client.get(f"/result/{report_id}?source_page=2&target_page=3&page_size=1")
        self.assertEqual(page_response.status_code, 200)
        page_body = page_response.data.decode("utf-8")
        self.assertIn("共 3 行，当前第 2 / 3 页", page_body)
        self.assertIn("共 3 行，当前第 3 / 3 页", page_body)
        self.assertIn("Alice-2", page_body)
        self.assertIn("alice-3", page_body)

    def test_compare_error_message_for_remote_root_denied(self) -> None:
        def raise_error(_: CompareConfig) -> dict[str, Any]:
            raise pymysql_err.OperationalError(
                1045,
                "Access denied for user 'root'@'10.182.112.138' (using password: YES)",
            )

        self.app.config["COMPARE_RUNNER"] = raise_error
        response = self.client.post("/compare", data=self._valid_form_data())
        self.assertEqual(response.status_code, 500)
        self.assertIn("无远程权限", response.data.decode("utf-8"))

    def test_compare_error_message_for_host_not_allowed(self) -> None:
        def raise_error(_: CompareConfig) -> dict[str, Any]:
            raise pymysql_err.OperationalError(
                1130,
                "Host '10.182.112.138' is not allowed to connect to this MySQL server",
            )

        self.app.config["COMPARE_RUNNER"] = raise_error
        response = self.client.post("/compare", data=self._valid_form_data())
        self.assertEqual(response.status_code, 500)
        self.assertIn("主机未授权", response.data.decode("utf-8"))

    def test_compare_error_message_for_wrong_password(self) -> None:
        def raise_error(_: CompareConfig) -> dict[str, Any]:
            raise pymysql_err.OperationalError(
                1045,
                "Access denied for user 'cmp_user'@'10.182.112.138' (using password: NO)",
            )

        self.app.config["COMPARE_RUNNER"] = raise_error
        response = self.client.post("/compare", data=self._valid_form_data())
        self.assertEqual(response.status_code, 500)
        self.assertIn("密码错误", response.data.decode("utf-8"))

    def _valid_form_data(self) -> dict[str, str]:
        return {
            "source_host": "10.0.0.1",
            "source_port": "3306",
            "source_user": "src_user",
            "source_password": "src_pass",
            "source_db": "db_src",
            "source_table": "orders",
            "target_host": "10.0.0.2",
            "target_port": "3306",
            "target_user": "tgt_user",
            "target_password": "tgt_pass",
            "target_db": "db_tgt",
            "target_table": "orders",
            "output_dir": str(self.base_output_dir / "reports"),
            "max_samples": "20",
        }


if __name__ == "__main__":
    unittest.main()
