from __future__ import annotations

import json
import re
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
        self.assertIn("MySQL Migration Compare", body)
        self.assertIn("Run Comparison", body)

    def test_compare_validation_error_for_missing_fields(self) -> None:
        response = self.client.post("/compare", data={"source_host": ""})
        self.assertEqual(response.status_code, 400)
        self.assertIn("缺少必填字段", response.data.decode("utf-8"))

    def test_compare_result_and_download_endpoints(self) -> None:
        form_data = self._valid_form_data()
        response = self.client.post("/compare", data=form_data)
        self.assertEqual(response.status_code, 200)
        body = response.data.decode("utf-8")
        self.assertIn("Comparison Result", body)
        self.assertIn("fake_report.json", body)
        self.assertIsNotNone(self.last_config)
        self.assertEqual(self.last_config.max_report_samples, 20)

        report_id_match = re.search(r"/download/([0-9a-f]+)/json", body)
        self.assertIsNotNone(report_id_match)
        report_id = report_id_match.group(1)

        json_response = self.client.get(f"/download/{report_id}/json")
        self.assertEqual(json_response.status_code, 200)
        self.assertEqual(json_response.mimetype, "application/json")
        json_response.close()

        markdown_response = self.client.get(f"/download/{report_id}/markdown")
        self.assertEqual(markdown_response.status_code, 200)
        self.assertEqual(markdown_response.mimetype, "text/markdown")
        markdown_response.close()

        unknown_response = self.client.get(f"/download/{report_id}/invalid")
        self.assertEqual(unknown_response.status_code, 400)

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
