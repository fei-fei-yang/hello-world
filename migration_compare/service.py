from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .comparator import compare_tables
from .models import CompareConfig
from .mysql_client import MySQLSnapshotReader


def run_comparison(config: CompareConfig) -> dict[str, Any]:
    with MySQLSnapshotReader(config.source.connection) as source_reader:
        source_snapshot = source_reader.load_table_snapshot(config.source.table)

    with MySQLSnapshotReader(config.target.connection) as target_reader:
        target_snapshot = target_reader.load_table_snapshot(config.target.table)

    comparison_result = compare_tables(
        source=source_snapshot,
        target=target_snapshot,
        max_samples=config.max_report_samples,
    )

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
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
        "comparison": comparison_result,
        "summary": {
            "overall_match": comparison_result["overall_match"],
            "structure_match": comparison_result["structure"]["is_match"],
            "data_match": comparison_result["data"]["is_match"],
        },
    }
    return report
