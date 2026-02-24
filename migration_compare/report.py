from __future__ import annotations

import json
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any


def write_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_path = output_dir / f"compare_report_{timestamp}.json"
    markdown_path = output_dir / f"compare_report_{timestamp}.md"

    normalized_report = _normalize_json_value(report)
    json_path.write_text(
        json.dumps(normalized_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    markdown_path.write_text(render_markdown_report(normalized_report), encoding="utf-8")
    return json_path, markdown_path


def render_markdown_report(report: dict[str, Any]) -> str:
    summary = report["summary"]
    comparison = report["comparison"]
    structure = comparison["structure"]
    data = comparison["data"]

    lines: list[str] = []
    lines.append("# 数据迁移对比报告")
    lines.append("")
    lines.append(f"- 生成时间: {report['generated_at']}")
    lines.append(
        f"- 源端: {report['source']['host']}:{report['source']['port']} / "
        f"{report['source']['database']}.{report['source']['table']}"
    )
    lines.append(
        f"- 目标端: {report['target']['host']}:{report['target']['port']} / "
        f"{report['target']['database']}.{report['target']['table']}"
    )
    lines.append("")

    lines.append("## 总结")
    lines.append("")
    lines.append(f"- 总体一致: {'是' if summary['overall_match'] else '否'}")
    lines.append(f"- 表结构一致: {'是' if summary['structure_match'] else '否'}")
    lines.append(f"- 数据一致: {'是' if summary['data_match'] else '否'}")
    lines.append("")

    lines.append("## 表结构对比（大小写敏感）")
    lines.append("")
    lines.append(f"- 缺失于目标端的列: {structure['missing_columns_in_target'] or '无'}")
    lines.append(f"- 缺失于源端的列: {structure['missing_columns_in_source'] or '无'}")
    lines.append(f"- 列顺序一致: {'是' if structure['column_order_match'] else '否'}")
    lines.append("")
    if structure["column_definition_mismatches"]:
        lines.append("### 列定义差异")
        for item in structure["column_definition_mismatches"]:
            lines.append(f"- 列 `{item['column']}` 差异:")
            for field, diff in item["differences"].items():
                lines.append(f"  - {field}: source={diff['source']} | target={diff['target']}")
    else:
        lines.append("- 列定义差异: 无")
    lines.append("")

    lines.append("## 数据对比（大小写敏感）")
    lines.append("")
    lines.append(f"- 对比模式: {data['mode']}")
    lines.append(f"- 对比列: {data['compared_columns'] or '无'}")
    lines.append(f"- 跳过列: {data['skipped_columns'] or '无'}")
    lines.append(f"- 主键列: {data['key_columns'] or '无'}")
    lines.append(f"- 源端行数: {data['source_row_count']}")
    lines.append(f"- 目标端行数: {data['target_row_count']}")
    lines.append(f"- 目标端缺失行数量: {data['missing_rows_in_target_count']}")
    lines.append(f"- 目标端多出行数量: {data['extra_rows_in_target_count']}")
    lines.append(f"- 行内容不一致数量: {data['changed_rows_count']}")
    lines.append(f"- 源端重复主键数量: {data['duplicate_key_count_source']}")
    lines.append(f"- 目标端重复主键数量: {data['duplicate_key_count_target']}")
    lines.append(f"- 备注: {data['note']}")
    lines.append("")

    _append_samples(lines, "目标端缺失行样例", data["missing_rows_in_target_samples"])
    _append_samples(lines, "目标端多出行样例", data["extra_rows_in_target_samples"])
    _append_samples(lines, "行内容差异样例", data["changed_rows_samples"])
    _append_samples(lines, "源端重复主键样例", data["duplicate_key_samples_source"])
    _append_samples(lines, "目标端重复主键样例", data["duplicate_key_samples_target"])

    return "\n".join(lines).rstrip() + "\n"


def _append_samples(lines: list[str], title: str, samples: list[Any]) -> None:
    lines.append(f"### {title}")
    if not samples:
        lines.append("- 无")
        lines.append("")
        return

    for sample in samples:
        lines.append(f"- {json.dumps(sample, ensure_ascii=False)}")
    lines.append("")


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize_json_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return value
