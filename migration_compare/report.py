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
    chinese_report = _build_chinese_report(normalized_report)
    json_path.write_text(
        json.dumps(chinese_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    markdown_path.write_text(render_markdown_report(chinese_report), encoding="utf-8")
    return json_path, markdown_path


def render_markdown_report(report: dict[str, Any]) -> str:
    report_info = report["报告信息"]
    summary = report["对比总结"]
    structure = report["结构对比结果"]
    data = report["数据对比结果"]
    raw_source = report["源端原始查询结果"]
    raw_target = report["目标端原始查询结果"]

    lines: list[str] = []
    lines.append("# 数据迁移对比报告")
    lines.append("")
    lines.append(f"- 生成时间: {report_info['生成时间']}")
    lines.append(f"- 源端: {report_info['源端']['主机']}:{report_info['源端']['端口']} / {report_info['源端']['数据库']}.{report_info['源端']['表']}")
    lines.append(f"- 目标端: {report_info['目标端']['主机']}:{report_info['目标端']['端口']} / {report_info['目标端']['数据库']}.{report_info['目标端']['表']}")
    lines.append("")

    lines.append("## 总结")
    lines.append("")
    lines.append(f"- 总体一致: {'是' if summary['总体一致'] else '否'}")
    lines.append(f"- 表结构一致: {'是' if summary['表结构一致'] else '否'}")
    lines.append(f"- 数据一致: {'是' if summary['数据一致'] else '否'}")
    lines.append("")

    lines.append("## 表结构对比（大小写敏感）")
    lines.append("")
    lines.append(f"- 缺失于目标端的列: {structure['缺失于目标端的列'] or '无'}")
    lines.append(f"- 缺失于源端的列: {structure['缺失于源端的列'] or '无'}")
    lines.append(f"- 列顺序一致: {'是' if structure['列顺序一致'] else '否'}")
    lines.append("")
    if structure["列定义差异"]:
        lines.append("### 列定义差异")
        for item in structure["列定义差异"]:
            lines.append(f"- 列 `{item['列名']}` 差异:")
            for field, diff in item["差异字段"].items():
                lines.append(f"  - {field}: 源端={diff['源端']} | 目标端={diff['目标端']}")
    else:
        lines.append("- 列定义差异: 无")
    lines.append("")

    lines.append("## 数据对比（大小写敏感）")
    lines.append("")
    lines.append(f"- 对比模式: {data['对比模式']}")
    lines.append(f"- 对比列: {data['对比列'] or '无'}")
    lines.append(f"- 跳过列: {data['跳过列'] or '无'}")
    lines.append(f"- 主键列: {data['主键列'] or '无'}")
    lines.append(f"- 源端行数: {data['源端行数']}")
    lines.append(f"- 目标端行数: {data['目标端行数']}")
    lines.append(f"- 目标端缺失行数量: {data['目标端缺失行数量']}")
    lines.append(f"- 目标端多出行数量: {data['目标端多出行数量']}")
    lines.append(f"- 行内容不一致数量: {data['行内容不一致数量']}")
    lines.append(f"- 源端重复主键数量: {data['源端重复主键数量']}")
    lines.append(f"- 目标端重复主键数量: {data['目标端重复主键数量']}")
    lines.append(f"- 备注: {data['备注']}")
    lines.append("")

    _append_samples(lines, "目标端缺失行样例", data["目标端缺失行样例"])
    _append_samples(lines, "目标端多出行样例", data["目标端多出行样例"])
    _append_samples(lines, "行内容差异样例", data["行内容差异样例"])
    _append_samples(lines, "源端重复主键样例", data["源端重复主键样例"])
    _append_samples(lines, "目标端重复主键样例", data["目标端重复主键样例"])

    lines.append("## 源端原始查询结果")
    lines.append("")
    lines.append("### 原始表结构")
    lines.append("```json")
    lines.append(json.dumps(raw_source["原始表结构"], ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("### 原始数据")
    lines.append("```json")
    lines.append(json.dumps(raw_source["原始数据"], ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")

    lines.append("## 目标端原始查询结果")
    lines.append("")
    lines.append("### 原始表结构")
    lines.append("```json")
    lines.append(json.dumps(raw_target["原始表结构"], ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("### 原始数据")
    lines.append("```json")
    lines.append(json.dumps(raw_target["原始数据"], ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")

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


def _build_chinese_report(report: dict[str, Any]) -> dict[str, Any]:
    structure = report["comparison"]["structure"]
    data = report["comparison"]["data"]
    raw_query = report.get("raw_query_result", {})
    source_raw = raw_query.get("source", {})
    target_raw = raw_query.get("target", {})

    return {
        "报告信息": {
            "生成时间": report["generated_at"],
            "源端": {
                "主机": report["source"]["host"],
                "端口": report["source"]["port"],
                "数据库": report["source"]["database"],
                "表": report["source"]["table"],
            },
            "目标端": {
                "主机": report["target"]["host"],
                "端口": report["target"]["port"],
                "数据库": report["target"]["database"],
                "表": report["target"]["table"],
            },
        },
        "对比总结": {
            "总体一致": report["summary"]["overall_match"],
            "表结构一致": report["summary"]["structure_match"],
            "数据一致": report["summary"]["data_match"],
        },
        "结构对比结果": {
            "是否一致": structure["is_match"],
            "缺失于目标端的列": structure["missing_columns_in_target"],
            "缺失于源端的列": structure["missing_columns_in_source"],
            "列顺序一致": structure["column_order_match"],
            "源端列顺序": structure["source_column_order"],
            "目标端列顺序": structure["target_column_order"],
            "列定义差异": _translate_schema_mismatches(structure["column_definition_mismatches"]),
        },
        "数据对比结果": {
            "是否一致": data["is_match"],
            "对比模式": _translate_mode(data["mode"]),
            "对比列": data["compared_columns"],
            "主键列": data["key_columns"],
            "跳过列": data["skipped_columns"],
            "源端行数": data["source_row_count"],
            "目标端行数": data["target_row_count"],
            "目标端缺失行数量": data["missing_rows_in_target_count"],
            "目标端多出行数量": data["extra_rows_in_target_count"],
            "行内容不一致数量": data["changed_rows_count"],
            "源端重复主键数量": data["duplicate_key_count_source"],
            "目标端重复主键数量": data["duplicate_key_count_target"],
            "目标端缺失行样例": data["missing_rows_in_target_samples"],
            "目标端多出行样例": data["extra_rows_in_target_samples"],
            "行内容差异样例": data["changed_rows_samples"],
            "源端重复主键样例": data["duplicate_key_samples_source"],
            "目标端重复主键样例": data["duplicate_key_samples_target"],
            "备注": _translate_note(data["note"]),
        },
        "源端原始查询结果": {
            "数据库": source_raw.get("database"),
            "表": source_raw.get("table"),
            "主键": source_raw.get("primary_key", []),
            "原始表结构": source_raw.get("columns", []),
            "原始数据": source_raw.get("rows", []),
        },
        "目标端原始查询结果": {
            "数据库": target_raw.get("database"),
            "表": target_raw.get("table"),
            "主键": target_raw.get("primary_key", []),
            "原始表结构": target_raw.get("columns", []),
            "原始数据": target_raw.get("rows", []),
        },
    }


def _translate_schema_mismatches(mismatches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    translated: list[dict[str, Any]] = []
    for item in mismatches:
        translated.append(
            {
                "列名": item["column"],
                "差异字段": {
                    field: {"源端": diff["source"], "目标端": diff["target"]}
                    for field, diff in item["differences"].items()
                },
            }
        )
    return translated


def _translate_mode(mode: str) -> str:
    mode_map = {
        "primary_key": "按主键逐行对比",
        "row_multiset": "按行多重集对比",
        "unavailable": "无法执行数据对比",
    }
    return mode_map.get(mode, mode)


def _translate_note(note: str) -> str:
    note_map = {
        "Compared by aligned primary key columns. String comparison is case-sensitive.": "已按对齐主键列逐行对比。字符串比较区分大小写。",
        "Primary keys are not aligned, so comparison falls back to row multiset mode. String comparison is case-sensitive.": "源端与目标端主键不一致，已回退为按行多重集对比。字符串比较区分大小写。",
        "No common columns found between source and target (case-sensitive comparison).": "源端与目标端没有同名列（大小写敏感），无法执行数据对比。",
    }
    return note_map.get(note, note)
