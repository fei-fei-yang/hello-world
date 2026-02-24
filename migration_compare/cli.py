from __future__ import annotations

import argparse
import json
import sys
from getpass import getpass
from pathlib import Path
from typing import Any

from .models import CompareConfig, DbConnection, EndpointConfig, TableIdentifier
from .report import write_report
from .service import run_comparison


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="migration-compare",
        description="MySQL 迁移数据端到端对比平台（表结构 + 数据，大小写敏感）",
    )
    parser.add_argument("--config", help="JSON 配置文件路径")
    parser.add_argument("--interactive", action="store_true", help="缺失字段时交互输入")
    parser.add_argument("--output-dir", help="报告输出目录")
    parser.add_argument("--max-samples", type=int, help="报告中每类差异样例最大数量")

    parser.add_argument("--source-host", help="源端 MySQL IP/域名")
    parser.add_argument("--source-port", type=int, help="源端 MySQL 端口")
    parser.add_argument("--source-user", help="源端 MySQL 用户名")
    parser.add_argument("--source-password", help="源端 MySQL 密码")
    parser.add_argument("--source-db", help="源端数据库名")
    parser.add_argument("--source-table", help="源端表名")

    parser.add_argument("--target-host", help="目标端 MySQL IP/域名")
    parser.add_argument("--target-port", type=int, help="目标端 MySQL 端口")
    parser.add_argument("--target-user", help="目标端 MySQL 用户名")
    parser.add_argument("--target-password", help="目标端 MySQL 密码")
    parser.add_argument("--target-db", help="目标端数据库名")
    parser.add_argument("--target-table", help="目标端表名")
    return parser


def load_json_config(config_path: str | None) -> dict[str, Any]:
    if not config_path:
        return {}
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file does not exist: {config_path}")

    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("Config root must be a JSON object.")
    return data


def resolve_runtime_config(args: argparse.Namespace) -> CompareConfig:
    config_data = load_json_config(args.config)
    source_cfg = config_data.get("source", {})
    target_cfg = config_data.get("target", {})

    source_endpoint = resolve_endpoint("source", args, source_cfg, args.interactive)
    target_endpoint = resolve_endpoint("target", args, target_cfg, args.interactive)

    output_dir = args.output_dir or config_data.get("output_dir") or "reports"
    if args.max_samples is not None:
        max_samples = args.max_samples
    elif "max_samples" in config_data:
        max_samples = config_data.get("max_samples")
    else:
        max_samples = 100

    if not isinstance(max_samples, int) or max_samples <= 0:
        raise ValueError("max_samples must be a positive integer.")

    return CompareConfig(
        source=source_endpoint,
        target=target_endpoint,
        output_dir=Path(output_dir),
        max_report_samples=max_samples,
    )


def resolve_endpoint(
    endpoint_name: str,
    args: argparse.Namespace,
    endpoint_config: dict[str, Any],
    interactive: bool,
) -> EndpointConfig:
    host = first_not_none(getattr(args, f"{endpoint_name}_host"), endpoint_config.get("host"))
    port = first_not_none(getattr(args, f"{endpoint_name}_port"), endpoint_config.get("port"), 3306)
    user = first_not_none(getattr(args, f"{endpoint_name}_user"), endpoint_config.get("user"))
    password = first_not_none(
        getattr(args, f"{endpoint_name}_password"),
        endpoint_config.get("password"),
    )
    database = first_not_none(getattr(args, f"{endpoint_name}_db"), endpoint_config.get("database"))
    table = first_not_none(getattr(args, f"{endpoint_name}_table"), endpoint_config.get("table"))

    if interactive:
        host = host or input(f"[{endpoint_name}] MySQL host/IP: ").strip()
        port = int(port or input(f"[{endpoint_name}] MySQL port (default 3306): ").strip() or "3306")
        user = user or input(f"[{endpoint_name}] MySQL user: ").strip()
        password = password or getpass(f"[{endpoint_name}] MySQL password: ")
        database = database or input(f"[{endpoint_name}] database: ").strip()
        table = table or input(f"[{endpoint_name}] table: ").strip()

    missing_fields = []
    for field_name, value in (
        ("host", host),
        ("port", port),
        ("user", user),
        ("password", password),
        ("database", database),
        ("table", table),
    ):
        if value in (None, ""):
            missing_fields.append(field_name)

    if missing_fields:
        raise ValueError(
            f"Missing required fields for {endpoint_name}: {', '.join(missing_fields)}. "
            "Provide arguments, config file values, or enable --interactive."
        )

    try:
        port_int = int(port)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{endpoint_name} port must be an integer.") from exc
    if port_int <= 0 or port_int > 65535:
        raise ValueError(f"{endpoint_name} port must be in range 1-65535.")

    return EndpointConfig(
        connection=DbConnection(
            host=str(host),
            port=port_int,
            user=str(user),
            password=str(password),
        ),
        table=TableIdentifier(database=str(database), table=str(table)),
    )


def first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        runtime_config = resolve_runtime_config(args)
        report = run_comparison(runtime_config)
        json_path, markdown_path = write_report(report, runtime_config.output_dir)
    except Exception as exc:  # noqa: BLE001 - CLI needs a single failure exit
        print(f"执行失败: {exc}", file=sys.stderr)
        return 1

    summary = report["summary"]
    print("对比执行完成。")
    print(f"- 表结构一致: {summary['structure_match']}")
    print(f"- 数据一致: {summary['data_match']}")
    print(f"- 总体一致: {summary['overall_match']}")
    print(f"- JSON 报告: {json_path}")
    print(f"- Markdown 报告: {markdown_path}")

    return 0 if summary["overall_match"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
