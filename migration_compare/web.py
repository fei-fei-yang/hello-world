from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from flask import Flask, Response, render_template, request, send_file

from .models import CompareConfig, DbConnection, EndpointConfig, TableIdentifier
from .report import write_report
from .service import run_comparison


CompareRunner = Callable[[CompareConfig], dict[str, Any]]
ReportWriter = Callable[[dict[str, Any], Path], tuple[Path, Path]]


@dataclass(frozen=True)
class StoredReport:
    report_id: str
    created_at: str
    json_path: Path
    markdown_path: Path


def create_app(
    compare_runner: CompareRunner | None = None,
    report_writer: ReportWriter | None = None,
) -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "migration-compare-web"
    app.config["REPORT_REGISTRY"] = {}
    app.config["COMPARE_RUNNER"] = compare_runner or run_comparison
    app.config["REPORT_WRITER"] = report_writer or write_report

    @app.get("/")
    def index() -> str:
        return render_template("index.html", values=_default_form_values(), error_message="")

    @app.post("/compare")
    def compare() -> tuple[str, int] | str:
        values = _collect_form_values(request.form)
        try:
            config = _build_compare_config(values)
        except ValueError as exc:
            return render_template("index.html", values=values, error_message=str(exc)), 400

        try:
            report = app.config["COMPARE_RUNNER"](config)
            json_path, markdown_path = app.config["REPORT_WRITER"](report, config.output_dir)
        except Exception as exc:  # noqa: BLE001 - UI endpoint needs a single error response
            return render_template("index.html", values=values, error_message=f"Execution failed: {exc}"), 500

        report_id = uuid4().hex
        app.config["REPORT_REGISTRY"][report_id] = StoredReport(
            report_id=report_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            json_path=json_path,
            markdown_path=markdown_path,
        )

        return render_template(
            "result.html",
            report=report,
            report_id=report_id,
            json_name=json_path.name,
            markdown_name=markdown_path.name,
        )

    @app.get("/download/<report_id>/<format_name>")
    def download(report_id: str, format_name: str) -> Response:
        stored = app.config["REPORT_REGISTRY"].get(report_id)
        if stored is None:
            return Response("Unknown report ID.", status=404)

        if format_name == "json":
            path = stored.json_path
            mimetype = "application/json"
        elif format_name == "markdown":
            path = stored.markdown_path
            mimetype = "text/markdown"
        else:
            return Response("Unsupported format.", status=400)

        if not path.exists():
            return Response("Report file no longer exists.", status=404)
        return send_file(path, as_attachment=True, download_name=path.name, mimetype=mimetype)

    return app


def _build_compare_config(values: dict[str, str]) -> CompareConfig:
    max_samples = _parse_int(values["max_samples"], "max_samples")
    if max_samples <= 0:
        raise ValueError("max_samples must be greater than 0.")

    source = _build_endpoint(values, "source")
    target = _build_endpoint(values, "target")
    output_dir = Path(values["output_dir"] or "reports")
    return CompareConfig(source=source, target=target, output_dir=output_dir, max_report_samples=max_samples)


def _build_endpoint(values: dict[str, str], prefix: str) -> EndpointConfig:
    host = values[f"{prefix}_host"].strip()
    user = values[f"{prefix}_user"].strip()
    password = values[f"{prefix}_password"]
    database = values[f"{prefix}_db"].strip()
    table = values[f"{prefix}_table"].strip()

    port = _parse_int(values[f"{prefix}_port"], f"{prefix}_port")
    if port <= 0 or port > 65535:
        raise ValueError(f"{prefix}_port must be between 1 and 65535.")

    missing = []
    for name, value in (
        (f"{prefix}_host", host),
        (f"{prefix}_user", user),
        (f"{prefix}_password", password),
        (f"{prefix}_db", database),
        (f"{prefix}_table", table),
    ):
        if value == "":
            missing.append(name)
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}.")

    return EndpointConfig(
        connection=DbConnection(host=host, port=port, user=user, password=password),
        table=TableIdentifier(database=database, table=table),
    )


def _parse_int(raw_value: str, field_name: str) -> int:
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer.") from exc


def _default_form_values() -> dict[str, str]:
    return {
        "source_host": "",
        "source_port": "3306",
        "source_user": "",
        "source_password": "",
        "source_db": "",
        "source_table": "",
        "target_host": "",
        "target_port": "3306",
        "target_user": "",
        "target_password": "",
        "target_db": "",
        "target_table": "",
        "output_dir": "reports",
        "max_samples": "100",
    }


def _collect_form_values(form: Any) -> dict[str, str]:
    values = _default_form_values()
    for key in values:
        values[key] = str(form.get(key, values[key]))
    return values


def _build_web_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="migration-compare-web",
        description="Web UI for MySQL migration comparison platform.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host for the web server.")
    parser.add_argument("--port", type=int, default=5000, help="Port for the web server.")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_web_parser()
    args = parser.parse_args(argv)
    app = create_app()
    app.run(host=args.host, port=args.port, debug=args.debug)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
