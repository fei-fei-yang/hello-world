from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DbConnection:
    host: str
    port: int
    user: str
    password: str


@dataclass(frozen=True)
class TableIdentifier:
    database: str
    table: str


@dataclass(frozen=True)
class EndpointConfig:
    connection: DbConnection
    table: TableIdentifier


@dataclass(frozen=True)
class CompareConfig:
    source: EndpointConfig
    target: EndpointConfig
    output_dir: Path
    max_report_samples: int = 100


@dataclass(frozen=True)
class ColumnMetadata:
    name: str
    ordinal_position: int
    data_type: str
    column_type: str
    is_nullable: str
    column_default: Any
    extra: str
    character_set_name: str | None
    collation_name: str | None
    column_key: str


@dataclass(frozen=True)
class TableSnapshot:
    table: TableIdentifier
    columns: list[ColumnMetadata]
    primary_key: list[str]
    rows: list[dict[str, Any]]
