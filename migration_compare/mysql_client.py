from __future__ import annotations

from typing import Any

import pymysql
from pymysql.cursors import DictCursor

from .models import ColumnMetadata, DbConnection, TableIdentifier, TableSnapshot


def quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


class MySQLSnapshotReader:
    """Read table schema and data from MySQL."""

    def __init__(self, connection: DbConnection) -> None:
        self._connection_info = connection
        self._connection: pymysql.connections.Connection | None = None

    def __enter__(self) -> "MySQLSnapshotReader":
        self._connection = pymysql.connect(
            host=self._connection_info.host,
            port=self._connection_info.port,
            user=self._connection_info.user,
            password=self._connection_info.password,
            charset="utf8mb4",
            cursorclass=DictCursor,
            autocommit=True,
        )
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    @property
    def connection(self) -> pymysql.connections.Connection:
        if self._connection is None:
            raise RuntimeError("Database connection is not initialized.")
        return self._connection

    def load_table_snapshot(self, table: TableIdentifier) -> TableSnapshot:
        columns = self.fetch_columns(table)
        if not columns:
            raise ValueError(f"Table {table.database}.{table.table} does not exist or has no columns.")

        primary_key = self.fetch_primary_key(table)
        rows = self.fetch_rows(table, [column.name for column in columns])
        return TableSnapshot(table=table, columns=columns, primary_key=primary_key, rows=rows)

    def fetch_columns(self, table: TableIdentifier) -> list[ColumnMetadata]:
        sql = """
            SELECT
                COLUMN_NAME,
                ORDINAL_POSITION,
                DATA_TYPE,
                COLUMN_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                EXTRA,
                CHARACTER_SET_NAME,
                COLLATION_NAME,
                COLUMN_KEY
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (table.database, table.table))
            rows = cursor.fetchall()

        return [
            ColumnMetadata(
                name=row["COLUMN_NAME"],
                ordinal_position=row["ORDINAL_POSITION"],
                data_type=row["DATA_TYPE"],
                column_type=row["COLUMN_TYPE"],
                is_nullable=row["IS_NULLABLE"],
                column_default=row["COLUMN_DEFAULT"],
                extra=row["EXTRA"],
                character_set_name=row["CHARACTER_SET_NAME"],
                collation_name=row["COLLATION_NAME"],
                column_key=row["COLUMN_KEY"],
            )
            for row in rows
        ]

    def fetch_primary_key(self, table: TableIdentifier) -> list[str]:
        sql = """
            SELECT COLUMN_NAME
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND INDEX_NAME = 'PRIMARY'
            ORDER BY SEQ_IN_INDEX
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (table.database, table.table))
            rows = cursor.fetchall()
        return [row["COLUMN_NAME"] for row in rows]

    def fetch_rows(self, table: TableIdentifier, columns: list[str]) -> list[dict[str, Any]]:
        if not columns:
            return []

        column_sql = ", ".join(quote_identifier(column) for column in columns)
        table_sql = f"{quote_identifier(table.database)}.{quote_identifier(table.table)}"
        sql = f"SELECT {column_sql} FROM {table_sql}"

        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
        return rows
