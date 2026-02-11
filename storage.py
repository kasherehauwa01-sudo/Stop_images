from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional


class Storage:
    # Пояснение: единое SQLite-хранилище для кэша, статусов строк и прогресса партий.
    def __init__(self, db_path: Path):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS image_cache (
                image_hash TEXT PRIMARY KEY,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS row_status (
                row_key TEXT PRIMARY KEY,
                image_hash TEXT,
                status TEXT NOT NULL,
                error_text TEXT,
                matches_count INTEGER NOT NULL DEFAULT 0,
                processed_at TEXT
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS report_rows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                row_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS batch_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                start_row INTEGER NOT NULL,
                batch_size INTEGER NOT NULL,
                top_n INTEGER NOT NULL,
                processed_count INTEGER NOT NULL,
                error_count INTEGER NOT NULL,
                match_count INTEGER NOT NULL
            )
            """
        )
        self.conn.commit()

    def get_cached_results(self, image_hash: str) -> Optional[List[Dict[str, str]]]:
        row = self.conn.execute(
            "SELECT result_json FROM image_cache WHERE image_hash = ?", (image_hash,)
        ).fetchone()
        return json.loads(row[0]) if row else None

    def set_cached_results(self, image_hash: str, payload: List[Dict[str, str]]) -> None:
        self.conn.execute(
            """
            INSERT INTO image_cache (image_hash, result_json, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(image_hash) DO UPDATE SET
                result_json = excluded.result_json,
                created_at = excluded.created_at
            """,
            (image_hash, json.dumps(payload, ensure_ascii=False), dt.datetime.utcnow().isoformat()),
        )
        self.conn.commit()

    def upsert_row_status(
        self,
        row_key: str,
        status: str,
        image_hash: Optional[str],
        matches_count: int,
        error_text: Optional[str] = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO row_status (row_key, image_hash, status, error_text, matches_count, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(row_key) DO UPDATE SET
                image_hash = excluded.image_hash,
                status = excluded.status,
                error_text = excluded.error_text,
                matches_count = excluded.matches_count,
                processed_at = excluded.processed_at
            """,
            (
                row_key,
                image_hash,
                status,
                error_text,
                matches_count,
                dt.datetime.utcnow().isoformat(),
            ),
        )
        self.conn.commit()

    def add_report_rows(self, row_key: str, report_rows: List[Dict[str, str]]) -> None:
        for item in report_rows:
            self.conn.execute(
                "INSERT INTO report_rows (row_key, payload_json, created_at) VALUES (?, ?, ?)",
                (row_key, json.dumps(item, ensure_ascii=False), dt.datetime.utcnow().isoformat()),
            )
        self.conn.commit()

    def get_report_rows_for_keys(self, row_keys: List[str]) -> List[Dict[str, str]]:
        if not row_keys:
            return []
        placeholders = ",".join(["?"] * len(row_keys))
        rows = self.conn.execute(
            f"SELECT payload_json FROM report_rows WHERE row_key IN ({placeholders})", row_keys
        ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def create_batch_run(self, start_row: int, batch_size: int, top_n: int) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO batch_runs (
                started_at, start_row, batch_size, top_n, processed_count, error_count, match_count
            ) VALUES (?, ?, ?, ?, 0, 0, 0)
            """,
            (dt.datetime.utcnow().isoformat(), start_row, batch_size, top_n),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_batch_run(self, run_id: int, processed_count: int, error_count: int, match_count: int) -> None:
        self.conn.execute(
            """
            UPDATE batch_runs
            SET completed_at = ?, processed_count = ?, error_count = ?, match_count = ?
            WHERE id = ?
            """,
            (dt.datetime.utcnow().isoformat(), processed_count, error_count, match_count, run_id),
        )
        self.conn.commit()

    def stats_for_keys(self, row_keys: List[str]) -> Dict[str, int]:
        if not row_keys:
            return {"processed": 0, "errors": 0, "matches": 0}
        placeholders = ",".join(["?"] * len(row_keys))
        rows = self.conn.execute(
            f"SELECT status, matches_count FROM row_status WHERE row_key IN ({placeholders})", row_keys
        ).fetchall()
        processed = len(rows)
        errors = sum(1 for r in rows if r["status"] == "error")
        matches = sum(int(r["matches_count"]) for r in rows)
        return {"processed": processed, "errors": errors, "matches": matches}
