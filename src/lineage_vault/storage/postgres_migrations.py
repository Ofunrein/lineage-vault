from __future__ import annotations

POSTGRES_MIGRATIONS: list[tuple[int, str]] = [
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS idempotency (
            idempotency_key TEXT PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ledger (
            seq SERIAL PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE,
            payload TEXT NOT NULL,
            prev_hash TEXT NOT NULL,
            entry_hash TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_ledger_event_id ON ledger(event_id);

        CREATE TABLE IF NOT EXISTS wal_staging (
            id SERIAL PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE,
            payload TEXT NOT NULL,
            committed INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_wal_committed ON wal_staging(committed);

        CREATE TABLE IF NOT EXISTS dataset_edges (
            id SERIAL PRIMARY KEY,
            src TEXT NOT NULL,
            dst TEXT NOT NULL,
            transform_id TEXT NOT NULL,
            event_time TEXT NOT NULL,
            schema_version INTEGER NOT NULL,
            payload TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_edges_dst ON dataset_edges(dst);
        CREATE INDEX IF NOT EXISTS idx_edges_src ON dataset_edges(src);
        CREATE INDEX IF NOT EXISTS idx_edges_time ON dataset_edges(event_time);

        CREATE TABLE IF NOT EXISTS field_mappings (
            id SERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            output_dataset TEXT NOT NULL,
            input_dataset TEXT NOT NULL,
            output_field TEXT NOT NULL,
            input_field TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_field_out ON field_mappings(output_dataset, output_field);
        CREATE INDEX IF NOT EXISTS idx_field_in ON field_mappings(input_dataset, input_field);

        CREATE TABLE IF NOT EXISTS run_events (
            run_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY (run_id, event_type)
        );
        """,
    ),
]
