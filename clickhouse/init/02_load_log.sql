CREATE TABLE IF NOT EXISTS raw._load_log
(
    table_name   LowCardinality(String),
    file_path    String,
    rows_loaded  UInt64,
    loaded_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (table_name, file_path);