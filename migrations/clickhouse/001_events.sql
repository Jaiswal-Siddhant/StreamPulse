CREATE TABLE IF NOT EXISTS events (
    event_id String,
    event_type LowCardinality(String),
    source LowCardinality(String),
    user_id String,
    occurred_at DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    schema_version UInt32,
    payload String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (source, event_type, occurred_at, event_id);
