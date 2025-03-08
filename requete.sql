CREATE KEYSPACE IF NOT EXISTS user_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS user_keyspace.random_user_table (
    user_id text,
    ingestion_time timestamp,
    gender text,
    full_name text,
    first_name text,
    last_name text,
    age int,
    country text,
    PRIMARY KEY (user_id, ingestion_time)
);

CREATE TABLE IF NOT EXISTS user_keyspace.country_stats (
    country text PRIMARY KEY,
    count_users bigint,
    avg_age double,
    last_update timestamp
);