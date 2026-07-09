CREATE TABLE IF NOT EXISTS demo_aggregated (
  `window_start` TIMESTAMP(3),
  `category` STRING,
  `total_value` INT,
  `event_count` BIGINT
) DISTRIBUTED INTO 1 BUCKETS;
