CREATE TABLE IF NOT EXISTS demo_events (
  `id` STRING,
  `value` INT,
  `event_time` TIMESTAMP(3),
  `category` STRING,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) DISTRIBUTED INTO 1 BUCKETS;
