INSERT INTO demo_aggregated
/*+ OPTIONS('properties.transaction.timeout.ms'='300000') */
SELECT
  window_start,
  category,
  SUM(`value`) AS total_value,
  COUNT(`id`) AS event_count
FROM
  TABLE(
    TUMBLE(
      TABLE demo_events,
      DESCRIPTOR(event_time),
      INTERVAL '30' SECOND
    )
  )
GROUP BY
  window_start,
  window_end,
  category;
