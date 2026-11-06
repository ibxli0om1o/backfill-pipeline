-- Rebuild one day of analytics.events from the immutable raw landing table.
-- The delete makes a rerun a no-op instead of a double load.

DELETE FROM {table} WHERE {column} = '{partition_date}'::date;

INSERT INTO {table} (event_id, user_id, event_type, occurred_at)
SELECT event_id, user_id, event_type, occurred_at
FROM raw_events
WHERE occurred_at >= '{partition_date}'::date
  AND occurred_at <  '{partition_date}'::date + interval '1 day'
ON CONFLICT (event_id) DO NOTHING;
