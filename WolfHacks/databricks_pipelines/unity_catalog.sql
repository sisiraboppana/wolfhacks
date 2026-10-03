-- Run as an authorized workspace administrator after confirming catalog ownership.
CREATE CATALOG IF NOT EXISTS wearable;
CREATE SCHEMA IF NOT EXISTS wearable.dev;
-- Execute after the pipeline has created its gold table.
CREATE OR REPLACE FUNCTION wearable.dev.get_session_metrics(p_user_id STRING, p_session_id STRING)
RETURNS TABLE(window_start TIMESTAMP, window_end TIMESTAMP, avg_hr DOUBLE, notification_count BIGINT)
RETURN SELECT window_start, window_end, avg_hr, notification_count
FROM wearable.dev.wearable_gold_1m
WHERE user_id = p_user_id AND session_id = p_session_id;
-- Grant USE CATALOG, USE SCHEMA, and EXECUTE only to the selected agent principal.
-- Caller authentication must bind user/session identity before invoking this tool.
