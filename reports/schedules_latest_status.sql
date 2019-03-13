-- Shows the status of the latest run of each schedule, except test schedules

SELECT
  schedule_log_id,
  server_id,
  server_name,
  schedule_id,
  schedule_description,
  is_enabled,
  start_time,
  end_time,
  run_duration,
  schedule_status
FROM

( /* bar */
SELECT
  schedule_id,
  MAX(start_time) AS start_time
FROM schedule_log
GROUP BY schedule_id
) AS bar

INNER JOIN

( /* foo */
SELECT
  schedule_log_id,
  schedule_log.server_id AS server_id,
  servers.hostname AS server_name,
  schedule_id,
  description AS schedule_description,
  is_enabled,
  start_time,
  end_time,
  (COALESCE(end_time, now()::timestamp(3)) - start_time) AS run_duration,
  schedule_log_status.name AS schedule_status
FROM schedules
  INNER JOIN servers USING (server_id)
  INNER JOIN schedule_log USING (schedule_id)
  INNER JOIN schedule_log_status USING (schedule_log_status_id)
WHERE (schedule_group_id IS NULL OR schedule_group_id NOT IN (2))
ORDER BY schedule_id, start_time
) AS foo

USING (schedule_id, start_time)

ORDER BY schedule_id