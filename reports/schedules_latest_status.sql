-- Shows the status of the latest run of each schedule

SELECT
  schedule_log_id,
  server_id,
  server_name,
  server_is_enabled,
  schedule_id,
  schedule_description,
  schedule_is_enabled,
  start_time,
  end_time,
  run_duration,
  returncode,
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
  servers.is_enabled AS server_is_enabled,
  schedule_id,
  schedule_description,
  schedules.is_enabled AS schedule_is_enabled,
  start_time,
  end_time,
  (COALESCE(end_time, now()::timestamp(3)) - start_time) AS run_duration,
  returncode,
  CASE
    WHEN end_time IS NOT null and returncode = 0 THEN 'Success'
    WHEN end_time IS NOT null and returncode <> 0 THEN 'Failure'
    WHEN end_time IS null AND returncode IS null THEN 'Running'
    ELSE 'Unknown'
  END AS schedule_status
FROM schedules
    INNER JOIN servers USING (server_id)
    INNER JOIN schedule_log USING (schedule_id)
ORDER BY schedule_id, start_time
) AS foo

USING (schedule_id, start_time)

ORDER BY schedule_id
