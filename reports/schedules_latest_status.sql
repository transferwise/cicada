-- Shows the status of the latest run of each schedule, except test schedules

SELECT
  server_id,
  server_name,
  schedule_id,
  schedule_description,
  start_time,
  end_time,
  schedule_status
FROM

( /* foo */
SELECT
  servers.server_id AS server_id,
  servers.hostname AS server_name,
  schedule_id,
  description AS schedule_description,
  start_time,
  end_time,
  schedule_log_status.name AS schedule_status
FROM servers
  INNER JOIN schedules USING (server_id)
  LEFT JOIN schedule_log USING (schedule_id)
  LEFT JOIN schedule_log_status USING (schedule_log_status_id)
WHERE (schedule_group_id IS NULL OR schedule_group_id NOT IN (2))
ORDER BY schedule_id, start_time
) AS foo

INNER JOIN

( /* bar */
SELECT
  schedule_id,
  MAX(start_time) AS start_time
FROM schedule_log
GROUP BY schedule_id
) AS bar

USING (schedule_id, start_time)

ORDER BY schedule_id