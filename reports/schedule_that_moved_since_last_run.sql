/*
    Show schedules that are
    - scheduled to make the next run
    - on a diffirent server to
    - where they ran previous time
*/

with latest_schedule_run as (
  select
    schedule_id,
    max(start_time) as start_time
  from schedule_log
  group by 1
  order by 1
)

select
  s.schedule_id,
  s.interval_mask,
  s.is_running,
  s.is_enabled,
  s.server_id as server_id,
  sl.server_id as latest_run_server_id,
  sl.schedule_log_id as latest_run_schedule_log_id,
  sl.start_time,
  sl.end_time,
  sl.returncode
from latest_schedule_run lsr
  inner join schedule_log sl using (schedule_id, start_time)
  inner join schedules s using (schedule_id)
where s.server_id <> sl.server_id
;
