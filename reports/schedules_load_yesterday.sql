-- Show schedules that ran the most yesterday

select
    schedule_id,
    sum(end_time - start_time) as total_run_duration
from schedule_log
where start_time > to_char(now() - interval '1 DAY', 'YYYY-MM-DD 00:00:00')::timestamp
    and start_time < to_char(now(), 'YYYY-MM-DD 00:00:00')::timestamp
group by schedule_id
order by 2 desc
