0.8.3
-----
- Switch to using `sslmode=require` for connections to backend DB

0.8.2
-----
- Switch to new PyPi publish method

0.8.1
-----
- Switch to using `sslmode=prefer` for connections to backend DB

0.7.0
-----
- Add delete_schedule command
- Add list_schedule_ids command


0.5.1
-----
- Bug fix setup


0.5.0
-----
- Sending slack alerts for not 0 return codes


0.4.1
-----
- Add --only_enabled_servers option to spread_schedules


0.4.0
-----
- abort_running to only SIGTERM the process that has been launched by Cicada, and not any child processes.


0.3.3
-----
- Don't keep database connection open while running job


0.3.2
-----
- Minor and cosmetic improvements for PyPI
