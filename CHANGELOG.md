0.10.1
-----
- Fix bug in delete_schedule introduced in 0.10.0

0.10.0 
-----
- Add smart_schedule command with optimise and rollback options (as well as blocklist functionality)
- Adds new column to existing table and new tables connected to smart_schedule command

0.9.0
-----
- Verify compatibility with Ubuntu 22.04
- Verify compatibility with PostgreSQL 15
- Update dependencies

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
