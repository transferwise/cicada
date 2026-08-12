0.10.4
------

Runtime
~~~~~~~
- After an `abort_running` request, keep the schedule marked as running until its process has stopped
- Clear additional abort requests while the process is stopping so they do not stop the next run
- If Cicada temporarily cannot check the process, keep waiting instead of reporting the schedule as complete
- When Cicada is asked to shut down, stop the launched process and wait for it to exit
- Check and clear `abort_running` in one database operation
- Use consistent database outage alerts and close database connections after errors
- Prevent repeated process-check errors from using all available CPU while Cicada waits for the process to stop
- Save process errors safely when a return code is missing or the error message contains punctuation

Tests
~~~~~
- Add tests for slow process shutdown, repeated abort requests, stopping Cicada, process-check failures, and safe error logging


0.10.3
------

Documentation
~~~~~~~~~~~~~
- Add editable Excalidraw source and refresh the database ERD

Tests and CI
~~~~~~~~~~~~
- Format the existing Python codebase with Black so the formatting check passes
- Run Flake8 and Black for every pull request and push to `main`
- Wait for Docker readiness and report container logs when the pytest workflow fails
- Run the full pytest suite once and always clean up its Docker environment
- Set the minimum test coverage to 78%
- Update CI to Ubuntu 22.04 and the version 6 checkout and Python setup actions


0.10.2
------
- Prevent disabled taps from being included in smart scheduling calculations
- Reset the smart_interval_mask when running spread_schedules
- Modify evaluation criteria to disincentivise overlaps for the first minute of the run

0.10.1
------
- Fix bug in delete_schedule introduced in 0.10.0


0.10.0
------
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
