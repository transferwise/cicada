# +--------- minute (0 - 59)
# | +--------- hour (0 - 23)
# | | +--------- day of the month (1 - 31)
# | | | +--------- month (1 - 12)
# | | | | +--------- day of the week (0 - 7) (Sunday to Saturday; 7 is also Sunday)
# * * * * *

import datetime
from croniter import croniter


def getIsRunning(dbCur, scheduleId):
    sqlquery = """/* Cicada libScheduler */
    SELECT is_running
    FROM schedules
    WHERE schedule_id = '""" + str(scheduleId) + """'
    """

    dbCur.execute(sqlquery)
    row = dbCur.fetchone()
    isRunning = row[0]

    return isRunning


def setIsRunning(dbCur, scheduleId):
    sqlquery = """/* Cicada libScheduler */
    UPDATE schedules
    SET is_running = 1
    WHERE schedule_id = '""" + str(scheduleId) + """'
    """

    dbCur.execute(sqlquery)


def resetIsRunning(dbCur, scheduleId):
    sqlquery = """/* Cicada libScheduler */
    UPDATE schedules
    SET is_running = 0
    WHERE schedule_id = '""" + str(scheduleId) + """'
    """

    dbCur.execute(sqlquery)


def resetAdhocDetails(dbCur, scheduleId):
    sqlquery = """/* Cicada libScheduler */
    UPDATE schedules SET
        adhoc_execute = 0,
        adhoc_parameters = NULL
    WHERE schedule_id = '""" + str(scheduleId) + """'
    """

    dbCur.execute(sqlquery)


def initScheduleLog(dbCur, serverId, scheduleId, fullCommand):
    # Get UUID
    sqlquery = """/* Cicada libScheduler */
    SELECT uuid_generate_v1()
    """
    dbCur.execute(sqlquery)
    row = dbCur.fetchone()
    scheduleLogId = str(row[0])

    sqlquery = """/* Cicada libScheduler */
    INSERT INTO schedule_log
        (schedule_log_id, server_id, schedule_id, full_command, start_time, schedule_log_status_id)
    VALUES
        ('""" + str(scheduleLogId) + """', """ + str(serverId) + """, '""" + str(scheduleId) + """', '""" + str(fullCommand) + """', now() ,1)
    """
    dbCur.execute(sqlquery)

    return scheduleLogId


def finalizeScheduleLog(dbCur, scheduleLogId, returncode, error_detail):
    if returncode == 0:
        scheduleLogStatusId = 2
    else:
        scheduleLogStatusId = 3

    sqlquery = """/* Cicada libScheduler */
    UPDATE schedule_log SET
        end_time = now(),
        returncode = """ + str(returncode) + """,
        error_detail = '""" + str(error_detail) + """',
        schedule_log_status_id = """ + str(scheduleLogStatusId) + """
    WHERE schedule_log_id = '""" + str(scheduleLogId) + """'
    """

    dbCur.execute(sqlquery)


def getScheduleDetails(dbCur, scheduleId):
    """Extract details of a schedule"""
    sqlquery = """/* Cicada libScheduler */
    SELECT
        schedule_description,
        server_id,
        schedule_order,
        is_async,
        is_enabled,
        adhoc_execute,
        interval_mask,
        first_run_date,
        last_run_date,
        command,
        parameters,
        adhoc_parameters,
        schedule_group_id,
        is_running
    FROM schedules
        WHERE schedule_id = '""" + str(scheduleId) + """'
    LIMIT 1
    """

    dbCur.execute(sqlquery)
    objScheduleDetails = dbCur
    return objScheduleDetails


def setScheduleDetails(dbCicada, scheduleId, scheduleDescription, serverId, scheduleOrder,
            isAsync, isEnabled, adhocExecute, intervalMask, firstRunDate, lastRunDate, execCommand,
            parameters, adhocParameters, scheduleGroupId)
    """Set details of a schedule"""
    sqlquery = """/* Cicada libScheduler */
    UPDATE schedules SET
        schedule_description = '""" + str(scheduleDescription) + """',
        server_id = '""" + str(serverId) + """', 
        schedule_order = '""" + str(scheduleOrder) + """',
        is_async = '""" + str(isAsync) + """',
        is_enabled = '""" + str(isEnabled) + """',
        adhoc_execute = '""" + str(adhocExecute) + """',
        interval_mask = '""" + str(intervalMask) + """',
        first_run_date = '""" + str(firstRunDate) + """',
        last_run_date = '""" + str(lastRunDate) + """',
        command = '""" + str(execCommand) + """',
        parameters = '""" + str(parameters) + """',
        adhoc_parameters = '""" + str(adhocParameters) + """',
        schedule_group_id = '""" + str(scheduleGroupId) + """'
    WHERE schedule_id = '""" + str(scheduleId) + """'
    """

    dbCur.execute(sqlquery)


def getScheduleExecutable(dbCur, scheduleId):
    """Extract details of executable of a schedule"""
    sqlquery = """/* Cicada libScheduler */
    SELECT
        command,
        COALESCE(adhoc_parameters, parameters, '') AS parameters
    FROM schedules
        WHERE schedule_id = '""" + str(scheduleId) + """'
    LIMIT 1
    """

    dbCur.execute(sqlquery)
    objScheduleExecutable = dbCur
    return objScheduleExecutable


def getAllSchedules(dbCur, serverId, isAsync):
    """ Extract all candidate schedules for a server
    +--------- minute (0 - 59)
    | +--------- hour (0 - 23)
    | | +--------- day of the month (1 - 31)
    | | | +--------- month (1 - 12)
    | | | | +--------- day of the week (0 - 7) (Sunday to Saturday; 7 is also Sunday)
    * * * * *
    """
    sqlquery = """/* Cicada libScheduler */
    SELECT
      schedule_id,
      interval_mask
    FROM
      ( /* bar */
      SELECT
        schedule_id,
        interval_mask,
        command,
        parameters,
        adhoc_execute,
        is_async,
        is_running
      FROM
        ( /* foo */
          (SELECT
            schedule_id,
            interval_mask,
            command,
            COALESCE(adhoc_parameters, parameters, '') AS parameters,
            adhoc_execute,
            is_async,
            is_running,
            schedule_order
          FROM schedules
            INNER JOIN servers USING (server_id)
          WHERE adhoc_execute = 0
            AND server_id = """ + str(serverId) + """
            AND schedules.is_enabled = 1
            AND servers.is_enabled = 1
            AND now() >= first_run_date
            AND now() <= last_run_date
          )

          UNION

          (SELECT
            schedule_id,
            '* * * * *' AS interval_mask,
            command,
            COALESCE(adhoc_parameters, parameters, '') AS parameters,
            adhoc_execute,
            is_async,
            is_running,
            schedule_order
          FROM schedules
            INNER JOIN servers USING (server_id)
          WHERE adhoc_execute = 1
            AND server_id = """ + str(serverId) + """
            AND servers.is_enabled = 1
          )
        ) foo
      ORDER BY schedule_order, schedule_id
      ) bar

    WHERE is_running = 0
        AND is_async = """ + str(isAsync) + """
    """

    dbCur.execute(sqlquery)
    curSchedules = dbCur

    objSchedules = list()

    nowMinute = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:00'), '%Y-%m-%d %H:%M:%S')

    for sRow in curSchedules.fetchall():
        scheduleId = str(sRow[0])
        intervalMask = str(sRow[1])

        iter = croniter(intervalMask, nowMinute - datetime.timedelta(minutes=1))
        nextIter = iter.get_next(datetime.datetime)

        if nowMinute == nextIter:
            objSchedules.append(scheduleId)

    return objSchedules
