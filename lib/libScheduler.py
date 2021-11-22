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
        schedule_id
        ,schedule_description
        ,server_id
        ,schedule_order
        ,is_async
        ,is_enabled
        ,adhoc_execute
        ,interval_mask
        ,first_run_date
        ,last_run_date
        ,command
        ,parameters
        ,adhoc_parameters
        ,schedule_group_id
        ,is_running
    FROM schedules
        WHERE schedule_id = '""" + str(scheduleId) + """'
    LIMIT 1
    """

    dbCur.execute(sqlquery)
    curScheduleDetails = dbCur

    objScheduleDetails = dict()

    for sRow in curScheduleDetails.fetchall():
        objScheduleDetails['scheduleId'] = sRow[0]
        objScheduleDetails['scheduleDescription'] = sRow[1]
        objScheduleDetails['serverId'] = sRow[2]
        objScheduleDetails['scheduleOrder'] = sRow[3]
        objScheduleDetails['isAsync'] = sRow[4]
        objScheduleDetails['isEnabled'] = sRow[5]
        objScheduleDetails['adhocExecute'] = sRow[6]
        objScheduleDetails['intervalMask'] = sRow[7]
        objScheduleDetails['firstRunDate'] = sRow[8]
        objScheduleDetails['lastRunDate'] = sRow[9]
        objScheduleDetails['execCommand'] = sRow[10]
        objScheduleDetails['parameters'] = sRow[11]
        objScheduleDetails['adhocParameters'] = sRow[12]
        objScheduleDetails['scheduleGroupId'] = sRow[13]
        objScheduleDetails['isRunning'] = sRow[14]

    return objScheduleDetails


def insertScheduleDetails(dbCur, scheduleSettings):
    """Insert a new schedule"""
    sqlquery = "/* Cicada libScheduler */\n"
    sqlquery = sqlquery + "INSERT INTO schedules (schedule_id"
    if scheduleSettings['scheduleDescription'] is not None:
        sqlquery = sqlquery + " ,schedule_description"
    if scheduleSettings['serverId'] is not None:
        sqlquery = sqlquery + " ,server_id"
    if scheduleSettings['scheduleOrder'] is not None:
        sqlquery = sqlquery + " ,schedule_order"
    if scheduleSettings['isAsync'] is not None:
        sqlquery = sqlquery + " ,is_async"
    if scheduleSettings['isEnabled'] is not None:
        sqlquery = sqlquery + " ,is_enabled"
    if scheduleSettings['adhocExecute'] is not None:
        sqlquery = sqlquery + " ,adhoc_execute"
    if scheduleSettings['intervalMask'] is not None:
        sqlquery = sqlquery + " ,interval_mask"
    if scheduleSettings['firstRunDate'] is not None:
        sqlquery = sqlquery + " ,first_run_date"
    if scheduleSettings['lastRunDate'] is not None:
        sqlquery = sqlquery + " ,last_run_date"
    if scheduleSettings['execCommand'] is not None:
        sqlquery = sqlquery + " ,command"
    if scheduleSettings['parameters'] is not None:
        sqlquery = sqlquery + " ,parameters"
    if scheduleSettings['adhocParameters'] is not None:
        sqlquery = sqlquery + " ,adhoc_parameters"
    if scheduleSettings['scheduleGroupId'] is not None:
        sqlquery = sqlquery + " ,schedule_group_id"

    sqlquery = sqlquery + ") VALUES ('" + str(scheduleSettings['scheduleId']) + "'"

    if scheduleSettings['scheduleDescription'] is not None:
        sqlquery = sqlquery + " ,'" + str(scheduleSettings['scheduleDescription']) + "'"
    if scheduleSettings['serverId'] is not None:
        sqlquery = sqlquery + " ," + str(scheduleSettings['serverId'])
    if scheduleSettings['scheduleOrder'] is not None:
        sqlquery = sqlquery + " ," + str(scheduleSettings['scheduleOrder'])
    if scheduleSettings['isAsync'] is not None:
        sqlquery = sqlquery + " ," + str(scheduleSettings['isAsync'])
    if scheduleSettings['isEnabled'] is not None:
        sqlquery = sqlquery + " ," + str(scheduleSettings['isEnabled'])
    if scheduleSettings['adhocExecute'] is not None:
        sqlquery = sqlquery + " ," + str(scheduleSettings['adhocExecute'])
    if scheduleSettings['intervalMask'] is not None:
        sqlquery = sqlquery + " ,'" + str(scheduleSettings['intervalMask']) + "'"
    if scheduleSettings['firstRunDate'] is not None:
        sqlquery = sqlquery + " ,'" + str(scheduleSettings['firstRunDate']) + "'"
    if scheduleSettings['lastRunDate'] is not None:
        sqlquery = sqlquery + " ,'" + str(scheduleSettings['lastRunDate']) + "'"
    if scheduleSettings['execCommand'] is not None:
        sqlquery = sqlquery + " ,'" + str(scheduleSettings['execCommand']) + "'"
    if scheduleSettings['parameters'] is not None:
        sqlquery = sqlquery + " ,'" + str(scheduleSettings['parameters']) + "'"
    if scheduleSettings['adhocParameters'] is not None:
        sqlquery = sqlquery + " ,'" + str(scheduleSettings['adhocParameters']) + "'"
    if scheduleSettings['scheduleGroupId'] is not None:
        sqlquery = sqlquery + " ," + str(scheduleSettings['scheduleGroupId'])

    sqlquery = sqlquery + ")"

    dbCur.execute(sqlquery)
    # print(sqlquery)


def updateScheduleDetails(dbCur, scheduleSettings):
    """Update and existing schedule"""
    sqlquery = "/* Cicada libScheduler */\n"
    sqlquery = sqlquery + "UPDATE schedules SET"
    sqlquery = sqlquery + " schedule_id = '" + str(scheduleSettings['scheduleId']) + "'"

    if scheduleSettings['scheduleDescription'] is not None:
        sqlquery = sqlquery + " ,schedule_description = '" + str(scheduleSettings['scheduleDescription']) + "'"
    if scheduleSettings['serverId'] is not None:
        sqlquery = sqlquery + " ,server_id = " + str(scheduleSettings['serverId'])
    if scheduleSettings['scheduleOrder'] is not None:
        sqlquery = sqlquery + " ,schedule_order = " + str(scheduleSettings['scheduleOrder'])
    if scheduleSettings['isAsync'] is not None:
        sqlquery = sqlquery + " ,is_async = " + str(scheduleSettings['isAsync'])
    if scheduleSettings['isEnabled'] is not None:
        sqlquery = sqlquery + " ,is_enabled = " + str(scheduleSettings['isEnabled'])
    if scheduleSettings['adhocExecute'] is not None:
        sqlquery = sqlquery + " ,adhoc_execute = " + str(scheduleSettings['adhocExecute'])
    if scheduleSettings['intervalMask'] is not None:
        sqlquery = sqlquery + " ,interval_mask = '" + str(scheduleSettings['intervalMask']) + "'"
    if scheduleSettings['firstRunDate'] is not None:
        sqlquery = sqlquery + " ,first_run_date = '" + str(scheduleSettings['firstRunDate']) + "'"
    if scheduleSettings['lastRunDate'] is not None:
        sqlquery = sqlquery + " ,last_run_date = '" + str(scheduleSettings['lastRunDate']) + "'"
    if scheduleSettings['execCommand'] is not None:
        sqlquery = sqlquery + " ,command = '" + str(scheduleSettings['execCommand']) + "'"
    if scheduleSettings['parameters'] is not None:
        sqlquery = sqlquery + " ,parameters = '" + str(scheduleSettings['parameters']) + "'"
    if scheduleSettings['adhocParameters'] is not None:
        sqlquery = sqlquery + " ,adhoc_parameters = '" + str(scheduleSettings['adhocParameters']) + "'"
    if scheduleSettings['scheduleGroupId'] is not None:
        sqlquery = sqlquery + " ,schedule_groupId = " + str(scheduleSettings['scheduleGroupId'])
    sqlquery = sqlquery + " WHERE schedule_id = '" + str(scheduleSettings['scheduleId']) + "'"

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

        # Skip entries with a bad intervalMask
        if croniter.is_valid(intervalMask):
            iter = croniter(intervalMask, nowMinute - datetime.timedelta(minutes=1))
            nextIter = iter.get_next(datetime.datetime)

            if nowMinute == nextIter:
                objSchedules.append(scheduleId)

    return objSchedules


def getSchedulesLoadYesterday(dbCur, serverIds: [int] = None):
    """Extract details of executable of a schedule"""
    sqlServerIdFilter = ""
    if serverIds:
        sqlServerIds = ','.join(str(serverId) for serverId in serverIds)
        sqlServerIdFilter = "and server_id in (" + sqlServerIds + ")"

    sqlquery = """/* Cicada libScheduler */
    select
        schedule_id,
        sum(end_time - start_time) as total_run_duration
    from schedule_log
    where start_time > to_char(now() - interval '1 DAY', 'YYYY-MM-DD 00:00:00')::timestamp
        and start_time < to_char(now(), 'YYYY-MM-DD 00:00:00')::timestamp
        """ + sqlServerIdFilter + """
    group by schedule_id
    order by 2 desc
    """

    dbCur.execute(sqlquery)
    curSchedulesLoadYesterday = dbCur

    objSchedulesLoadYesterday = list()
    for sRow in curSchedulesLoadYesterday.fetchall():
        objSchedulesLoadYesterday.append(str(sRow[0]))

    return objSchedulesLoadYesterday


def getServers(dbCur, enabled_only: bool = True, serverIds: [int] = None):
    """Extract details of executable of a schedule"""
    sqlEnabledFilter = " and is_enabled = 1" if enabled_only else ""
    sqlServerIdFilter = ""
    if serverIds:
        sqlServerIds = ','.join(str(serverId) for serverId in serverIds)
        sqlServerIdFilter = " and server_id in (" + sqlServerIds + ")"

    sqlquery = """/* Cicada libScheduler */
    select server_id from servers
    where 1 = 1
    """ + sqlEnabledFilter + """
    """ + sqlServerIdFilter + """
    order by server_id
    """

    dbCur.execute(sqlquery)
    curEnabledServers = dbCur

    objEnabledServers = list()
    for sRow in curEnabledServers.fetchall():
        objEnabledServers.append(str(sRow[0]))

    return objEnabledServers