#!/usr/bin/python
# 2015-07-01 Louis Pieterse
# #####################################################
# ## Import Database environmental definitions ########
# #####################################################

import os
import socket
import sys
import yaml

# #####################################################
# ## Define DB connections ############################
# #####################################################
# Requires python3 -m pip install psycopg2-binary
# Psycopg Developer info
#    http://initd.org/psycopg/docs/install.html
#    http://initd.org/psycopg/docs/connection.html
import psycopg2

# Get hostname
hostname = socket.gethostname()
if hostname.find(".") != -1:
    hostname = hostname[:hostname.find(".")]
else:
    hostname = hostname

fqdn = socket.getfqdn()

ip4Address = socket.gethostbyname(fqdn)


# Create a PgSQL database connection based on definition requested
def init_db():
    with open(os.path.abspath(os.path.dirname(sys.argv[0]) + '/../config/definitions.yml'), 'r') as definitions_yml:
        definitions = yaml.load(definitions_yml)

    host = definitions['db_cicada']['host']
    port = definitions['db_cicada']['port']
    dbname = definitions['db_cicada']['dbname']
    user = definitions['db_cicada']['user']
    password = definitions['db_cicada']['password']

    conn_string = "host='{0}' port='{1}' dbname='{2}' user='{3}' password='{4}'".format(host, port, dbname, user, password)
    database = psycopg2.connect(conn_string)

    # Set connection to autocommit
    database.autocommit = True

    cursor = database.cursor()

    return cursor


# Close a PgSQL connection
def close_db(cursor):
    cursor.close()


# #####################################################
# ## PgSQL related utilities ##########################
# #####################################################
def singleCommand(dbCur, command):
    sqlquery = command
    dbCur.execute(sqlquery)


def registerServer(dbCur):
    sqlquery = """/* Cicada libScheduler */
    INSERT INTO servers (hostname, fqdn, ip4_address, is_enabled)
      VALUES
    ('""" + hostname + """','""" + fqdn + """','""" + ip4Address + """', 1 )
    ON CONFLICT DO NOTHING
    """

    dbCur.execute(sqlquery)


def getServerId(dbCur):
    sqlquery = """/* DB Management libPgSQL */
    SELECT server_id
    FROM servers
    WHERE hostname='""" + hostname + """'
    """

    dbCur.execute(sqlquery)
    row = dbCur.fetchone()

    try:
        serverId = str(row[0])
        return serverId
    except Exception as e:
        error_detail = e.output
        print("ERROR : " + hostname + " not defined in table servers | DETAILS : " + error_detail)
        sys.exit()
