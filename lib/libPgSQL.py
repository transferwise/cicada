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

# Get short hostname
hostname = socket.gethostname()
i = hostname.find(".")
if i != -1:
    hostname = hostname[:i]


# Create a PgSQL database connection based on definition requested
def init_db():
    with open(os.path.abspath(os.path.dirname(sys.argv[0]) + '/../config/env_def.yml'), 'r') as env_def_yml:
        env_def = yaml.load(env_def_yml)

    host = env_def['db_config']['host']
    port = env_def['db_config']['port']
    dbname = env_def['db_config']['dbname']
    user = env_def['db_config']['user']
    password = env_def['db_config']['password']

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
def getServerId(dbCur):
    sqlquery = """/* DB Management libPgSQL */
    SELECT server_id
    FROM servers
    WHERE name='""" + hostname + """'
    """

    dbCur.execute(sqlquery)
    row = dbCur.fetchone()

    try:
        serverId = str(row[0])
        return serverId
    except Exception as e:
        print("ERROR : host " + hostname + " not defined in table servers")
        sys.exit()


def singleCommand(dbCur, command):
    sqlquery = command
    dbCur.execute(sqlquery)
