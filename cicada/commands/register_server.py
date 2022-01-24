"""Register this server."""

from cicada.lib import postgres
from cicada.lib import utils
from cicada.lib import scheduler


@utils.named_exception_handler("register_server")
def main(dbname=None):
    """Register this server."""

    host_details = scheduler.get_host_details()

    sqlquery = f"""
    INSERT INTO servers
      (hostname, fqdn, ip4_address, is_enabled)
    VALUES
      ('{host_details['hostname']}','{host_details['fqdn']}','{host_details['ip4_address']}', 1 )
    ON CONFLICT DO NOTHING
    """

    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    db_cur.execute(sqlquery)
    db_cur.close()
    db_conn.close()
