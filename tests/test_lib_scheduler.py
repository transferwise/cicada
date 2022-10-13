"""test_lib_scheduler.py"""

import socket

from cicada.lib import scheduler


def test_get_host_details():
    """test_get_host_details"""
    hostname = socket.gethostname()
    if hostname.find(".") != -1:
        hostname = hostname[: hostname.find(".")]

    fqdn = socket.getfqdn()

    ip4_address = socket.gethostbyname(fqdn)

    host_details = scheduler.get_host_details()

    assert (
        hostname == host_details["hostname"]
        and fqdn == host_details["fqdn"]
        and ip4_address == host_details["ip4_address"]
    )


def test_generate_exec_schedule_command():
    """test_generate_exec_schedule_command"""
    full_command = scheduler.generate_exec_schedule_command("test_schedule_id")

    assert full_command == [
        "/opt/cicada/venv/bin/cicada",
        "exec_schedule",
        "--schedule_id=test_schedule_id",
    ]


def test_generate_exec_schedule_command_no_dbname():
    """test_generate_exec_schedule_command_no_dbname"""
    full_command = scheduler.generate_exec_schedule_command("test_schedule_id", None)

    assert full_command == [
        "/opt/cicada/venv/bin/cicada",
        "exec_schedule",
        "--schedule_id=test_schedule_id",
    ]


def test_generate_exec_schedule_command_with_dbname():
    """test_generate_exec_schedule_command_with_dbname"""
    full_command = scheduler.generate_exec_schedule_command("test_schedule_id", "test_db")

    assert full_command == [
        "/opt/cicada/venv/bin/cicada",
        "exec_schedule",
        "--schedule_id=test_schedule_id",
        "test_db",
    ]
