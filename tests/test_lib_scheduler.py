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


def test_get_full_command_simple():
    """test_get_full_command with simple command and parameters"""
    command = "sleep"
    parameters = "0.5"

    result = scheduler.get_full_command(command, parameters)

    assert result == ["sleep", "0.5"]


def test_get_full_command_no_parameters():
    """test_get_full_command with command only, no parameters"""
    command = "echo"
    parameters = ""

    result = scheduler.get_full_command(command, parameters)

    assert result == ["echo"]


def test_get_full_command_multiple_parameters():
    """test_get_full_command with multiple space-separated parameters"""
    command = "echo"
    parameters = "hello world test"

    result = scheduler.get_full_command(command, parameters)

    assert result == ["echo", "hello", "world", "test"]


def test_get_full_command_quoted_parameters():
    """test_get_full_command with quoted parameters containing spaces"""
    command = "echo"
    parameters = '"hello world" "foo bar"'

    result = scheduler.get_full_command(command, parameters)

    assert result == ["echo", "hello world", "foo bar"]


def test_get_full_command_single_quotes():
    """test_get_full_command with single-quoted parameters"""
    command = "echo"
    parameters = "'hello world' 'test value'"

    result = scheduler.get_full_command(command, parameters)

    assert result == ["echo", "hello world", "test value"]


def test_get_full_command_mixed_quotes():
    """test_get_full_command with mixed single and double quotes"""
    command = "echo"
    parameters = "\"double quoted\" 'single quoted' unquoted"

    result = scheduler.get_full_command(command, parameters)

    assert result == ["echo", "double quoted", "single quoted", "unquoted"]


def test_get_full_command_with_special_characters():
    """test_get_full_command with special shell characters in quoted strings"""
    command = "echo"
    parameters = '"test$var" "path/to/file" "value&more"'

    result = scheduler.get_full_command(command, parameters)

    assert result == ["echo", "test$var", "path/to/file", "value&more"]


def test_get_full_command_with_escaped_quotes():
    """test_get_full_command with escaped quotes inside quoted strings"""
    command = "echo"
    parameters = r'"she said \"hello\""'

    result = scheduler.get_full_command(command, parameters)

    assert result == ["echo", 'she said "hello"']


def test_get_full_command_with_equals_signs():
    """test_get_full_command with equals signs in parameters (common in env vars)"""
    command = "env"
    parameters = "VAR1=value1 VAR2=value2"

    result = scheduler.get_full_command(command, parameters)

    assert result == ["env", "VAR1=value1", "VAR2=value2"]


def test_get_full_command_with_flags():
    """test_get_full_command with command flags and parameters"""
    command = "python3"
    parameters = "-m pytest --verbose"

    result = scheduler.get_full_command(command, parameters)

    assert result == ["python3", "-m", "pytest", "--verbose"]


def test_get_full_command_with_path_spaces():
    """test_get_full_command with file paths containing spaces"""
    command = "cat"
    parameters = '"/path/with spaces/file.txt"'

    result = scheduler.get_full_command(command, parameters)

    assert result == ["cat", "/path/with spaces/file.txt"]


def test_get_full_command_command_with_path():
    """test_get_full_command with full path in command"""
    command = "/usr/bin/python3"
    parameters = "script.py arg1 arg2"

    result = scheduler.get_full_command(command, parameters)

    assert result == ["/usr/bin/python3", "script.py", "arg1", "arg2"]


def test_get_full_command_whitespace_handling():
    """test_get_full_command properly handles extra whitespace"""
    command = "echo"
    parameters = "  hello    world  "

    result = scheduler.get_full_command(command, parameters)

    assert result == ["echo", "hello", "world"]


def test_get_full_command_semicolon_in_quotes():
    """test_get_full_command with semicolon in quotes (should not execute as separate command)"""
    command = "echo"
    parameters = '"command1; command2"'

    result = scheduler.get_full_command(command, parameters)

    assert result == ["echo", "command1; command2"]


def test_get_full_command_pipe_in_quotes():
    """test_get_full_command with pipe character in quotes (should not pipe)"""
    command = "echo"
    parameters = '"value1 | value2"'

    result = scheduler.get_full_command(command, parameters)

    assert result == ["echo", "value1 | value2"]


def test_get_full_command_ampersand_in_quotes():
    """test_get_full_command with ampersand in quotes (should not background)"""
    command = "echo"
    parameters = '"command & background"'

    result = scheduler.get_full_command(command, parameters)

    assert result == ["echo", "command & background"]
