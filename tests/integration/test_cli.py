from common import exec_command


def test_help():
    returncode, stdout, stderr = exec_command(
        ['dcos-kafka', 'kafka', '--help'])

    assert returncode == 0
    assert stdout == b"""DCOS Kafka Example Subcommand

Usage:
    dcos kafka info

Options:
    --help           Show this screen
    --version        Show version
"""
    assert stderr == b''
