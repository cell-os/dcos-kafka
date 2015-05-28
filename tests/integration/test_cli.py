from common import exec_command


def test_help():
    returncode, stdout, stderr = exec_command(
        ['dcos-kafka', 'kafka', 'help'])

    assert returncode == 0
    assert stdout.startswith(b"Usage:")
    assert stderr == b''
