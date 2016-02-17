#
#    Copyright (C) 2015 Mesosphere, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""DCOS Kafka"""
from __future__ import print_function

import os
import subprocess
import sys

import pkg_resources
from dcos import marathon, util
from dcos_kafka import constants


def api_url(app_id="kafka"):
    client = marathon.create_client()
    tasks = client.get_tasks(app_id)

    if len(tasks) == 0:
        raise CliError("Kafka is not running")

    base_url = util.get_config().get('kafka.url')
    if base_url != None:
        base_url = base_url.rstrip("/")

    dcos_url = util.get_config().get('core.dcos_url')
    if dcos_url != None:
        base_url = util.get_config().get('core.dcos_url').rstrip("/")
        base_url += '/service/kafka'

    cell_url = util.get_config().get('core.cell_url')
    if cell_url != None:
        base_url = cell_url.format(service=app_id.replace("/", "_"))

    return base_url

def find_java():
    def executable(file_path):
        return os.path.isfile(file_path) and os.access(file_path, os.X_OK)

    java_binary = 'java'
    if util.is_windows_platform():
        java_binary = java_binary + '.exe'

    java_home = os.environ.get('JAVA_HOME')
    if java_home is not None and executable(java_home + "/bin/" + java_binary):
        return java_home + "/bin/" + java_binary

    if 'PATH' in os.environ:
        for path in os.environ['PATH'].split(os.pathsep):
            path = path.strip('"')
            java_file = os.path.join(path, java_binary)

            if executable(java_file):
                return java_file

    raise CliError("This command requires Java to be installed. "
                   "Please install JRE")


def find_jar():
    for f in pkg_resources.resource_listdir('dcos_kafka', None):
        if f.startswith("kafka-mesos") and f.endswith(".jar"):
            return pkg_resources.resource_filename('dcos_kafka', f)

    raise CliError("kafka-mesos*.jar not found in package resources")


def run(app_id, args):
    help_arg = len(args) > 0 and args[0] == "help"
    if help_arg:
        args[0] = "help"

    command = [find_java(), "-jar", find_jar()]
    command.extend(args)

    env = os.environ.copy()
    env["KM_NO_SCHEDULER"] = "true"

    if not help_arg:
        env["KM_API"] = api_url(app_id)

    # Workaround for FRAMEWORK-544
    # the java program does not like it if tputs doesn't work,
    # and tputs requires that $TERM is set
    if not 'TERM' in env:
        env['TERM'] = 'dumb'

    process = subprocess.Popen(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    stdout, stderr = process.communicate()
    print(stdout.decode("utf-8"), end="")
    print(stderr.decode("utf-8"), end="", file=sys.stderr)

    return process.returncode

def _cli_config_schema():
    """
    :returns: schema for kafka cli config
    :rtype: dict
    """
    return pkg_resources.resource_string(
            'dcos_kafka',
            'data/config-schema/kafka.json').decode('utf-8')

class CliError(Exception):
    pass


def main():
    args = sys.argv[2:]  # remove dcos-kafka & kafka
    if len(args) == 1 and args[0] == "--info":
        print("Start and manage Kafka brokers")
        return 0

    if len(args) == 1 and args[0] == "--version":
        print(constants.version)
        return 0

    if len(args) == 1 and args[0] == "--config-schema":
        print(_cli_config_schema())
        return 0

    if "--help" in args or "-h" in args:
        if "--help" in args:
            args.remove("--help")

        if "-h" in args:
            args.remove("-h")

        args.insert(0, "help")

    app_id = "kafka"
    if "--app-id" in args:
        idx = args.index("--app-id")
        app_id = args[idx + 1]
        del args[idx:idx+2]

    try:
        return run(app_id, args)
    except CliError as e:
        print("Error: " + str(e), file=sys.stderr)
        return 1
