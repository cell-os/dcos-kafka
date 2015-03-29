"""DCOS Kafka Example Subcommand

Usage:
    dcos kafka info

Options:
    --help           Show this screen
    --version        Show version
"""
import docopt
from dcos_kafka import constants


def main():
    args = docopt.docopt(
        __doc__,
        version='dcos-marathon version {}'.format(constants.version))

    if args['kafka'] and args['info']:
        print('Example of a DCOS subcommand')
    else:
        print(__doc__)
        return 1

    return 0
