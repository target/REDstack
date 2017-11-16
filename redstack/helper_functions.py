import argparse
import logging
import socket
import time

import paramiko
from paramiko.ssh_exception import NoValidConnectionsError, AuthenticationException, SSHException

from domain.node import Node
from exceptions import *

from redstack.exceptions import ShellException

logger = logging.getLogger("root_logger")


def parse_args():
    # type: () -> argparse.Namespace
    """ 
    Parses command line arguments.
    :return: Namespace object containing command line arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--config", help="The absolute path to your rs-conf.yml configuration file",
                        default="/opt/redstack/REDstack/conf/rs-conf.yml",
                        required=False)

    parser.add_argument("--cluster", help="The absolute path to a cluster json object",
                        default="/opt/redstack/REDstack/cluster.json",
                        required=False)

    return parser.parse_args()


def setup_logger():
    # type: () -> None
    """
    Sets up the logger with formatting for redstack
    :return: 
    """

    # Configure Logger object for process
    formatter = logging.Formatter('%(asctime)s: %(message)s', '%Y-%m-%d %H:%M:%S')

    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger("root_logger")
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


def retry(func, tries, exceptions, *args, **kwargs):
    # type: (any, int, tuple or Exception, args) -> any
    """ 
    Helper function for attempting to execute more than once
    :param func: A method to execute
    :param tries: Number of times to retry
    :param exceptions: Exceptions to retry when caught
    :param args: args to pass to the function
    """
    for i in range(tries):
        try:
            return func(*args, **kwargs)
        except exceptions as e:
            if i == tries - 1:
                raise
            logger.info('{0} failed on try {1} with {2}'.format(func.__name__, i, e.message))
            time.sleep(5)


def unmount(node, ssh_user, private_key):
    # type: (Node, str, str) -> None
    """
    Reformats a drive with paramiko
    :param node: A node to reformat the drive on
    :param ssh_user: The user to ssh with
    :param private_key: The key to ssh with
    :return: 
    """
    test_node_ssh_availability(node, ssh_user, private_key)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    retry(ssh.connect, 5, (socket.error, SSHException, AuthenticationException, NoValidConnectionsError),
          node.floating_ip, username=ssh_user, key_filename=private_key, timeout=30)
    stdin, stdout, stderr = ssh.exec_command('if df -h | grep /grid/0; then sudo umount -f -l /grid/0; fi;',
                                             get_pty=True)

    if stdout.channel.recv_exit_status() == 0:
        ssh.close()
        logger.info('Reformat succeeded on ' + node.name)
    else:
        stderr_str = stderr.read()
        logger.info(stderr_str)
        ssh.close()
        raise ShellException("Reformat failed on node: " + node.name)


def set_root_mysql_password(node, ssh_user, private_key, new_password):
    # type: (Node, str, str) -> None
    """
    Sets the mysql root password
    :param node: A node to reformat the drive on
    :param ssh_user: The user to ssh with
    :param private_key: The key to ssh with
    :return: 
    """
    test_node_ssh_availability(node, ssh_user, private_key)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    retry(ssh.connect, 5, (socket.error, SSHException, AuthenticationException, NoValidConnectionsError),
          node.floating_ip, username=ssh_user, key_filename=private_key, timeout=30)
    stdin, stdout, stderr = ssh.exec_command('mysqladmin -u root password {0}'.format(new_password), get_pty=True)

    if stdout.channel.recv_exit_status() == 0:
        ssh.close()
        logger.info('Set mysql password for root mysql user ' + node.name)
    else:
        stderr_str = stderr.read()
        logger.info(stderr_str)
        ssh.close()
        raise ShellException("Failed to change mysql password for root user: " + node.name)


def test_node_ssh_availability(node, ssh_user, private_key, retries=50):
    # type: (Node, str, str) -> None
    """
    Attempts to connect to the given ip over ssh
    :param node: A node to reformat the drive on
    :param ssh_user: The user to ssh with
    :param private_key: The key to ssh with
    :param retries: How many time to try and connect
    :raises paramiko.SSHException: when the node cannot be reached
    """

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    for attempt in range(retries):
        try:
            ssh.connect(node.floating_ip, username=ssh_user, key_filename=private_key, timeout=5)
            ssh.close()
            logger.info("Established SSH connection with {0} -- {1}@{2}".format(node.name, ssh_user, node.floating_ip))
            return
        except (socket.error, SSHException, AuthenticationException, NoValidConnectionsError) as e:
            logger.debug('Socket error while checking SSH availability on {0} - {1}'.format(node.name, e))
            time.sleep(5)

    logger.error('Timed out attempting to establish SSH connection with {0}'.format(node.name))
    raise paramiko.SSHException("Exhausted retries connecting to node")
