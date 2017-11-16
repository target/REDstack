import os
import subprocess
from threading import Thread

from domain.cluster import Cluster
from domain.deploy import Deploy
from environment import Environment
from helper_functions import *
from openstack import Openstack
from redstack.exceptions import ChefException, ShellException

logger = logging.getLogger("root_logger")


class Chef:

    def __init__(self, deploy):
        # type: (Deploy) -> None
        """
        Constructor for Chef
        :param deploy: the cluster deploy object
        """
        # the cluster object with the list of nodes and how to connect to them
        self.deploy = deploy

        # standardized sleep time
        self.sleep_time = 5

        # knife command used to run chef on the nodes
        self.knife_command = "knife solo cook -i {0} {1}@{2} runlists/{3} --no-berkshelf --ssh-keepalive-interval 30"

        # user to track thread exceptions
        self.thread_exception = False

    def converge(self, runlist=None, nodes=None):
        # type: (str, []) -> None
        """
        The main function for converging chef on a cluster or set of nodes
        :param runlist: A string that can be found in the template 
        :param nodes: The nodes to run chef upon
        :return: 
        """

        if not runlist:
            # We have not recieved a custom specified runlist, run the default roles
            self._create_runtime_recipe()
            self._converge_default()
        else:
            self._converge_custom(runlist, nodes)

    def _create_runtime_recipe(self):
        # type: () -> None
        """
        Creates a file to be used to inject attributes at runtime for a knife solo cook run
        """

        recipe_location = '{0}/cookbooks/redstack/recipes/runtime.rb'.format(self.deploy.directory)
        attribute_hash = {
            'redstack': {
                'version': self.deploy.redstack_version,
                'cluster': self.deploy.cluster.get_hosts_list()
            },
            'ambari': {
                'repo_version': self.deploy.ambari_version,
                'release_version': '1.0'
            },
            'repo_version': self.deploy.hdp_version,
            'util_repo_version': self.deploy.hdp_utils_version,
            'kerberos_realm': self.deploy.kerberos_realm,
            'kerberos_password': self.deploy.kerberos_password,
            'domain': self.deploy.fqdn_address,
            'master_node': self.deploy.cluster.master_node.fqdn,
            'volume_device': self.deploy.volume_device,
            'mount_location': self.deploy.mount_location,
            'ambari_mysql_password': self.deploy.ambari_db_password
        }
        flat_hash = []
        self._flatten_dict(attribute_hash, rv=flat_hash)

        with open(recipe_location, 'w') as runtime_file:
            runtime_file.writelines(['node.force_override' + line + '\n' for line in flat_hash])

        logger.info('Created dynamic recipe at {0}'.format(recipe_location))

    def _flatten_dict(self, v, prefix='', rv=list()):
        # type: ({} or object, str, []) -> None
        """
        Recursive function that takes a dictionary and flattens it into a list for chef attributes
        :param v: The value to check if it's another dict
        :param prefix: The prefix for the values when appending
        :param rv: The return value for the end of the recursize function call
        """
        if isinstance(v, dict):
            for k, v2 in v.items():
                p2 = "{}['{}']".format(prefix, k)
                self._flatten_dict(v2, p2, rv)
        else:
            rv.append('{} = {}'.format(prefix, repr(str(v))))

    def _converge_default(self):
        # type: () -> None
        """
        Opens a thread on each server in the cluster, and calls converge_node on each one, blocks until the threads
        are finished
        """

        nodes = self.deploy.cluster.nodes
        threads = []

        for node in nodes:
            t = Thread(target=self._converge_node, args=[node.role + '.json', node, True, True])
            threads.append(t)

        for thread in threads:
            thread.start()

        threads_alive = len(threads)
        while threads_alive > 0:
            time.sleep(10)
            threads_alive = 0
            for thread in threads:
                if thread.isAlive():
                    threads_alive += 1
            logger.info('Still executing Chef: {0} nodes remaining...'.format(threads_alive))
            if self.thread_exception:
                time.sleep(1)
                os._exit(1)

        logger.info('Nodes successfully converged')
        
    def _converge_custom(self, runlist, nodes):
        # type: (str, []) -> None
        """
        Opens a thread on each server in the cluster, and calls converge_node on each one, blocks until the threads
        are finished
        :param runlist: The cheflist string that maps to an item in the redstack template
        :param nodes: The nodes to run the runlist
        """

        threads = []

        for node in nodes:
            t = Thread(target=self._converge_node, args=[runlist, node, False, False])
            threads.append(t)

        for thread in threads:
            thread.start()

        threads_alive = len(threads)
        while threads_alive > 0:
            time.sleep(10)
            threads_alive = 0
            for thread in threads:
                if thread.isAlive():
                    threads_alive += 1
            logger.info('Still executing Chef: {0} nodes remaining...'.format(threads_alive))
            if self.thread_exception:
                time.sleep(1)
                os._exit(1)

        logger.info('Nodes successfully converged')

    def _converge_node(self, runlist, node, install_chef=False, reformat_on_failure=False):
        # type: (str, Node, bool, bool) -> None
        """
        Installs chef 12.12 on the node
        :param runlist: the runlist to execute on the node
        :param node: the Node we want to install chef on
        :param install_chef: whether or not to install_chef on the node
        :param reformat_on_failure: if the node fails, reformat the drive and rebuild the node
        """
        try:
            test_node_ssh_availability(node, self.deploy.cluster.ssh_user, self.deploy.cluster.private_key)

            knife_command = self.knife_command.format(self.deploy.cluster.private_key,
                                                      self.deploy.cluster.ssh_user,
                                                      node.floating_ip, runlist)

            tries_left = self.deploy.chef_tries
            while True:
                tries_left -= 1

                if install_chef:
                    self._install_chef(node)

                logger.info("Executing runlist {0} on {1} for deployment {2}".format(runlist, node.name, self.deploy.name))
                logger.info(knife_command)

                process = subprocess.Popen(knife_command, cwd=self.deploy.directory, shell=True,
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                with open('{0}/logs/{1}-converge'.format(self.deploy.directory, node.name), 'w') as log_file:
                    log_file.write('\n <<<<< Converging {0} - {1} >>>>> \n'.format(node.name, runlist))

                    while True:
                        nextline = process.stdout.readline()
                        if nextline == '' and process.poll() is not None:
                            break
                        log_file.write(nextline)
                        if node.primary and self.deploy.log_chef_to_stdout:
                            logger.warning('CHEF: {0}'.format(nextline.strip('\n')))
                        process.stdout.flush()

                    if process.returncode == 0:
                        logger.info("Runlist {0} succeeded on {1} for {2} - {3}".format(runlist, node.name,
                                                                                        self.deploy.name,
                                                                                        node.floating_ip))
                        return
                    else:
                        logger.warning("Runlist {0} failed on {1} for {2} - {3}".format(runlist, node.name,
                                                                                        self.deploy.name,
                                                                                        node.floating_ip))
                        while True:
                            nextline = process.stderr.readline()
                            if nextline == '' and process.poll() is not None:
                                break
                            log_file.write(nextline)

                            logger.error('CHEF-ERROR: {0}'.format(nextline.strip('\n')))
                            process.stderr.flush()

                    if tries_left == 0:
                        raise ChefException("Runlist {0} failed on {1} for {2} - {3}".format(
                            runlist, node.name, self.deploy.name, node.floating_ip))
                    elif reformat_on_failure:
                        logger.warning("Reformatting and rebuilding {0}".format(node.name))
                        self._rebuild_and_reformat(node)

                logger.warning("Chef failed on {0}, retrying {1} more times".format(node.name, tries_left))

        except:
            self.thread_exception = True
            raise

    def _install_chef(self, node):
        # type: (Node) -> None
        """
        Installs chef 12.12 on the node
        :param node: the Node we want to install chef on
        """

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(node.floating_ip, username=self.deploy.cluster.ssh_user,
                    key_filename=self.deploy.cluster.private_key, timeout=30)

        with open('{0}/logs/{1}-chefinstall.log'.format(self.deploy.directory, node.name), 'a') as log_file:
            stdin, stdout, stderr = ssh.exec_command('curl {0} > /tmp/chef.rpm; rpm -qa | grep chef || sudo rpm '
                                                     '-i /tmp/chef.rpm'.format(self.deploy.chef_rpm_uri), get_pty=True)
            stdout_str = stdout.read()
            stderr_str = stderr.read()
            log_file.write(stdout_str)
            log_file.write(stderr_str)

            if stdout.channel.recv_exit_status() == 0:
                if node.primary and self.deploy.log_chef_to_stdout:
                    [logger.info(line) for line in stdout_str.split('\n')]
                logger.info('Chef installed on ' + node.name)
            else:
                [logger.error(line) for line in stdout_str.split('\n')]
                [logger.error(line) for line in stderr_str.split('\n')]
                raise ShellException("Chef failed to install on node: {0} : {1}".format(node.name, stderr_str))

        ssh.close()

    def _rebuild_and_reformat(self, node):
        # type: (Node) -> None
        """
        Reformats the node and rebuilds it in Openstack
        :param node: The node to rebuild and reformat
        """
        Openstack.rebuild_node(self.deploy, node)
        unmount(node, self.deploy.cluster.ssh_user, self.deploy.cluster.private_key)

if __name__ == '__main__':
    setup_logger()
    args = parse_args()

    cluster = Cluster(json_file=args.cluster)
    deploy = Deploy(config_file=args.config, cluster=cluster)

    environment = Environment(deploy)
    environment.create()

    chef = Chef(deploy)
    chef.converge()
