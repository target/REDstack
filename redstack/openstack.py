import json
import os
from threading import Thread

from cinderclient import client as cinderclient
from glanceclient import Client as GlanceClient
from heatclient import client as heatclient
from keystoneauth1 import session, \
    exceptions as keystoneauth1_exceptions
from keystoneauth1.identity import v3, v2
from neutronclient.v2_0 import client as neutronclient
from novaclient import client as novaclient
from novaclient.v2.servers import Server

from domain.deploy import Deploy
from environment import Environment
from heat_template import HeatTemplate
from helper_functions import *
from redstack.exceptions import ConfigException, RebuildException, BasicOpenstackNetworkingException, \
    ExistingNonRedstackResourcesException, HeatException

logger = logging.getLogger("root_logger")


class Openstack:

    @staticmethod
    def rebuild_node(deploy, node):
        # type (redstack.domain.Deploy, redstack.domain.Node, int, int) -> bool
        """ 
        Rebuild an openstack node with Nova API binding.

        Use nova API to rebuild an Openstack node. Return bool indicating result of rebuild. The rebuild will
        be retried x amount of times before indicating total failure by returning False.

        :param deploy: Deploy object with deployment details
        :param node: Node object representing server to rebuild
        :raises: RebuildException if the node fails to rebuild correctly
        """
        sess = Openstack.create_ost_auth_session(deploy)
        nova = novaclient.Client("2", session=sess, region_name=deploy.region)
        glance = GlanceClient("2", session=sess, region_name=deploy.region)
        server = nova.servers.get(node.server_id)

        images = retry(glance.images.list, 5, (keystoneauth1_exceptions.connection.ConnectFailure, IndexError))

        image_id = None
        for image in images:
            if deploy.image_name == image.name:
                image_id = image.id

        if image_id:
            retry(server.rebuild, 5, (keystoneauth1_exceptions.connection.ConnectFailure, IndexError), image_id)
        else:
            raise ConfigException("Could not find image {0} to rebuild with".format(deploy.image_name))

        server = nova.servers.get(node.server_id)

        start = time.time()
        while server.status != 'REBUILD':
            server = nova.servers.get(node.server_id)

            # Fail if the node never responds
            if time.time() - start > 480:
                raise RebuildException("Instance failed to respond to request for rebuild")

        while server.status not in ["ACTIVE", "ERROR"]:
            server = nova.servers.get(node.server_id)

        if server.status == 'ACTIVE':
            return
        else:
            raise RebuildException("Instance fell into ERROR state after rebuild")

    @staticmethod
    def create_ost_auth_session(deploy):
        # type: (Deploy) -> session.Session
        """ Create a keystoneauth Session object using keystone v3 used to create Openstack API clients.

        :return: Session used to authenticate with various Openstack API clients.
        """
        if deploy.auth_version == 2:
            auth = v2.Password(auth_url=deploy.openstack_auth_url,
                               username=deploy.ost_username,
                               password=deploy.ost_password,
                               tenant_name=deploy.ost_project_name)
        elif deploy.auth_version == 3:
            if deploy.ost_project_id:
                auth = v3.Password(auth_url=deploy.openstack_auth_url,
                                   username=deploy.ost_username,
                                   password=deploy.ost_password,
                                   project_id=deploy.ost_project_id)
            else:
                auth = v3.Password(auth_url=deploy.openstack_auth_url,
                                   username=deploy.ost_username,
                                   password=deploy.ost_password,
                                   project_name=deploy.ost_project_name,
                                   user_domain_name=deploy.ost_domain,
                                   project_domain_name=deploy.ost_domain)
        else:
            raise ConfigException("auth_version must be in [2,3]")

        return session.Session(auth=auth, verify=deploy.cacert)

    def __init__(self, deploy):
        # type: (Deploy) -> None
        """
        Constructor for Openstack
        :param deploy: 
        """
        self.deploy = deploy

        # Create auth session fail if no auth
        self.ost_auth_session = Openstack.create_ost_auth_session(deploy)

        self.cinder = cinderclient.Client("2", session=self.ost_auth_session, region_name=deploy.region)
        self.neutron = neutronclient.Client(session=self.ost_auth_session, region_name=deploy.region)
        self.nova = novaclient.Client("2", session=self.ost_auth_session, region_name=deploy.region)
        self.heat = heatclient.Client("1", session=self.ost_auth_session, region_name=deploy.region)

        # Retries for certain features
        self.retries = 5

        # Sleep backoff for certain features
        self.sleep = 30
        self.short_sleep = 5

        # run retries on these exceptions, as a tuple
        self.retry_exceptions = (keystoneauth1_exceptions.connection.ConnectFailure, IndexError)

        self.thread_exception = False

    def build(self):
        # type: () -> None
        """ 
        Build a fresh cluster in Openstack.

        This method coordinates the building of an Openstack stack with new resources. Existing REDstack resources are
        deleted (non redstack resources on project trigger exception). Private
        key is created. And finally stack is built using Heat API.

        :return: Cluster object associated with this deployment
        """
        # Create private key file (currently in openstack utilities) and write it to deploy directory
        if self.deploy.key_name:
            self.deploy.cluster.private_key = os.path.join(self.deploy.directory, self.deploy.key_name)
        else:
            self.deploy.cluster.private_key = self._create_private_key()

        logger.info("Starting Openstack build using fresh resources.")

        # Clean existing resources, raise error if resources remain after cleaning
        self._cleanup_existing_resources()

        # Exception raised if basic networking not enabled on the cluster
        heat_template = HeatTemplate(self.deploy)
        if self._use_existing_network():

            # Add an external gateway if it doesn't exist
            router = self._get_routers()[0]
            self.neutron.add_gateway_router(router["id"], {"network_id": self.deploy.external_network_id})

            # Gather subnet and network information
            subnet = self._get_subnets()[0]
            subnet_id = str(subnet["id"])
            private_network_id = str(subnet["network_id"])

            # Generate heat template
            heat_template.generate_with_existing_network(subnet_id, private_network_id)
        else:
            # Generate heat template
            heat_template.generate()

        # Attempt to create stack with heat template
        self._build_stack_from_template()

        # Get node information and create list of Node objects
        retry(self._populate_node_object_list, self.retries, self.retry_exceptions)

    def rebuild(self):
        # type: () -> None
        """ Rebuild an exisiting stack in Openstack.

        This method coordinates the rebuild of an existing REDstack stack in Openstack. Each server is rebuilt using
        Nova API, and attached volumes are reformatted and attached back to Nodes.

        :return: Cluster object associated with this deployment
        """
        servers = self._get_servers()

        if not self.deploy.key_name or not self.deploy.cluster.private_key:
            raise ConfigException('In order to rebuild an existing openstack cluster you must specify a key_name and '
                                  'a path to the key location')

        logger.info("Starting Openstack rebuild with existing resources.")

        rebuild_threads = []
        for server in servers:
            rebuild_threads.append(Thread(target=self._rebuild_server, args=[server]))
        for thread in rebuild_threads:
            thread.start()

        # wait for threads to finish
        thread_alive = True
        while thread_alive:
            thread_alive = False
            for thread in rebuild_threads:
                if thread.isAlive():
                    thread_alive = True
            if self.thread_exception:
                time.sleep(1)
                os._exit(1)

        # Get node information and create list of Node objects
        retry(self._populate_node_object_list, self.retries, self.retry_exceptions)
        self.deploy.cluster.private_key = os.path.join(self.deploy.directory, self.deploy.key_name)

    def _rebuild_server(self, server):
        # type: (Server) -> None
        """ 
        Rebuild a single Openstack server
        Server rebuild has two parts, a rebuild using Nova API and a volume reformat using chef.
        :param: server - Openstack server to rebuild
        """
        try:
            node = self.deploy.cluster.get_node(server.name)
            node.server_id = server.id
            node.floating_ip = server.networks.values()[0][1]

            Openstack.rebuild_node(self.deploy, node)
            logger.info("Successfully rebuilt {0}".format(node.name))

            unmount(node, self.deploy.cluster.ssh_user, self.deploy.cluster.private_key)
        except:
            self.thread_exception = True
            raise

    def _use_existing_network(self):
        # type: () -> bool
        """ 
        Check if an openstack project has an existing network
        :return: Whether or not the project has an existing network we can use
        """
        router_list = self._get_routers()
        subnet_list = self._get_subnets()
        network_list = self._get_networks()

        if len(router_list) == 1 and len(subnet_list) == 1 and len(network_list) == 2 \
                and self.deploy.try_existing_network:
            return True  # Network is probably already configured
        else:
            return False

    def _build_stack_from_template(self):
        # type: () -> None
        """ 
        Create a stack in Openstack using Heat API and generated Heat template.

        The Heat template is created using a template file and heat_template module. Once this template is created,
        an attempt is made to build the stack using Heat API. Return true if the stack build is successful, otherwise
        false is returned.

        :return: True if creation succeeded, False if it failed and should be retried on next Vlan
        """
        template = open(os.path.join(self.deploy.directory, "template.yml"), "r")

        logger.info("Starting stack build process.")

        stack = self.heat.stacks.create(stack_name=self.deploy.stack_name, template=template.read(), parameters={})
        uid = stack["stack"]["id"]
        stack = self.heat.stacks.get(stack_id=uid).to_dict()

        while stack["stack_status"] == "CREATE_IN_PROGRESS":
            logger.info("Stack build in progress. {0} servers built".format(len(self._get_servers())))
            time.sleep(self.sleep)
            stack = self.heat.stacks.get(stack_id=uid).to_dict()

        if stack["stack_status"] == "CREATE_COMPLETE":
            logger.info("Stack build complete.")
            return
        elif stack["stack_status"] == "CREATE_FAILED":
            logger.error("Reason for stack build failure: {0}".format(stack["stack_status_reason"]))

            if len(self._get_heat_stacks()) > 0:
                self._cleanup_existing_resources()

            raise HeatException(stack["stack_status_reason"])

    def _cleanup_existing_resources(self):
        # type: () -> None
        """ 
        Initiate REDstack resource deletion if any exist.

        If existing REDstack resources exist on the project when called, they will be scheduled for immediate deletion.
        If non-redstack resources are found to exist on the project, an ExistingNonRedstackResourcesException will
        be thrown indicating the problem that stops the deploy.

        :return: None
        """
        stack_list = self._get_heat_stacks()

        if len(stack_list) > 1:
            raise ExistingNonRedstackResourcesException("Non-Redstack resources exist on this project.")
        elif len(stack_list) == 1 and stack_list[0].stack_name.lower() == self.deploy.stack_name:
            logger.info("Found existing REDstack resources, scheduling them for deletion")

            # Allow multiple stack delete attempts
            retry(self._destroy_existing_resources, 3, HeatException, stack_list[0].id)

        # If any resources remain... raise error
        for i in range(self.retries):
            if len(self._get_floating_ips()) == 0 \
                    and len(self._get_servers()) == 0 \
                    and len(self._get_cinder_volumes()) == 0:
                break
            else:
                if i == self.retries - 1:
                    raise ExistingNonRedstackResourcesException("Non-Redstack resources exist on this project. \n"
                                                                "Floating IPs: {0}\n"
                                                                "Servers: {1}\n"
                                                                "Cinder Volumes: {2}".format(self._get_floating_ips(),
                                                                                             self._get_servers(),
                                                                                             self._get_cinder_volumes()))
                time.sleep(self.short_sleep)

        logger.info("Openstack project clear of all resources, ready to build.")

    def _destroy_existing_resources(self, stack_id):
        # type: (str) -> None
        """ 
        Perform the actual API calls to destroy the specified stack

        The Heat API is used to initiate a delete process of the heat stack for the project. If the delete is
        successful, true is returned. Otherwise, false.

        :param: The id of the stack to delete
        :raises HeatException: if the stack fails to delete
        """
        # Delete floating ips to speed up teardown
        logger.info("Starting floating IP delete in parallel.")
        self._delete_floating_ip_list(self._get_floating_ips())

        logger.info("Starting deletion of remaining stack resources.")
        retry(self.heat.stacks.delete, self.retries, Exception, stack_id)

        stack = self.heat.stacks.get(stack_id=stack_id).to_dict()
        while stack["stack_status"] == "DELETE_IN_PROGRESS":
            logger.info("Stack delete in progress. {0} servers remaining.".format(len(self._get_servers())))
            time.sleep(self.sleep)
            stack = self.heat.stacks.get(stack_id=stack_id).to_dict()

        if stack["stack_status"] == "DELETE_COMPLETE":
            logger.info("Successfully deleted stack.")
            return
        else:
            logger.error("Stack status: {0}".format(stack["stack_status"]))
            raise HeatException("Reason for stack status: {0}".format(stack["stack_status_reason"]))

    def _delete_floating_ip_list(self, ip_list):
        # type: ([{}]) -> None
        """ 
        Delete each floating ip from param list in paralell.

        Start a thread for each floating IP that needs to be deleted. Wait until all threads return before
        returning to caller.

        :param ip_list: List of IPs to delete
        :return: None
        """
        delete_threads = []
        for ip in ip_list:
            delete_threads.append(Thread(target=self._delete_floating_ip, args=[ip]))
        for job in delete_threads:
            job.start()

        thread_alive = True
        while thread_alive:
            thread_alive = False
            for thread in delete_threads:
                if thread.isAlive():
                    thread_alive = True
            if self.thread_exception:
                time.sleep(1)
                os._exit(1)

    def _delete_floating_ip(self, ip):
        # type: ({}) -> None
        """ 
        Perform the delete of a floating ip.

        Use Neutron API to delete the floating ip with an id equal to that of the method param. Retry x amount of
        times if connection issues occur.

        :param ip: IP object to delete
        :return: None
        """
        try:
            retry(self.neutron.delete_floatingip, self.retries, self.retry_exceptions, ip['id'])
        except neutronclient.exceptions.NotFound:
            logger.warning("Neutron claims the {0} wasn't found, "
                           "probably because it's already deleted".format(ip['id']))
        except:
            self.thread_exception = True

        logger.info('Floating IP {0} deleted'.format(ip['floating_ip_address']))

    def _populate_node_object_list(self):
        # type: () -> None
        """ 
        Generate a list of domain.node.Node objects associated with the Deployment.

        Each object in the list represents a node in the Project that has been created.

        :return: [] of domain.node.Node objects.
        """
        server_list = self._get_servers()
        for server in server_list:
            node = self.deploy.cluster.get_node(server.name)
            node.ram = self.nova.flavors.get(server.flavor["id"]).ram
            node.server_id = server.id
            node.internal_ip = server.networks.values()[0][0]
            node.floating_ip = server.networks.values()[0][1]

    def _create_private_key(self):
        # type: () -> str
        """ 
        Create private key that will be added to each Ost node created for project.

        Creates the key and writes it to the deployment directory.

        :return: absolute path to key file
        """
        self.deploy.key_name = self.deploy.name
        private_key = self.nova.keypairs.create(self.deploy.name).private_key

        key_path = os.path.join(self.deploy.directory, self.deploy.name)
        with open(key_path, "w") as key_file:
            key_file.write(private_key)
            os.chmod(key_path, 0o400)

        logger.info("Created new Private Key file for Openstack deployment")
        return key_path

    def _get_servers(self):
        # type: () -> []
        """ 
        Return list of Server objects that belong the the Openstack project.
        :return: List of Server objects belonging to Openstack project.
        """
        servers = retry(self.nova.servers.list, self.retries, self.retry_exceptions)
        return servers

    def _get_networks(self):
        # type: () -> [{}]
        """ 
        Return list of Network objects associated with Openstack project.
        :return:
        """
        raw_networks = retry(self.neutron.list_networks, self.retries, self.retry_exceptions)
        return raw_networks['networks']

    def _get_subnets(self):
        # type: () -> [{}]
        """ 
        Return list of Subnets associated with Openstack project.
        :return: list of subnets
        """
        raw_subnets = retry(self.neutron.list_subnets, self.retries, self.retry_exceptions)
        return raw_subnets['subnets']

    def _get_floating_ips(self):
        # type: () -> [{}]
        """ 
        Return list of Floating IPs belonging to this Openstack project.
        :return: list of floating ips
        """
        raw_floatingips = retry(self.neutron.list_floatingips, self.retries, self.retry_exceptions)
        return raw_floatingips["floatingips"]

    def _get_routers(self):
        # type: () -> [{}]
        """ 
        Return list of Routers associated with this Openstack project.
        :return: list of routers
        """
        routers = retry(self.neutron.list_routers, self.retries, self.retry_exceptions)
        return routers["routers"]

    def _get_heat_stacks(self):
        # type: () -> []
        """ 
        Return list of heat templates existing for this Openstack project.
        :return: list of heat stacks
        """
        raw_stacks = retry(self.heat.stacks.list, self.retries, self.retry_exceptions)
        return list(raw_stacks)

    def _get_cinder_volumes(self):
        # type: () -> []
        """ 
        Return list of cinder volumes existing in this Openstack project.
        :return: list of cinder volumes
        """
        volumes = retry(self.cinder.volumes.list, self.retries, self.retry_exceptions)
        return volumes


if __name__ == "__main__":
    setup_logger()
    args = parse_args()

    deploy = Deploy(config_file=args.config)

    environment = Environment(deploy)
    environment.create()

    ost = Openstack(deploy)
    ost.build()
