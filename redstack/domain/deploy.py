import logging
import os
import time

import yaml

from cluster import Cluster

logger = logging.getLogger('root_logger')


class Deploy:

    def __init__(self, config_file=None, cluster=None):
        # type: (str, Cluster) -> None
        """
        Constructor for Deploy
        :param config_file: location of the rs-conf.yml file
        :param cluster: A cluster object to initialize with
        """

        # Read main configuration file
        with open(config_file, 'r') as config_yaml_file:
            config_dict = yaml.load(config_yaml_file)

        self.redstack_version = config_dict['redstack_version']
        self.directory_base = config_dict['deployment_directory_base']
        self.installation_directory = config_dict['installation_directory']
        self.cookbook_directory = config_dict['cookbook_directory']

        self.log_path = config_dict['log_path']
        self.log_level = config_dict['log_level']

        self.stack_name = config_dict['stack_name']

        self.auth_version = config_dict['auth_version']
        self.image_name = config_dict['image_name']
        self.availability_zone = config_dict['availability_zone']
        self.region = config_dict['region']

        self.openstack_auth_url = config_dict['openstack_auth_url']
        self.external_network_id = config_dict['external_network_id']

        self.try_existing_network = config_dict['try_existing_network']
        self.subnet_cidr = config_dict['subnet_cidr']
        self.expose_ui_ssh = config_dict['expose_ui_ssh']
        self.subnet_dns_nameservers = config_dict['subnet_dns_nameservers']

        self.cacert = config_dict['cacert']

        self.ost_username = config_dict['ost_username']
        self.ost_password = config_dict['ost_password']
        self.ost_project_id = config_dict['ost_project_id']
        self.ost_project_name = config_dict['ost_project_name']
        self.ost_domain = config_dict['ost_domain']

        self.use_existing_openstack = config_dict['use_existing_openstack']

        self.key_name = config_dict['key_name']

        self.stack_type = config_dict['stack_type']

        self.template_name = config_dict['template_file']

        self.hdp_major_version = config_dict['hdp_major_version']
        self.hdp_version = config_dict['hdp_version']
        self.hdp_utils_version = config_dict['hdp_utils']
        self.define_custom_repos = config_dict['define_custom_repos']

        self.ambari_version = config_dict['ambari_version']
        self.ambari_password = config_dict['ambari_password']

        self.fqdn_address = config_dict['fqdn_address']
        self.kerberos_realm = config_dict['kerberos_realm']
        self.kerberos_password = config_dict['kerberos_password']

        self.volume_device = config_dict['volume_device']
        self.mount_location = config_dict['mount_location']

        self.chef_rpm_uri = config_dict['chef_rpm_uri']
        self.chef_version = config_dict['chef_version']
        self.chef_tries = config_dict['chef_tries']
        self.log_chef_to_stdout = config_dict['log_chef_to_stdout']

        self.ambari_db_password = config_dict['ambari_db_password']
        self.mysql_root_password = config_dict['mysql_root_password']

        # To be set when the blueprints are created
        self.blueprint = None
        self.host_mapping = None
        self.stack_definition = None
        self.utils_definition = None

        # Set the deploy name and directory based on the current time
        self.name = "{0}-{1}".format(config_dict["cluster_name"], str(int(time.time())))
        self.directory = os.path.join(config_dict['deployment_directory_base'], self.name)

        # Initialize the cluster object based on whether or not a cluster json file was passed
        if not cluster:
            # Read cluster template file
            template_file = '{0}/conf/templates/{1}'.format(config_dict['installation_directory'],
                                                            config_dict['template_file'])
            self.cluster = Cluster(
                cluster_name=config_dict['cluster_name'],
                ssh_user=config_dict['ssh_user'],
                private_key=config_dict['existing_key_location'],
                key_name=config_dict['key_name'],
                template_file=template_file,
                fqdn_address=self.fqdn_address
            )
        else:
            self.cluster = cluster
