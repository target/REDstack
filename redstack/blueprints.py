import json
import os

from helper_functions import *
from redstack.domain.deploy import Deploy


class BlueprintBuilder:
    def __init__(self, deploy):
        # type: (Deploy) -> None
        """
        Constructor for BluePrintBuilder
        :param deploy: The current deploy object
        """
        self.deploy = deploy

        self.configurations = {}
        self.host_groups = {}

        self.stack_type = 'HDP'

        self.blueprint_directory = os.path.join(self.deploy.installation_directory,
                                                'conf', 'blueprints', self.stack_type)

    def create_all(self):
        # type: () -> None
        """
        The main method to build all of the blueprints and apply them to the deploy object
        :return: 
        """
        self.deploy.blueprint = self._build_blueprint()
        self.deploy.host_mapping = self._create_host_mapping()
        self.deploy.stack_definition = self._create_stack_definition()
        self.deploy.utils_definition = self._create_utils_definition()

    def _create_stack_definition(self):
        # type: () -> {}
        """
        create the stack definition dict from the json
        :return: 
        """
        stack_definition_file = os.path.join(self.blueprint_directory, self.deploy.hdp_version, 'hdp-stack.json')

        with open(stack_definition_file, 'r') as json_file:
            return json.load(json_file)

    def _create_utils_definition(self):
        # type: () -> {}
        """
        create the stack definition utils dict from the json file
        :return: 
        """
        utils_definition_file = os.path.join(self.blueprint_directory, self.deploy.hdp_version, 'hdp-utils.json')

        with open(utils_definition_file, 'r') as json_file:
            return json.load(json_file)

    def _build_blueprint(self):
        # type: () -> {}
        """
        Returns a dictionary mapping of the blueprints built from the blueprint config values for the current stack
        :return: 
        """
        self._build_configurations()
        self._build_host_groups()

        blueprint = {
            "configurations": [],
            "host_groups": [],
            "Blueprints": {
                "stack_name": self.stack_type,
                "stack_version": self.deploy.hdp_major_version
            }
        }

        for config in self.configurations:
            final_config = {
                config: {
                    'properties_attributes': self.configurations[config].properties_attributes,
                    'properties': self.configurations[config].properties
                }
            }
            blueprint['configurations'].append(final_config)

        for group in self.host_groups:
            new_group = {
                "components": self.host_groups[group].components,
                "configurations": self.host_groups[group].configurations,
                "name": self.host_groups[group].name,
                "cardinality": self.host_groups[group].cardinality
            }
            blueprint['host_groups'].append(new_group)

        blueprint['configurations'].append(self._create_kerberos_env())
        blueprint['configurations'].append(self._create_krb5_conf())

        return blueprint

    def _create_host_mapping(self):
        # type: () -> {}
        """
        Returns a dictionary of the hostmapping file
        :return: 
        """

        host_mapping = {
            "blueprint": "redstack",
            "default_password": "test",
            "host_groups": [],
            "credentials": [
                {
                    "alias": "kdc.admin.credential",
                    "principal": "admin/admin",
                    "key": self.deploy.kerberos_password,
                    "type": "temporary"
                }
            ],
            "security": {
                "type": "KERBEROS"
            },
            "Clusters": {
                "cluster_name": "redstack"
            }
        }

        groups = []
        [groups.append(node.ambari_group) for node in self.deploy.cluster.nodes if node.ambari_group not in groups]

        for group in groups:
            host_group = {
                "hosts": [],
                "name": group
            }

            for node in self.deploy.cluster.nodes:
                if node.ambari_group == group:
                    host_group['hosts'].append(
                        {
                            "fqdn": node.fqdn
                        }
                    )

            host_mapping['host_groups'].append(host_group)

        return host_mapping

    def _create_kerberos_env(self):
        # type: () -> {}
        """
        Returns a dictionary with the kerberos env
        :return: 
        """
        return {
            "kerberos-env": {
                "properties_attributes": {},
                "properties": {
                    "realm": self.deploy.kerberos_realm,
                    "kdc_type": "mit-kdc",
                    "kdc_host": self.deploy.cluster.master_node.fqdn,
                    "admin_server_host": self.deploy.cluster.master_node.fqdn,
                    "encryption_types": "aes des3-cbc-sha1 rc4 des-cbc-md5",
                    "executable_search_paths":
                        "/usr/bin, /usr/kerberos/bin, /usr/sbin, /usr/lib/mit/bin, /usr/lib/mit/sbin",
                    "ldap_url": "",
                    "container_dn": ""
                }
            }
        }

    def _create_krb5_conf(self):
        # type: () -> {}
        """
        Retirns a dictionary with the krb5-conf
        :return: 
        """
        return {
            "krb5-conf": {
                "properties_attributes": {},
                "properties": {
                    "domains": self.deploy.kerberos_realm,
                    "manage_krb5_conf": "false"
                }
            }
        }

    def _build_configurations(self):
        # type: () -> None
        """
        Create the configurations based on the text file
        :return: 
        """
        config_file = os.path.join(self.blueprint_directory, 'configurations', 'configurations.txt')

        with open(config_file, "r") as configs:
            for config in configs:
                config_path = os.path.join(self.blueprint_directory, 'configurations', config).rstrip('\n')

                with open(config_path) as json_file:
                    config_json = json.load(json_file)

                    # creates a new object for each individual configuration
                    config_object = config_json.keys()[0]
                    self.configurations[config_json.keys()[0]] = \
                        Configuration(config_json[config_object]['properties_attributes'],
                                      config_json[config_object]['properties'])

    def _build_host_groups(self):
        # type: () -> None
        """
        Create the host groups based on the text file
        :return: 
        """
        host_group_file = os.path.join(self.blueprint_directory, 'host_groups', 'host_groups.txt')

        with open(host_group_file, "r") as host_groups:
            for host_group in host_groups:

                # getting the names of files with each host group
                host_group_path = os.path.join(self.blueprint_directory, 'host_groups', host_group).rstrip('\n')

                # reading individual host groups
                with open(host_group_path) as json_file:
                    host_group_json = json.load(json_file)
                    self.host_groups[host_group_json['name']] = HostGroup(host_group_json['components'],
                                                                          host_group_json['configurations'],
                                                                          host_group_json['name'],
                                                                          host_group_json['cardinality'])

    def _change_yarn_mem_allocation(self):
        self.configurations['yarn-site'].properties['yarn.nodemanager.resource.memory-mb'] = "20544"


class Configuration:
    def __init__(self, attributes, properties):
        # type: ({}, {}) -> None
        """
        Constructor for Configuration
        :return: 
        """
        self.properties_attributes = attributes
        self.properties = properties

    def add_ldap_entries(self, ldif_entries):
        self.properties['content'] += ldif_entries


class HostGroup:
    def __init__(self, components, configurations, name, cardinality):
        # type: ({}, {}, str, int) -> None
        """
        Constructor for HostGroup
        :return: 
        """
        self.components = components
        self.configurations = configurations
        self.name = name
        self.cardinality = cardinality

if __name__ == "__main__":
    setup_logger()
    args = parse_args()

    deploy = Deploy(config_file=args.config)

    blueprint_builder = BlueprintBuilder(deploy)
    deploy.blueprint = blueprint_builder.create_all()
