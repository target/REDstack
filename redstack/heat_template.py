import os

import yaml

import helper_functions
from domain.cluster import Cluster
from domain.deploy import Deploy
from domain.node import Node


class HeatTemplate():
    def __init__(self, deploy, output_file=None):
        # type: () -> None
        """
        Constructor for HeatTemplate
        :param deploy: the current deploy object
        :param output_file: The optional output file for the heat template
        """
        self.deploy = deploy

        # Default the output file if we don't use it
        if output_file:
            self.output_file = output_file
        else:
            self.output_file = os.path.join(self.deploy.directory, "template.yml")

    def generate_with_existing_network(self, subnet_id, network_id):
        # type: (str, str) -> None
        """
        Create the heat template for a setup with an existing network
        :param subnet_id: The existing id for the subnet to use
        "param network_id: The existing id for the network to use
        """
        heat_dict = {
            'resources': {},
            'parameters': {
                'private_network': {
                    'default': network_id,
                    'type': 'string'
                },
                'private_subnet': {
                    'default': subnet_id,
                    'type': 'string'
                },
                'image': {
                    'default': self.deploy.image_name,
                    'type': 'string'
                },
                'key_name': {
                    'default': self.deploy.key_name,
                    'type': 'string'
                },
                'public_network': {
                    'default': self.deploy.external_network_id,
                    'type': 'string'
                }
            },
            'description': 'The template for redstack',
            'heat_template_version': '2013-05-23'
        }

        # Add the security group
        heat_dict['resources']['rs_security_group'] = self.create_security_group()

        # Create entries for all of the nodes
        for node in self.deploy.cluster.nodes:

            # Create volume size entries in template dictionary
            volume_size_block_title = '{0}_node_volume_size'.format(node.name)

            heat_dict['parameters'][volume_size_block_title] = self.create_volume_size_entry(
                node.name, node.volume_size)

            # Create Floating IP Resource
            heat_dict['resources']['floating_ip_' + node.name] = self.create_fip_entry(node.name)

            # Create Public Port Resource
            heat_dict['resources']['public_port_' + node.name] = self.create_public_port_entry(True)

            # Create Node Resource
            heat_dict['resources'][node.name] = self.create_node_entry(node)

            # Create Volume Attachment Resource
            heat_dict['resources']['volume_attachment_' + node.name] = self.create_volume_attachment_entry(node.name)

            # Create Volume Resource
            heat_dict['resources']['volume_' + node.name] = self.create_volume_entry(node.name, volume_size_block_title)

        with open(self.output_file, "w") as yml_file:
            yaml.dump(heat_dict, yml_file, default_flow_style=False)

    def generate(self):
        # type: () -> None
        """
        Create the heat template
        """
        heat_dict = {
            'resources': {
                "rs_network": {
                    "type": "OS::Neutron::Net",
                    "properties": {
                        "name": 'rs_network'
                    }
                },
                "rs_subnet": {
                    "type": "OS::Neutron::Subnet",
                    "properties": {
                        "network_id": {
                            "get_resource": "rs_network"
                        },
                        "cidr": self.deploy.subnet_cidr,
                        'enable_dhcp': True,
                        'dns_nameservers': self.deploy.subnet_dns_nameservers
                    },
                    'depends_on': 'rs_network'
                },
                "rs_router": {
                    "type": "OS::Neutron::Router",
                    "properties": {
                        "external_gateway_info": {
                            "network": {
                                'get_param': 'public_network'
                            }
                        }
                    }
                },
                "rs_router_interface": {
                    "type": "OS::Neutron::RouterInterface",
                    "properties": {
                        "router_id": {
                            "get_resource": "rs_router"
                        },
                        "subnet_id": {
                            "get_resource": "rs_subnet"
                        }
                    },
                    'depends_on': [
                        'rs_router',
                        'rs_subnet'
                    ]
                }
            },
            'parameters': {
                'image': {
                    'default': self.deploy.image_name,
                    'type': 'string'
                },
                'key_name': {
                    'default': self.deploy.key_name,
                    'type': 'string'
                },
                'public_network': {
                    'default': self.deploy.external_network_id,
                    'type': 'string'
                }
            },
            'description': 'The template for redstack',
            'heat_template_version': '2013-05-23'
        }

        # Add the security group
        heat_dict['resources']['rs_security_group'] = self.create_security_group()

        # Create entries for all of the nodes
        for node in self.deploy.cluster.nodes:

            # Create volume size entries in template dictionary
            volume_size_block_title = '{0}_node_volume_size'.format(node.name)

            heat_dict['parameters'][volume_size_block_title] = self.create_volume_size_entry(
                node.name, node.volume_size)

            # Create Floating IP Resource
            heat_dict['resources']['floating_ip' + node.name] = self.create_fip_entry(node.name)

            # Create Public Port Resource
            heat_dict['resources']['public_port_' + node.name] = self.create_public_port_entry(False)

            # Create Node Resource
            heat_dict['resources'][node.name] = self.create_node_entry(node)

            # Create Volume Attachment Resource
            heat_dict['resources']['volume_attachment_' + node.name] = self.create_volume_attachment_entry(node.name)

            # Create Volume Resource
            heat_dict['resources']['volume_' + node.name] = self.create_volume_entry(node.name, volume_size_block_title)

        with open(self.output_file, "w") as yml_file:
            yaml.dump(heat_dict, yml_file, default_flow_style=False)

    def create_security_group(self):
        # type: () -> {}
        """
        Create a custom security group for redstack
        :return: A dictionary for the heat templates
        """

        rs_security_group = {
            'type': 'OS::Neutron::SecurityGroup',
            'properties': {
                'description': 'Add security group rules for redstack',
                'name': 'rs_security_group',
                'rules': [
                    {
                        'direction': 'ingress',
                        'remote_ip_prefix': self.deploy.subnet_cidr
                    },
                    {
                        'direction': 'egress',
                        'remote_ip_prefix': '0.0.0.0/0'
                    },
                    # App history server
                    {
                        'protocol': 'tcp',
                        'direction': 'ingress',
                        'remote_ip_prefix': self.deploy.expose_ui_ssh,
                        'port_range_min': '8188',
                        'port_range_max': '8188'
                    },
                    # Namenode
                    {
                        'protocol': 'tcp',
                        'direction': 'ingress',
                        'remote_ip_prefix': self.deploy.expose_ui_ssh,
                        'port_range_min': '50070',
                        'port_range_max': '50070'
                    },
                    {   # knox and ambari server
                        'protocol': 'tcp',
                        'direction': 'ingress',
                        'remote_ip_prefix': self.deploy.expose_ui_ssh,
                        'port_range_min': '8443',
                        'port_range_max': '8443'
                    },
                    {   # ssh
                        'protocol': 'tcp',
                        'direction': 'ingress',
                        'remote_ip_prefix': self.deploy.expose_ui_ssh,
                        'port_range_min': '22',
                        'port_range_max': '22'
                    },
                    {   # namenode ui
                        'protocol': 'tcp',
                        'direction': 'ingress',
                        'remote_ip_prefix': self.deploy.expose_ui_ssh,
                        'port_range_min': '50070',
                        'port_range_max': '50070'
                    },
                    {   # resource manager ui
                        'protocol': 'tcp',
                        'direction': 'ingress',
                        'remote_ip_prefix': self.deploy.expose_ui_ssh,
                        'port_range_min': '8088',
                        'port_range_max': '8088'
                    },
                    {   # history server ui
                        'protocol': 'tcp',
                        'direction': 'ingress',
                        'remote_ip_prefix': self.deploy.expose_ui_ssh,
                        'port_range_min': '19888',
                        'port_range_max': '19888'
                    },
                    {   # spark history server ui
                        'protocol': 'tcp',
                        'direction': 'ingress',
                        'remote_ip_prefix': self.deploy.expose_ui_ssh,
                        'port_range_min': '18080',
                        'port_range_max': '18080'
                    },
                    {   # zeppelin notebook
                        'protocol': 'tcp',
                        'direction': 'ingress',
                        'remote_ip_prefix': self.deploy.expose_ui_ssh,
                        'port_range_min': '9995',
                        'port_range_max': '9995'
                    },
                    {   # journalnode ui
                        'protocol': 'tcp',
                        'direction': 'ingress',
                        'remote_ip_prefix': self.deploy.expose_ui_ssh,
                        'port_range_min': '8480',
                        'port_range_max': '8480'
                    }
                ]
            }
        }

        return rs_security_group

    @staticmethod
    def create_volume_size_entry(node_name, volume_size):
        # type: (str, int) -> {}
        """
        Create the volume size entry 
        :param node_name: name of the node
        :param volume_size: size of the volume on the node
        :return: dictionary for the heat template
        """
        return {
            'default': volume_size,
            'description': 'Size of volume to attach to %s compute instances' % node_name,
            'type': 'number'
        }

    def create_fip_entry(self, node_name):
        # type: (str) -> {}
        """
        Create the floating IP entry
        :param node_name: name of the node
        :return: dictionary for the heat template
        """
        return {
            'properties': {
                'floating_network_id': {
                    'get_param': 'public_network'
                },
                'port_id': {
                    'get_resource': 'public_port_' + node_name
                }
            },
            'depends_on': 'public_port_' + node_name,
            'type': 'OS::Neutron::FloatingIP'
        }

    def create_public_port_entry(self, existing_network):
        # type: (bool) -> {}
        """
        Create the public port entry (VLAN)
        :return: dictionary for the heat template
        whether or not to use the existing network reference
        """
        public_port_entry = {
            'properties': {
                "security_groups": [
                    {
                        'get_resource': 'rs_security_group'
                    }
                ]
            },
            'depends_on': [
                'rs_security_group'
            ],
            'type': 'OS::Neutron::Port'
        }

        if existing_network:
            public_port_entry['properties']['fixed_ips'] = [{'subnet_id': {'get_param': 'private_subnet'}}]
            public_port_entry['properties']['network_id'] = {'get_param': 'private_network'}
        else:
            public_port_entry['depends_on'].append('rs_router_interface')
            public_port_entry['properties']['fixed_ips'] = [{'subnet_id': {'get_resource': 'rs_subnet'}}]
            public_port_entry['properties']['network_id'] = {'get_resource': 'rs_network'}

        return public_port_entry

    def create_node_entry(self, node):
        # type: (Node) -> {}
        """
        returns a node entry for a node
        :param node: The node to create an entry for/
        :return: dictionary for the heat template
        """
        node_entry = {
            'properties': {
                'flavor': node.flavor,
                'image': {
                    'get_param': 'image'
                },
                'key_name': {
                    'get_param': 'key_name'
                },
                'name': node.name,
                'networks': [
                    {
                        'port': {
                            'get_resource': 'public_port_' + node.name
                        }
                    }
                ],
            },
            'depends_on': 'public_port_{0}'.format(node.name),
            'type': 'OS::Nova::Server'
        }

        if self.deploy.availability_zone:
            node_entry['properties']['availability_zone'] = self.deploy.availability_zone

        return node_entry

    def create_volume_attachment_entry(self, node_name):
        # type: (str) -> {}
        """
        Create a volume attachment entry for the heat template
        :param node_name: The name of the node to make it with
        :return: the response as a dictionary
        """
        return {
            'depends_on': [
                node_name,
                'volume_' + node_name
            ],
            'properties': {
                'instance_uuid': {
                    'get_resource': node_name
                },
                'mountpoint': self.deploy.volume_device,
                'volume_id': {
                    'get_resource': 'volume_' + node_name
                }
            },
            'type': 'OS::Cinder::VolumeAttachment'
        }

    def create_volume_entry(self, node_name, volume_size_block_title):
        # type: (str, str) -> {}
        """
        Create a volume entry for the heat template
        :param node_name: The name of the node to create it for
        :param volume_size_block_title: The volume title specified up north
        :return: The reponse as a dictionary
        """
        volume_entry = {
            'properties': {
                'name': 'volume_' + node_name,
                'size': {
                    'get_param': volume_size_block_title
                }
            },
            'type': 'OS::Cinder::Volume'
        }

        if self.deploy.availability_zone:
            volume_entry['properties']['availability_zone'] = self.deploy.availability_zone

        return volume_entry

if __name__ == '__main__':
    helper_functions.setup_logger()
    args = helper_functions.parse_args()

    deployment = Deploy(config_file=args.config, cluster=Cluster())

    obj = HeatTemplate(deployment)
    obj.generate()
