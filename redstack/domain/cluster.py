import json
import yaml

from redstack.domain.node import Node
from redstack.exceptions import NodeNotFoundException


class Cluster:

    def __init__(self, ssh_user=None, private_key=None, key_name=None, cluster_name=None,
                 template_file=None, fqdn_address=None, json_file=None):
        # type: (str, str, str, str, str, {}, str, str) -> None
        """
        Constructor for Cluster
        :param ssh_user: ssh username for base image authentication
        :param private_key: private key string representation for authentication
        :param key_name: name of the keypair in openstack
        :param project_name: name of the project in openstack
        :param cluster_name: custom name to assign to cluster in ambari
        :param node_dict: dictionary representation of nodes from the template file
        :param fqdn_address: fqdn to append to the hostname
        :param json_file: json file representing a cluster to initialize with instead
        """

        # If we are passed a JSON file, initialize using the JSON file
        if json_file:
            with open(json_file, 'r') as file:
                cluster_dict = json.load(file)
                self.ssh_user = cluster_dict['ssh_user']
                self.private_key = cluster_dict['private_key']
                self.key_name = cluster_dict['key_name']
                self.cluster_name = cluster_dict['cluster_name']
                self.master_node = Node(
                    name=cluster_dict['master_node']['name'], fqdn=cluster_dict['master_node']['fqdn'],
                    internal_ip=cluster_dict['master_node']['internal_ip'],
                    floating_ip=cluster_dict['master_node']['floating_ip'], ram=cluster_dict['master_node']['ram'],
                    role=cluster_dict['master_node']['role'], volume_size=cluster_dict['master_node']['volume_size'],
                    flavor=cluster_dict['master_node']['flavor'],
                    ambari_group=cluster_dict['master_node']['ambari_group'],
                    primary=cluster_dict['master_node']['primary']
                )

                self.nodes = []
                for node_dict in cluster_dict['nodes']:
                    node = Node(name=node_dict['name'], fqdn=node_dict['fqdn'], internal_ip=node_dict['internal_ip'],
                                floating_ip=node_dict['floating_ip'], ram=node_dict['ram'], role=node_dict['role'],
                                volume_size=node_dict['volume_size'], flavor=node_dict['flavor'],
                                ambari_group=node_dict['ambari_group'], primary=node_dict['primary'])
                    self.nodes.append(node)

        # If we are passed kwargs instead, initialize with those
        else:
            self.ssh_user = ssh_user
            self.private_key = private_key
            self.key_name = key_name
            self.cluster_name = cluster_name

            self.nodes = []

            with open(template_file, 'r') as template_yaml_file:
                template_dictionary = yaml.load(template_yaml_file)

                # Build initial nodes
                for node_spec, node_properties in template_dictionary['nodes'].iteritems():
                    node_count = int(node_properties['count'])

                    # Loop over all nodes of the current node type
                    for i in range(1, node_count + 1):
                        if node_count == 1:
                            node_name = node_spec
                        else:
                            node_name = '%s%d' % (node_spec, i)

                        # Fill out the node properties
                        node_fqdn = node_name + fqdn_address
                        node_role = node_properties['runlist']
                        node_volume_size = node_properties['volume_size']
                        node_flavor = node_properties['flavor']
                        ambari_group = node_properties['ambari_group']
                        node_primary = True if node_name == template_dictionary['primary'] else False

                        node = Node(name=node_name, ambari_group=ambari_group, fqdn=node_fqdn, role=node_role,
                                    volume_size=node_volume_size, flavor=node_flavor, primary=node_primary)

                        if node_primary:
                            self.master_node = node

                        self.nodes.append(node)

    def to_json(self):
        # type: () -> str
        """
        Return a json object containing the cluster json representation
        :return: A json string representation of the cluster object
        """
        return json.dumps({
            'ssh_user': self.ssh_user,
            'private_key': self.private_key,
            'key_name': self.key_name,
            'nodes': [
                node.__dict__ for node in self.nodes
            ],
            'cluster_name': self.cluster_name,
            'master_node': self.master_node.__dict__
        })

    def get_hosts_list(self):
        # type: () -> {}
        """
        Return a dictionary that can be injected as a chef attribute as json
        :return: A dictionary of the nodes as a json string
        """

        hosts = {}
        for node in self.nodes:
            host = {
                'fqdn': node.fqdn,
                'internal_ip': node.internal_ip,
                'external_ip': node.floating_ip
            }
            hosts[node.name] = host

        return hosts

    def get_node(self, node_name):
        # type: (str) -> Node
        """
        Returns a node based on the node name given
        :param node_name: 
        :return: A node object
        """
        for node in self.nodes:
            if node.name == node_name:
                return node

        raise NodeNotFoundException('Node not found in cluster node list')
