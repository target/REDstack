""" Module for creating the deployment environment for each deployment.

Deployment environment is the deployment directory containing all resources for the deployment to complete
successfully.
"""

import json
import logging
import os
import shutil

from domain.deploy import Deploy

logger = logging.getLogger("root_logger")


class Environment:
    def __init__(self, deploy):
        # type: (Deploy) -> None
        """
        Constructor for Environment
        :param deploy: The current deploy to make an environment for
        """
        self.deploy = deploy

    def create(self):
        # type: (Deploy) -> None
        """ 
        Created deployment directory and copies all necessary resources for deployment to it
        :return: None
        """
        deployment_path = os.path.join(self.deploy.directory_base, self.deploy.name)

        # This will create the deployment directory before the copy occurs
        shutil.copytree(os.path.join(self.deploy.installation_directory, "cookbook"), os.path.join(deployment_path))
        shutil.copytree(self.deploy.cookbook_directory, os.path.join(self.deploy.directory, 'cookbooks'))
        shutil.copytree(os.path.join(self.deploy.installation_directory, 'conf', 'users'),
                        os.path.join(self.deploy.directory, 'data_bags', 'users'))
        os.makedirs(os.path.join(self.deploy.directory, 'cookbooks', 'redstack', 'recipes'))
        os.makedirs(os.path.join(self.deploy.directory, 'logs', 'ambari'))

        # write blueprint
        with open(os.path.join(self.deploy.directory, 'logs', 'ambari', 'blueprint.json'), 'w') as file:
            file.write(json.dumps(self.deploy.blueprint))

        # write hostmapping
        with open(os.path.join(self.deploy.directory, 'logs', 'ambari', 'hostmapping.json'), 'w') as file:
            file.write(json.dumps(self.deploy.host_mapping))

        self._create_knife_rb()

        # If an existing openstack key is being used, copy the key to the deployment directory and update key name
        if self.deploy.key_name:
            new_private_key = os.path.join(self.deploy.directory, self.deploy.key_name)
            shutil.copyfile(self.deploy.cluster.private_key, new_private_key)
            self.deploy.cluster.private_key = os.path.join(self.deploy.directory, new_private_key)
            os.chmod(self.deploy.cluster.private_key, 0o400)

        logger.info('Deploy directory: {0}'.format(self.deploy.directory))

    def _create_knife_rb(self):
        """ 
        Writes the knife.rb file needed to converge chef on nodes
        :return: None
        """
        contents = """
        node_path                           'nodes'
        cookbook_path                       '{0}'
        role_path                           'roles'
        environment_path                    'environments'
        data_bag_path                       'data_bags'
        knife[:bootstrap_version] =         '{1}'
        knife[:berkshelf_path] =            '{0}'
        Chef::Config[:ssl_verify_mode] =    :verify_peer if defined? ::Chef
        knife[:host_key_verify] =           false
        """.format(
            os.path.join(self.deploy.directory, "cookbooks"),
            self.deploy.chef_version
        )

        with open(os.path.join(self.deploy.directory, "knife.rb"), 'w') as knife_rb:
            knife_rb.write(contents)
