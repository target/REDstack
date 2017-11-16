import logging
import os

import helper_functions
from ambari import Ambari
from blueprints import BlueprintBuilder
from chef import Chef
from domain.deploy import Deploy
from environment import Environment
from openstack import Openstack


def install(config_file):
    """ 
    Coordinates the cluster deployment from start to finish.
    :param: config_file - Path to main configuration file
    :return: None
    """
    logger.info("Beginning the deployment of a cluster")

    # Create a Deploy object
    deploy = Deploy(config_file)

    # Update logging handler to reflect process configuration
    logger.setLevel(deploy.log_level)

    # Build blueprints for ambari
    blueprint_builder = BlueprintBuilder(deploy)
    blueprint_builder.create_all()

    # Create deployment directory
    environment = Environment(deploy)
    environment.create()

    # OST phase
    openstack = Openstack(deploy)

    if deploy.use_existing_openstack:
        openstack.rebuild()
    else:
        openstack.build()

    # Log out the cluster thus far
    with open(os.path.join(deploy.installation_directory, 'cluster.json'), 'w') as cluster_json_file:
        logger.info(deploy.cluster.to_json())
        cluster_json_file.write(deploy.cluster.to_json())

    # Chef phase
    chef = Chef(deploy)
    chef.converge()

    # Ambari phase
    ambari = Ambari(deploy)
    ambari.install()

    logger.info('REDstack install completed - Ambari: https://{0}:8443'.format(deploy.cluster.master_node.floating_ip))


if __name__ == "__main__":
    helper_functions.setup_logger()
    logger = logging.getLogger('root_logger')

    args = helper_functions.parse_args()

    install(args.config)
