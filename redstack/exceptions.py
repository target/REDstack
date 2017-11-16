"""
Custom exceptions for the project
"""
class ExistingNonRedstackResourcesException(Exception):
    """ 
    Exception indicating Non-REDstack resources exist on the project causing deploys to fail.
    """
    pass


class BasicOpenstackNetworkingException(Exception):
    """ 
    Exception indicating Openstack project is missing one or more of: router, subnet, network
    """
    pass


class ChefException(Exception):
    """
    Exception indicating that Chef failed and returned a failing error code
    """
    pass


class ShellException(Exception):
    """
    Exception indicating that a command failed to execute and returned a failing error code
    """
    pass


class RebuildException(Exception):
    """
    Exception indicating that a node failed to rebuild in Openstack, or failed to react to an API call
    """
    pass


class HeatException(Exception):
    """
    Exception indicating that an openstack HEAT cluster failed to create
    """
    pass


class AmbariException(Exception):
    """
    Exception indication that something failed during the Ambari phase
    """
    pass


class NodeNotFoundException(Exception):
    """
    Exception that indications a node was not found in the data structure
    """
    pass


class ConfigException(Exception):
    """
    Exception that indicates something was misconfigured in the config file
    """
    pass
