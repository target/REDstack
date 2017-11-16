
class Node:

    def __init__(self, name=None, fqdn=None, internal_ip=None, server_id=None, floating_ip=None, ram=None,
                 role=None, volume_size=None, flavor=None, ambari_group=None, primary=False):
        self.name = name
        self.fqdn = fqdn
        self.internal_ip = internal_ip
        self.floating_ip = floating_ip
        self.server_id = server_id
        self.ram = ram
        self.role = role
        self.volume_size = volume_size
        self.flavor = flavor
        self.ambari_group = ambari_group
        self.primary = primary
