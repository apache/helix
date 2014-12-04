"""base class for anything that connects to helix"""


class Partition(object):
    """Object to deal helix partitions"""

    def __init__(self, name, hosts):
        super(Partition, self).__init__()
        self.name = name
        self.hosts = hosts

    def __str__(self):
        return "Partition {0} - Hosts: {1}".format(self.name, ", ".join(
            [x.ident for x in self.hosts]))

    def __repr__(self):
        return "{0}('{1}', {2})".format(self.__class__.__name__, self.name,
                                        self.hosts)
