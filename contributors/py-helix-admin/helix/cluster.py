"""base class for anything that connects to helix"""

from participant import Participant
from partition import Partition
from resourcegroup import ResourceGroup

from helixexceptions import HelixException
import functions


class Cluster(object):
    """Basic model of a cluster, holds participants, partitions, slices,
    external view, ideal state, resource groups"""
    ver = (1, 0)

    def __init__(self, host, cluster):
        super(Cluster, self).__init__()
        self.host = host
        self.cluster = cluster

        # dynamically loaded data below
        self._partitions = {}
        self._participants = {}
        self._resources = {}
        self._ideal_state = {}
        self._external_view = {}

    def __str__(self):
        return "{0} Object for {1}".format(self.__class__.__name__,
                                           self.cluster)

    def __repr__(self):
        return "{0}({1}, {2})".format(self.__class__.__name__, self.cluster,
                                      self.host)

    def load_resources(self):
        """queries helix for resource groups and loades them into model"""
        try:
            for cur_resource in functions.get_resource_groups(self.host,
                                                              self.cluster):
                data = functions.get_resource_group(self.host, self.cluster,
                                                    cur_resource)
                name = data["id"]
                count = data["simpleFields"]["NUM_PARTITIONS"]
                replicas = data["simpleFields"]["REPLICAS"]
                statemode = data["simpleFields"]["STATE_MODEL_DEF_REF"]
                resource = ResourceGroup(name,
                                         count, replicas,
                                         statemode, data)
                partitions = data["mapFields"]
                for part, hosts in partitions.items():
                    phosts = []
                    for host, status in hosts.items():
                        participant = self.participants[host]
                        participant.partitions[part] = status
                        phosts.append(participant)

                    partition = Partition(part, phosts)
                    resource.add_partition(partition)

                self._resources[cur_resource] = resource
        except HelixException:
            pass

    @property
    def resources(self):
        """sanely handle resource loading and usage"""
        if not self._resources:
            self.load_resources()
        return self._resources

    @resources.setter
    def resources(self, value):
        """ensure an exception is raise on an attempt to set resource groups"""
        raise HelixException("Resource groups cannont be added in this manner")

    def _cluster_exists(self):
        """verify cluster exists in helix"""
        if self.cluster in functions.get_clusters(self.host):
            return True
        return False

    def load_participants(self):
        """create instances of storage node for participants in this cluster"""
        self._participants = {}

        try:
            instances = functions.get_instances(self.host, self.cluster)
            for instance in instances:
                ident = instance["id"]
                enabled = instance["simpleFields"]["HELIX_ENABLED"]
                alive = instance["simpleFields"]["Alive"]
                data = instance
                participant = Participant(ident, alive, enabled, data)
                self._participants[instance["id"]] = participant
        except HelixException:
            pass

    @property
    def participants(self):
        """returns participants, if not loaded, loads them then returns"""
        if not self._participants:
            self.load_participants()
        return self._participants

    @participants.setter
    def participants(self, value):
        raise HelixException("Participants cannot added in this fashion!")

    def load_partitions(self):
        """query partitions from helix and load into model"""
        self._partitions = {}
        for resource in self.resources:
            newstate = functions.get_ideal_state(self.host, self.cluster,
                                                 resource)
            self._partitions[resource] = {}
            if newstate:
                for part in newstate:
                    hosts = [self.participants[x] for x in newstate[part]]
                    partition = Partition(part, hosts)
                    self._partitions[resource][part] = partition
                    for host in newstate[part]:
                        self.participants[host].partitions[part] = partition

    @property
    def partitions(self):
        """return partitions"""
        if not self._partitions:
            self.load_partitions()
        return self._partitions

    def load_ideal_state(self):
        """query ideal state from helix and load into model"""
        self._ideal_state = {}
        for resource in self.resources:
            self._ideal_state[resource] = \
                functions.get_ideal_state(self.host, self.cluster, resource)

    @property
    def ideal_state(self):
        """return ideal state"""
        if not self._ideal_state:
            self.load_ideal_state()
        return self._ideal_state

    @ideal_state.setter
    def ideal_state(self, value):
        """setter for ideal state"""
        raise HelixException("Cannot adjust Ideal State in this manner")

    def load_external_view(self):
        """query external view from helix and load into model"""
        self._external_view = {}
        for resource in self.resources:
            self._external_view[resource] = \
                functions.get_external_view(self.host, self.cluster, resource)

    @property
    def external_view(self):
        """return external view"""
        if not self._external_view:
            self.load_external_view()
        return self._external_view

    @external_view.setter
    def external_view(self, value):
        """setter for external view"""
        raise HelixException("External View cannot be modified!")

    def get_config(self, config):
        """ get requested config from helix"""
        return functions.get_config(self.host, self.cluster, config)

    def set_cluster_config(self, config):
        """ set given configs in helix"""
        return functions.set_config(self.host, self.cluster, config)

    def set_resource_config(self, config, resource):
        """ set given configs in helix"""
        rname = resource
        if isinstance(resource, ResourceGroup):
            rname = resource.name
        return functions.set_config(self.host, self.cluster, config,
                                    resource=rname)

    def set_participant_config(self, config, participant):
        pname = participant
        if isinstance(participant, Participant):
            pname = participant.ident
        """ set given configs in helix"""
        return functions.set_config(self.host, self.cluster, config,
                                    participant=pname)

    def activate_cluster(self, grand, enabled=True):
        """activate this cluster with the specified grand cluster"""
        return functions.activate_cluster(self.host, self.cluster, grand,
                                          enabled)

    def deactivate_cluster(self, grand):
        """deactivate this cluster against the given grandcluster"""
        return functions.deactivate_cluster(self.host, self.cluster, grand)

    def add_cluster(self):
        """add cluster to helix"""
        return functions.add_cluster(self.host, self.cluster)

    def add_instance(self, instances, port):
        """add instance to cluster"""
        return functions.add_instance(self.host, self.cluster, instances, port)

    def rebalance(self, resource, replicas, key=""):
        """rebalance a resource group"""
        return functions.rebalance(self.host, self.cluster, resource,
                                   replicas, key)

    def add_resource(self, resource, partitions, state_model_def, mode=""):
        """add resource to cluster"""
        return functions.add_resource(self.host, self.cluster, resource,
                                      partitions, state_model_def, mode)

    def enable_instance(self, instance, enabled=True):
        """enable instance, assumes instance a participant object"""
        ident = None
        if isinstance(instance, Participant):
            ident = instance.ident
        elif isinstance(instance, str):
            ident = instance
        else:
            raise HelixException("Instance must be a string or participant")
        return functions.enable_instance(self.host, self.cluster, ident,
                                         enabled)

    def disable_instance(self, instance):
        """disable instance, assumes instance is a participant object"""
        return self.enable_instance(instance, enabled=False)

    def enable_partition(self, resource, partition, instance, enabled=True):
        """enable partition, assumes instance and partition are
        helix objects"""
        ident = None
        part_id = None

        if isinstance(instance, Participant):
            ident = instance.ident
        elif isinstance(instance, str):
            ident = instance
        else:
            raise HelixException("Instance must be a string or participant")

        if isinstance(partition, Partition):
            part_id = partition.name
        elif isinstance(partition, str):
            part_id = partition
        else:
            raise HelixException("Partition must be a string or partition")

        return functions.enable_partition(self.host, self.cluster, resource,
                                          part_id, ident, enabled)

    def disable_partition(self, resource, partition, instance):
        """disable partition, conveience function for enable partition"""
        return self.enable_partition(resource, partition, instance,
                                     enabled=False)

    def enable_resource(self, resource, enabled=True):
        """enable/disable resource"""
        resource_name = None
        if isinstance(resource, ResourceGroup):
            resource_name = resource.name
        elif isinstance(resource, str):
            resource_name = resource
        else:
            raise HelixException(
                "Resource must be a string or a resource group object")

        return functions.enable_resource(self.host, self.cluster,
                                         resource_name, enabled)

    def disable_resource(self, resource):
        """disable given function"""
        return self.enable_resource(resource, enabled=False)

    def add_resource_tag(self, resource, tag):
        """add a tag to a resource"""
        resource_name = None
        if isinstance(resource, ResourceGroup):
            resource_name = resource.name
        elif isinstance(resource, str):
            resource_name = resource
        else:
            raise HelixException("Resource must be resource object or string")

        return functions.add_resource_tag(self.host, self.cluster,
                                          resource_name, tag)

    # del resource not yet available in api
    # def del_resource_tag(self, resource, tag):
    # """del a tag to a resource"""
    #     resource_name = None
    #     if isinstance(resource, ResourceGroup):
    #         resource_name = resource.name
    #     elif isinstance(resource, str):
    #         resource_name = resource
    #     else:
    #         raise HelixException("Resource must be resource object or str")
    #
    #     return functions.del_resource_tag(self.host, self.cluster,
    #                                       resource_name, tag)

    def add_instance_tag(self, instance, tag):
        ident = None

        if isinstance(instance, Participant):
            ident = instance.ident
        elif isinstance(instance, str):
            ident = instance
        else:
            raise HelixException("Instance must be a string or participant")

        return functions.add_instance_tag(self.host, self.cluster, ident, tag)

    def del_instance_tag(self, instance, tag):
        ident = None

        if isinstance(instance, Participant):
            ident = instance.ident
        elif isinstance(instance, str):
            ident = instance
        else:
            raise HelixException("Instance must be a string or participant")

        return functions.del_instance_tag(self.host, self.cluster, ident, tag)

    def del_instance(self, instance):
        """remove instance from cluster, assumes instance is a
        participant object"""
        return functions.del_instance(self.host, self.cluster, instance.ident)

    def del_resource(self, resource):
        """remove resource group from cluster, assumes resource is a
        resource object"""
        return functions.del_resource(self.host, self.cluster, resource.name)

    def del_cluster(self):
        """remove cluster from helix"""
        return functions.del_cluster(self.host, self.cluster)
