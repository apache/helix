#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""library to handle helix commands"""
# XXX: Explore using zookeeper transactions for some of the write operations into zookeeper.
# Currently, it looks like ensure_path and create with the make_path argument are not supported in transactions so it isn't usable out of the box.
import json
from ordereddict import OrderedDict
from kazoo.client import KazooClient

from statemodeldefs import STATE_DEF_MAP
from helixexceptions import HelixException
from helixexceptions import HelixAlreadyExistsException
from helixexceptions import HelixDoesNotExistException
from kazoo.exceptions import NodeExistsError

RESOURCE_MODES = ["FULL_AUTO", "CUSTOMIZED", "SEMI_AUTO", "USER_DEFINED"]

IDEAL_STATE_PATH = "/{clusterName}/IDEALSTATES"
RESOURCE_IDEAL_STATE_PATH = "/{clusterName}/IDEALSTATES/{resourceName}"
EXTERNAL_VIEW_STATE_PATH = "/{clusterName}/EXTERNALVIEW/{resourceName}"
INSTANCE_PATH = "/{clusterName}/INSTANCES"
PARTICIPANT_CONFIG_PATH = "/{clusterName}/CONFIGS/PARTICIPANT/{instanceName}"
PARTICIPANTS_CONFIG_PATH = "/{clusterName}/CONFIGS/PARTICIPANT"
CLUSTER_CONFIG_PATH = "/{clusterName}/CONFIGS/CLUSTER/{clusterName}"
CONFIG_PATH = "/{clusterName}/CONFIGS/{configName}/{entityName}"
STATE_MODEL_DEF_PATH = "/{clusterName}/STATEMODELDEFS/{stateModelName}"
LIVE_INSTANCE_PATH = "/{clusterName}/LIVEINSTANCES/{instanceName}"

HELIX_ZOOKEEPER_PATHS = {
    "cluster": [
        "/{clusterName}/CONFIGS",
        "/{clusterName}/CONFIGS/RESOURCE",
        "/{clusterName}/CONFIGS/CLUSTER",
        PARTICIPANTS_CONFIG_PATH,
        "/{clusterName}/LIVEINSTANCES",
        INSTANCE_PATH,
        IDEAL_STATE_PATH,
        #"/{clusterName}/RESOURCEASSIGNMENTS",
        "/{clusterName}/EXTERNALVIEW",
        "/{clusterName}/STATEMODELDEFS",
        "/{clusterName}/CONTROLLER",
        "/{clusterName}/CONTROLLER/HISTORY",
        "/{clusterName}/CONTROLLER/ERRORS",
        "/{clusterName}/CONTROLLER/MESSAGES",
        "/{clusterName}/CONTROLLER/STATUSUPDATES",
        "/{clusterName}/PROPERTYSTORE",
    ],
    "resource": [
        RESOURCE_IDEAL_STATE_PATH,
        "/{clusterName}/RESOURCEASSIGNMENTS/{resourceName}",
        EXTERNAL_VIEW_STATE_PATH
    ],
    "instance": [
        #"/{clusterName}/LIVEINSTANCES/{instanceName}",
        "/{clusterName}/INSTANCES/{instanceName}",
        "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES",
        "/{clusterName}/INSTANCES/{instanceName}/ERRORS",
        "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES",
        "/{clusterName}/INSTANCES/{instanceName}/MESSAGES"
    ],
    "statemodel": [
        STATE_MODEL_DEF_PATH
    ]
}

CLUSTER_CONFIG_TEMPLATE = OrderedDict()
CLUSTER_CONFIG_TEMPLATE["id"] = "{clusterName}"
CLUSTER_CONFIG_TEMPLATE["mapFields"] = {}
CLUSTER_CONFIG_TEMPLATE["listFields"] = {}
CLUSTER_CONFIG_TEMPLATE["simpleFields"] = {"allowParticipantAutoJoin": "true"}


class ZookeeperHelixFunctions(object):
    """Zookeeper based client to manage helix clusters"""
    def __init__(self, zookeeper_connect_string, zk_root):
        """Constructor."""
        self.zk = KazooClient(hosts=zookeeper_connect_string)
        self.zk.start()
        self.zk_root = zk_root

    def _list_path(self, path):
        """List a zookeeper path."""
        return self.zk.get_children(path)

    def _is_valid_cluster(self, cluster):
        """Validate cluster configuration."""
        for path in HELIX_ZOOKEEPER_PATHS.get("cluster"):
            full_path = self._build_path(path.format(clusterName=cluster))
            if not self.zk.exists(full_path):
                return False
        return True

    def _build_path(self, path):
        """Construct zookeeper path."""
        return "".join([self.zk_root, path])

    @classmethod
    def _build_instance_entry(cls, instance, enabled="true"):
        """Create the data entry for an instance."""
        host, port = instance.split(":")
        instance_data = OrderedDict()
        instance_data["id"] = "{host}_{port}".format(host=host, port=port)
        instance_data["listFields"] = {}
        instance_data["mapFields"] = {}
        instance_data["simpleFields"] = OrderedDict()
        instance_data["simpleFields"]["HELIX_ENABLED"] = enabled
        instance_data["simpleFields"]["HELIX_HOST"] = host
        instance_data["simpleFields"]["HELIX_PORT"] = port
        return instance_data

    def create_root(self):
        """Initialize zookeeper root"""
        path = self._build_path("")
        if not self.zk.exists(path):
            self.zk.create(path)
        return True

    def get_clusters(self):
        """ querys helix cluster for all clusters """
        if self.zk.exists(self.zk_root):
            return [ cluster for cluster in self._list_path(self.zk_root) if self._is_valid_cluster(cluster) ]
        else:
            return []

    def get_resource_groups(self, cluster):
        """ querys helix cluster for resources groups of the current cluster"""
        return self._list_path(self._build_path(IDEAL_STATE_PATH.format(clusterName=cluster)))

    def get_resource_tags(self, cluster):
        """returns a dict of resource tags for a cluster"""
        resource_tags = {}
        for resource in self.get_resource_groups(cluster):
            resource_data, resource_meta = self._get_resource_group(cluster, resource)
            tag = resource_data.get("INSTANCE_GROUP_TAG")
            if tag:
                resource_tags[tag] = [resource]

        return resource_tags

    def _get_resource_group(self, cluster, resource):
        """ gets the ideal state of the specified resource group of the
        current cluster"""

        if resource not in self.get_resource_groups(cluster):
            raise HelixException(
                "{resource} is not a resource group of {cluster}".format(resource=resource, cluster=cluster))

        data, stat = self.zk.get(self._build_path(RESOURCE_IDEAL_STATE_PATH.format(clusterName=cluster, resourceName=resource)))
        return (json.loads(data), stat)

    def get_resource_group(self, cluster, resource):
        """ COMPAT: gets the ideal state of the specified resource group of the
        current cluster"""

        return self._get_resource_group(cluster, resource)[0]

    def _get_ideal_state(self, cluster, resource):
        """ gets the ideal state of the specified resource group of the
        current cluster"""

        if resource not in self.get_resource_groups(cluster):
            raise HelixException(
                "{0} is not a resource group of {1}".format(resource, cluster))

        return self._get_resource_group(cluster, resource)["mapFields"]

    def get_ideal_state(self, cluster, resource):
        """ COMPAT: gets the ideal state of the specified resource group of the
        current cluster"""

        return self._get_ideal_state(cluster, resource)[0]

    def _get_external_view(self, cluster, resource):
        """return the external view for a given cluster and resource"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixException(
                "{0} is not a resource group of {1}".format(resource, cluster))
        data, stat = self.zk.get(self._build_path(EXTERNAL_VIEW_STATE_PATH.format(clusterName=cluster, resourceName=resource)))
        return (json.loads(data)["mapFields"], stat)

    def get_external_view(self, cluster, resource):
        """ COMPAT: return the external view for a given cluster and resource"""
        return self._get_external_view(cluster, resource)[0]

    def get_instances(self, cluster):
        """get list of instances registered to the cluster"""
        if not cluster:
            raise HelixException("Cluster must be set before "
                                 "calling this function")

        instances = []
        for instance in self._list_path(self._build_path(PARTICIPANTS_CONFIG_PATH.format(clusterName=cluster))):
            instance_data = json.loads(self.zk.get(self._build_path(PARTICIPANT_CONFIG_PATH.format(clusterName=cluster, instanceName=instance)))[0])
            if self.zk.exists(self._build_path(LIVE_INSTANCE_PATH.format(clusterName=cluster, instanceName=instance))):
                instance_data["simpleFields"]["Alive"] = "true"
            else:
                instance_data["simpleFields"]["Alive"] = "false"
            instances.append(instance_data)
        return instances

    def _get_instance_detail(self, cluster, instance):
        """get details of an instance"""
        data, stat = self.zk.get(self._build_path(PARTICIPANT_CONFIG_PATH.format(clusterName=cluster, instanceName=instance)))
        return (json.loads(data), stat)

    def get_instance_detail(self, cluster, instance):
        """ COMPAT: get details of an instance"""
        return self._get_instance_detail(cluster, instance)[0]

    def _get_config(self, cluster, config, entity):
        """get requested config"""
        data, stat = self.zk.get(self._build_path(CONFIG_PATH.format(clusterName=cluster, configName=config, entityName=entity)))
        return (json.loads(data), stat)

    def get_config(self, cluster, config, entity):
        """ COMPAT: get requested config"""
        return self._get_config(cluster, config, entity)[0]

    def add_cluster(self, cluster):
        """add a cluster to helix"""
        if cluster in self.get_clusters():
            raise HelixAlreadyExistsException(
                "Cluster {0} already exists".format(cluster))

        for path in HELIX_ZOOKEEPER_PATHS.get("cluster"):
            self.zk.ensure_path(self._build_path(path.format(clusterName=cluster)))

        data = CLUSTER_CONFIG_TEMPLATE
        data["id"] = cluster

        try:
            self.zk.create(self._build_path(CLUSTER_CONFIG_PATH.format(clusterName=cluster)), json.dumps(data))
        except NodeExistsError:
            # Ignore existing cluster
            pass

        # Insert state defs if they don't exist
        for state_def in STATE_DEF_MAP:
            if not self.zk.exists(self._build_path(STATE_MODEL_DEF_PATH.format(clusterName=cluster, stateModelName=state_def))):
                self.zk.create(self._build_path(STATE_MODEL_DEF_PATH.format(clusterName=cluster, stateModelName=state_def)), json.dumps(STATE_DEF_MAP[state_def]))

        return True

    def add_instance(self, cluster, instances, port):
        """add a list of instances to a cluster"""
        if cluster not in self.get_clusters():
            raise HelixDoesNotExistException(
                "Cluster {0} does not exist".format(cluster))

        if not isinstance(instances, list):
            instances = [instances]
        instances = ["{instance}:{port}".format(instance=instance, port=port) for instance in instances]
        try:
            newinstances = set(instances)
            oldinstances = set(
                [x["id"].replace('_', ':') for x in self.get_instances(cluster)])
            instances = list(newinstances - oldinstances)
        except HelixException:
            # this will get thrown if instances is empty,
            # which if we're just populating should happen
            pass

        if instances:
            for instance in instances:
                data = self._build_instance_entry(instance)
                self.zk.create(self._build_path(PARTICIPANT_CONFIG_PATH.format(clusterName=cluster, instanceName=instance.replace(':', '_'))), json.dumps(data))
                for path in HELIX_ZOOKEEPER_PATHS.get("instance"):
                    self.zk.ensure_path(self._build_path(path.format(clusterName=cluster, instanceName=instance.replace(':', '_'))))
            return True
        else:
            raise HelixAlreadyExistsException(
                "All instances given already exist in cluster")


    def rebalance(self, cluster, resource, replicas, key=""):
        """rebalance the given resource group"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixException(
                "{0} is not a resource group of {1}".format(resource, cluster))

        # TODO: key usage is currently not supported.
        if not key == "":
            raise NotImplementedError

        resource_data, resource_meta = self._get_resource_group(cluster, resource)
        resource_data["simpleFields"]["REPLICAS"] = replicas
        self.zk.set(self._build_path(RESOURCE_IDEAL_STATE_PATH.format(clusterName=cluster, resourceName=resource)), json.dumps(resource_data))

        return True

    def activate_cluster(self, cluster, grand_cluster, enabled=True):
        """activate the cluster with the grand cluster"""
        if grand_cluster not in self.get_clusters():
            raise HelixException(
                "grand cluster {0} does not exist".format(grand_cluster))

        raise NotImplementedError

    def deactivate_cluster(self, cluster, grand_cluster):
        """deactivate the cluster with the grand cluster"""
        return self.activate_cluster(cluster, grand_cluster, enabled=False)


    def add_resource(self, cluster, resource, partitions,
                     state_model_def, mode="", state_model_factory_name="DEFAULT"):
        """Add given resource group"""
        if resource in self.get_resource_groups(cluster):
            raise HelixAlreadyExistsException(
                "ResourceGroup {0} already exists".format(resource))

        data = {"id": resource,
                "mapFields": {},
                "listFields": {},
                "simpleFields": {
                    "IDEAL_STATE_MODE": "AUTO",
                    "NUM_PARTITIONS": partitions,
                    "REBALANCE_MODE": mode,
                    "REPLICAS": "0",
                    "STATE_MODEL_DEF_REF": state_model_def,
                    "STATE_MODEL_FACTORY_NAME": state_model_factory_name
                    }
               }

        if mode:
            if mode in RESOURCE_MODES:
                data["mode"] = mode
            else:
                raise ValueError("Invalid mode ({mode})".format(mode=mode))

        self.zk.create(self._build_path(RESOURCE_IDEAL_STATE_PATH.format(clusterName=cluster, resourceName=resource)), json.dumps(data))
        return True

    def enable_resource(self, cluster, resource, enabled=True):
        """enable or disable specified resource"""
        raise NotImplementedError

    def disable_resource(self, cluster, resource):
        """function for disabling resources"""
        return self.enable_resource(cluster, resource, enabled=False)

    def alter_ideal_state(self, cluster, resource, newstate):
        """alter ideal state"""
        raise NotImplementedError

    def enable_instance(self, cluster, instance, enabled=True):
        """enable instance within cluster"""
        raise NotImplementedError

    def disable_instance(self, cluster, instance):
        """wrapper for ease of use for disabling an instance"""
        return self.enable_instance(cluster, instance, enabled=False)

    def swap_instance(self, cluster, old, new):
        """swap instance"""
        raise NotImplementedError

    def enable_partition(self, cluster, resource, partition, instance,
                       enabled=True):
        """enable Partition """
        if resource not in self.get_resource_groups(cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))
        raise NotImplementedError

    def disable_partition(self, cluster, resource, partitions, instance):
        """disable Partition """
        return self.enable_partition(cluster, resource, partitions, instance,
                                     enabled=False)

    def reset_partition(self, cluster, resource, partitions, instance):
        """reset partition"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))

        raise NotImplementedError

    def reset_resource(self, cluster, resource):
        """reset resource"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))

        raise NotImplementedError

    def reset_instance(self, cluster, instance):
        """reset instance"""
        if instance not in self.get_instances(cluster):
            raise HelixDoesNotExistException(
                "Instance {0} does not exist".format(instance))

        raise NotImplementedError

    def add_instance_tag(self, cluster, instance, tag):
        """add tag to an instance"""
        instance_data, instance_meta = self._get_instance_detail(cluster, instance)
        instance_tags = instance_data.get("listFields").get("TAG_LIST", [])
        if tag in instance_tags:
            raise HelixAlreadyExistsException(
                "Tag ({tag}) already exists for instance ({instance}).".format(tag=tag, instance=instance))

        instance_tags.append(tag)
        instance_data["listFields"]["TAG_LIST"] = instance_tags

        # XXX: Apply some retry logic here
        self.zk.set(self._build_path(PARTICIPANT_CONFIG_PATH.format(clusterName=cluster, instanceName=instance)), json.dumps(instance_data), version=instance_meta.version)
        return True

    def del_instance_tag(self, cluster, instance, tag):
        """remove tag from instance"""
        if instance not in [x["id"] for x in self.get_instances(cluster)]:
            raise HelixDoesNotExistException(
                "Instance {0} does not exist.".format(instance))

    def add_resource_tag(self, cluster, resource, tag):
        """add tag to resource group"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))

        resource_data, resource_stat = self._get_resource_group(cluster, resource)
        resource_data["simpleFields"]["INSTANCE_GROUP_TAG"] = tag

        self.zk.set(self._build_path(RESOURCE_IDEAL_STATE_PATH.format(clusterName=cluster, resourceName=resource)), json.dumps(resource_data), version=resource_stat.version)
        return True

    def del_resource_tag(self, cluster, resource, tag):
        """Delete resource tag."""
        if resource not in self.get_resource_groups(cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))
        raise NotImplementedError

    def get_instance_taginfo(self, cluster):
        """Get resource tag info."""
        instance_tags = {}
        for instance in self.get_instances(cluster):
            list_fields = instance.get("listFields")
            if "TAG_LIST" in list_fields:
                for tag in list_fields.get("TAG_LIST"):
                    if tag in instance_tags:
                        instance_tags[tag].append(instance.get("id"))
                    else:
                        instance_tags[tag] = [instance.get("id")]
        return instance_tags

    def expand_cluster(self, cluster):
        """expand cluster"""
        raise NotImplementedError

    def expand_resource(self, cluster, resource):
        """expand resource"""
        raise NotImplementedError

    def add_resource_property(self, cluster, resource, properties):
        """Add resource property. Properties must be a dictionary of properties."""
        raise NotImplementedError

    def set_config(self, cluster, configs, participant=None, resource=None):
        """sets config in helix"""
        raise NotImplementedError

    def remove_config(self, cluster, configs, participant=None, resource=None):
        """sets config in helix"""
        raise NotImplementedError

    def get_zk_path(self, path):
        """get zookeeper path"""
        return self.zk.get(self._build_path(path))

    def del_zk_path(self, path):
        """delete zookeeper path"""
        return self.zk.delete(self._build_path(path))

    def add_state_model(self, cluster, newstate):
        """add state model"""
        raise NotImplementedError

    def del_instance(self, cluster, instance):
        """delete instance"""
        if cluster not in self.get_clusters():
            raise HelixDoesNotExistException(
                "Cluster {0} does not exist.".format(cluster))

        if instance not in [x["id"] for x in self.get_instances(cluster)]:
            raise HelixDoesNotExistException(
                "Instance {0} does not exist.".format(instance))

        self.zk.delete(self._build_path(PARTICIPANT_CONFIG_PATH.format(clusterName=cluster, instanceName=instance.replace(':', '_'))))

        # Reverse zookeeper structure for destruction.
        for path in HELIX_ZOOKEEPER_PATHS.get("instance")[::-1]:
            self.zk.delete(self._build_path(path.format(clusterName=cluster, instanceName=instance.replace(':', '_'))))
        return True

    def del_resource(self, cluster, resource):
        """delete specified resource from cluster"""
        if cluster not in self.get_clusters():
            raise HelixDoesNotExistException(
                "Cluster {0} does not exist.".format(cluster))

        if resource not in self.get_resource_groups(cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))

        self.zk.delete(self._build_path(RESOURCE_IDEAL_STATE_PATH.format(clusterName=cluster, resourceName=resource)))
        return True

    def del_cluster(self, cluster):
        """delete cluster"""
        if cluster not in self.get_clusters():
            raise HelixDoesNotExistException(
                "Cluster {0} does not exist.".format(cluster))

        self.zk.delete(self._build_path(CLUSTER_CONFIG_PATH.format(clusterName=cluster)))

        for path in HELIX_ZOOKEEPER_PATHS.get("cluster")[::-1]:
            self.zk.ensure_path(self._build_path(path.format(clusterName=cluster)))

        return True

    def send_message(self, cluster, path, **kwargs):
        """Send helix IPC message."""
        raise NotImplementedError
