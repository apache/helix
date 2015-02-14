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
import json
from restkit import Resource

from helixexceptions import HelixException
from helixexceptions import HelixAlreadyExistsException
from helixexceptions import HelixDoesNotExistException

class RestHelixFunctions:
    def __init__(self, host):
        if "http://" not in host:
            self.host = "http://{0}".format(host)
        else:
            self.host = host

    def _post_payload(self, path, data, **kwargs):
        """generic function to handle posting data
        :rtype : return body of page
        :param path: path to interact with
        :param data: data to send
        :param kwargs:  additional keyword args
        """

        res = Resource(self.host)

        payload = "jsonParameters={0}".format(json.dumps(data))
        for key, value in kwargs.items():
            payload += '&{0}={1}'.format(key, json.dumps(value))
        headers = {"Content-Type": "application/json"}
        # print "path is %s" % path
        page = res.post(path=path, payload=payload, headers=headers)
        body = page.body_string()
        if body:
            body = json.loads(body)

            if isinstance(body, dict) and "ERROR" in body:
                raise HelixException(body["ERROR"])

        # test what was returned, see if any exceptions need to be raise
        # if not body:
        # raise HelixException("body for path {0} is empty".format(path))
        # else:
        # print "BODY IS EMPTY FOR ", path
        # print "BODY is %s." % body

        return body


    def _get_page(self, path):
        """if we're specifying a cluster then verify that a cluster is set"""

        res = Resource(self.host)

        page = res.get(path=path)
        data = page.body_string()
        body = None
        try:
            body = json.loads(data)
        except ValueError:
            body = json.loads(data[:-3])

        # test what was returned, see if any exceptions need to be raise
        if not body:
            raise HelixException("body for path {0} is empty".format(path))

        if isinstance(body, dict) and "ERROR" in body:
            raise HelixException(body["ERROR"])

        return body


    def _delete_page(self, path):
        """delete page at a given path"""
        retval = None

        res = Resource(self.host)

        page = res.delete(path)
        data = page.body_string()
        if data:
            retval = json.loads(data)

        return retval


    def get_clusters(self):
        """ querys helix cluster for all clusters """
        return self._get_page("/clusters")["listFields"]["clusters"]


    def get_resource_groups(self, cluster):
        """ querys helix cluster for resources groups of the current cluster"""
        return self._get_page("/clusters/{0}/resourceGroups".format(cluster))[
            "listFields"]["ResourceGroups"]


    def get_resource_tags(self, cluster):
        """returns a dict of resource tags for a cluster"""
        return self._get_page("/clusters/{0}/resourceGroups".format(cluster))[
            "mapFields"]["ResourceTags"]


    def get_resource_group(self, cluster, resource):
        """ gets the ideal state of the specified resource group of the
        current cluster"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixException(
                "{0} is not a resource group of {1}".format(resource, cluster))

        return self._get_page("/clusters/{0}/resourceGroups/{1}".format(cluster,
            resource))

    def get_ideal_state(self, cluster, resource):
        """ gets the ideal state of the specified resource group of the
        current cluster"""

        if resource not in self.get_resource_groups(cluster):
            raise HelixException(
                "{0} is not a resource group of {1}".format(resource, cluster))

        return self._get_page("/clusters/{0}/resourceGroups/{1}/idealState".
                         format(cluster, resource))["mapFields"]

    def get_external_view(self, cluster, resource):
        """return the external view for a given cluster and resource"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixException(
                "{0} is not a resource group of {1}".format(resource, cluster))

        return self._get_page("/clusters/{0}/resourceGroups/{1}/externalView".format(
                             cluster, resource))["mapFields"]

    def get_instances(self, cluster):
        """get list of instances registered to the cluster"""
        if not cluster:
            raise HelixException("Cluster must be set before "
                                 "calling this function")

        return self._get_page("/clusters/{0}/instances".format(cluster))[
            "instanceInfo"]

    def get_instance_detail(self, cluster, name):
        """get details of an instance"""
        return self._get_page("/clusters/{0}/instances/{1}".format(cluster, name))

    def get_config(self, cluster, config):
        """get requested config"""
        return self._get_page("/clusters/{0}/configs/{1}".format(cluster, config))

    def add_cluster(self, cluster):
        """add a cluster to helix"""
        if cluster in self.get_clusters():
            raise HelixAlreadyExistsException(
                "Cluster {0} already exists".format(cluster))

        data = {"command": "addCluster",
                "clusterName": cluster}

        page = self._post_payload("/clusters", data)
        return page

    def add_instance(self, cluster, instances, port):
        """add a list of instances to a cluster"""
        if cluster not in self.get_clusters():
            raise HelixDoesNotExistException(
                "Cluster {0} does not exist".format(cluster))

        if not isinstance(instances, list):
            instances = [instances]
        instances = ["{0}:{1}".format(instance, port) for instance in instances]
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
            data = {"command": "addInstance",
                    "instanceNames": ";".join(instances)}

            instance_path = "/clusters/{0}/instances".format(cluster)
            # print "adding to", instance_path
            page = self._post_payload(instance_path, data)
            return page

        else:
            raise HelixAlreadyExistsException(
                "All instances given already exist in cluster")

    def rebalance(self, cluster, resource, replicas, key=""):
        """rebalance the given resource group"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixException(
                "{0} is not a resource group of {1}".format(resource, cluster))

        data = {"command": "rebalance",
                "replicas": replicas}

        if key:
            data["key"] = key
        page = self._post_payload("/clusters/{0}/resourceGroups/{1}/idealState".format(
                                 cluster, resource), data)
        return page

    def activate_cluster(self, cluster, grand_cluster, enabled=True):
        """activate the cluster with the grand cluster"""
        if grand_cluster not in self.get_clusters():
            raise HelixException(
                "grand cluster {0} does not exist".format(grand_cluster))

        data = {'command': 'activateCluster',
                'grandCluster': grand_cluster}

        if enabled:
            data["enabled"] = "true"
        else:
            data["enabled"] = "false"

        page = self._post_payload("/clusters/{0}".format(cluster), data)
        return page

    def deactivate_cluster(self, cluster, grand_cluster):
        """deactivate the cluster with the grand cluster"""
        return activate_cluster(cluster, grand_cluster, enabled=False)

    def add_resource(self, cluster, resource, partitions, state_model_def, mode=""):
        """Add given resource group"""
        if resource in self.get_resource_groups(cluster):
            raise HelixAlreadyExistsException(
                "ResourceGroup {0} already exists".format(resource))

        data = {"command": "addResource",
                "resourceGroupName": resource,
                "partitions": partitions,
                "stateModelDefRef": state_model_def}

        if mode:
            data["mode"] = mode

        return self._post_payload("/clusters/{0}/resourceGroups".format(cluster),
                             data)

    def enable_resource(self, cluster, resource, enabled=True):
        """enable or disable specified resource"""
        data = {"command": "enableResource"}
        if enabled:
            data["enabled"] = "true"
        else:
            data["enabled"] = "false"

        return self._post_payload("/clusters/{0}/resourceGroups/{1}".format(
            cluster, resource), data)

    def disable_resource(self, cluster, resource):
        """function for disabling resources"""
        return enable_resource(cluster, resource, enabled=False)

    def alter_ideal_state(self, cluster, resource, newstate):
        """alter ideal state"""
        data = {"command": "alterIdealState"}
        return self._post_payload("/clusters/{0}/resourceGroups/{1}/idealState".format(
                                 cluster, resource), data,
                             newIdealState=newstate)

    def enable_instance(self, cluster, instance, enabled=True):
        """enable instance within cluster"""
        data = {"command": "enableInstance"}
        if enabled:
            data["enabled"] = "true"
        else:
            data["enabled"] = "false"

        return self._post_payload("/clusters/{0}/instances/{1}".format(cluster,
                                                                        instance),
                             data)

    def disable_instance(self, cluster, instance):
        """wrapper for ease of use for disabling an instance"""
        return enable_instance(cluster, instance, enabled=False)

    def swap_instance(self, cluster, old, new):
        """swap instance"""
        data = {"command": "swapInstance",
                "oldInstance": old,
                "newInstance": new}

        return self._post_payload("/cluster/{0}/instances".format(cluster), data)

    def enable_partition(self, cluster, resource, partition, instance,
                         enabled=True):
        """enable Partition """
        if resource not in self.get_resource_groups(cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))

        data = {"command": "enablePartition",
                "resource": resource,
                "partition": partition,
                "enabled": enabled}
        return self._post_payload("/clusters/{0}/instances/{1}".format(cluster,
                                                                        instance),
                             data)

    def disable_partition(self, cluster, resource, partitions, instance):
        """disable Partition """
        return enable_partition(cluster, resource, partitions, instance,
                                enabled=False)

    def reset_partition(self, cluster, resource, partitions, instance):
        """reset partition"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))

        data = {"command": "resetPartition",
                "resource": resource,
                "partition": " ".join(partitions)}
        return self._post_payload("/clusters/{0}/instances/{1}".format(cluster,
                                                                        instance),
                             data)

    def reset_resource(self, cluster, resource):
        """reset resource"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))

        data = {"command": "resetResource"}
        return self._post_payload("/clusters/{0}/resourceGroups/{1}".format(cluster,
                                                                       resource),
                             data)

    def reset_instance(self, cluster, instance):
        """reset instance"""
        if instance not in self.get_instances(cluster):
            raise HelixDoesNotExistException(
                "Instance {0} does not exist".format(instance))

        data = {"command": "resetInstance"}
        return self._post_payload("/clusters/{0}/instances/{1}".format(cluster,
                                                                        instance),
                             data)

    def add_instance_tag(self, cluster, instance, tag):
        """add tag to an instance"""
        data = {"command": "addInstanceTag",
                "instanceGroupTag": tag}
        return self._post_payload("/clusters/{0}/instances/{1}".format(
                                 cluster, instance), data)

    def del_instance_tag(self, cluster, instance, tag):
        """remove tag from instance"""
        data = {"command": "removeInstanceTag",
                "instanceGroupTag": tag}
        return self._post_payload("/clusters/{0}/instances/{1}".format(
                                 cluster, instance), data)

    def add_resource_tag(self, cluster, resource, tag):
        """add tag to resource group"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))

        data = {"command": "addResourceProperty",
                "INSTANCE_GROUP_TAG": tag}
        return self._post_payload("/clusters/{0}/resourceGroups/{1}/idealState".format(
                                 cluster, resource), data)

    """
    del resource currently does not exist in helix api
    def del_resource_tag(self, cluster, resource, tag):
        if resource not in self.get_resource_groups(host, cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))

        data = {"command": "removeResourceProperty",
                "INSTANCE_GROUP_TAG": tag}
        return _post_payload(host,
                             "/clusters/{0}/resourceGroups/{1}/idealState".format(
                                 cluster, resource), data)
    """

    def get_instance_taginfo(self, cluster):
        return self._get_page("/clusters/{0}/instances".format(
            cluster))["tagInfo"]

    def expand_cluster(self, cluster):
        """expand cluster"""
        data = {"command": "expandCluster"}
        return self._post_payload("/clusters/{0}/".format(cluster), data)

    def expand_resource(self, cluster, resource):
        """expand resource"""
        data = {"command": "expandResource"}

        return self._post_payload("/clusters/{0}/resourceGroup/{1}/idealState".format(
                                 cluster, resource), data)

    def add_resource_property(self, cluster, resource, properties):
        """add resource property properties must be a dictionary of properties"""
        properties["command"] = "addResourceProperty"

        return self._post_payload("/clusters/{0}/resourceGroup/{1}/idealState".format(
                                 cluster, resource), properties)

    def _handle_config(self, cluster, configs, command, participant=None,
                       resource=None):
        """helper function to set or delete configs in helix"""
        data = {"command": "{0}Config".format(command),
                "configs": ",".join(
                    ["{0}={1}".format(x, y) for x, y in configs.items()])}

        address = "/clusters/{0}/configs/".format(cluster)
        if participant:
            address += "participant/{0}".format(participant)
        elif resource:
            address += "resource/{0}".format(resource)
        else:
            address += "cluster"

        return self._post_payload(address, data)

    def set_config(self, cluster, configs, participant=None, resource=None):
        """sets config in helix"""
        return self._handle_config(cluster, configs, "set", participant, resource)

    def remove_config(self, cluster, configs, participant=None, resource=None):
        """sets config in helix"""
        return self._handle_config(host, "remove", cluster, configs, participant,
                              resource)

    def get_zk_path(self, path):
        """get zookeeper path"""
        return self._get_page("zkPath/{0}".format(path))

    def del_zk_path(self, path):
        """delete zookeeper path"""
        return self._delete_page("zkPath/{0}".format(path))

    def get_zk_child(self, path):
        """get zookeeper child"""
        return self._get_page("zkChild/{0}".format(path))

    def del_zk_child(self, path):
        """delete zookeeper child"""
        return self._delete_page("zkChild/{0}".format(path))

    def add_state_model(self, cluster, newstate):
        """add state model"""
        data = {"command": "addStateModel"}

        return self._post_payload("/clusters/{0}/StateModelDefs".format(cluster),
                             data, newStateModelDef=newstate)

    def del_instance(self, cluster, instance):
        """delete instance"""
        if instance not in [x["id"] for x in self.get_instances(cluster)]:
            raise HelixDoesNotExistException(
                "Instance {0} does not exist.".format(instance))

        page = self._delete_page("/clusters/{0}/instances/{1}".format(cluster,
                                                                 instance))
        return page

    def del_resource(self, cluster, resource):
        """delete specified resource from cluster"""
        if resource not in self.get_resource_groups(cluster):
            raise HelixDoesNotExistException(
                "ResourceGroup {0} does not exist".format(resource))

        page = self._delete_page("/clusters/{0}/resourceGroups/{1}".format(
            cluster, resource))
        return page

    def del_cluster(self, cluster):
        """delete cluster"""
        page = self._delete_page("/clusters/{0}".format(cluster))

        return page

    def send_message(self, cluster, path, **kwargs):
        pass
