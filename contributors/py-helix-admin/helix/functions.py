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


def _post_payload(host, path, data, **kwargs):
    """generic function to handle posting data
    :rtype : return body of page
    :param host: host to send data to
    :param path: path to interact with
    :param data: data to send
    :param kwargs:  additional keyword args
    """

    if "http://" not in host:
        host = "http://{0}".format(host)

    res = Resource(host)

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


def _get_page(host, path):
    """if we're specifying a cluster then verify that a cluster is set"""

    if "http://" not in host:
        host = "http://{0}".format(host)

    res = Resource(host)

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


def _delete_page(host, path):
    """delete page at a given path"""
    retval = None
    if "http://" not in host:
        host = "http://{0}".format(host)

    res = Resource(host)

    page = res.delete(path)
    data = page.body_string()
    if data:
        retval = json.loads(data)

    return retval


def get_clusters(host):
    """ querys helix cluster for all clusters """
    return _get_page(host, "/clusters")["listFields"]["clusters"]


def get_resource_groups(host, cluster):
    """ querys helix cluster for resources groups of the current cluster"""
    return _get_page(host, "/clusters/{0}/resourceGroups".format(cluster))[
        "listFields"]["ResourceGroups"]


def get_resource_tags(host, cluster):
    """returns a dict of resource tags for a cluster"""
    return _get_page(host, "/clusters/{0}/resourceGroups".format(cluster))[
        "mapFields"]["ResourceTags"]


def get_resource_group(host, cluster, resource):
    """ gets the ideal state of the specified resource group of the
    current cluster"""
    if resource not in get_resource_groups(host, cluster):
        raise HelixException(
            "{0} is not a resource group of {1}".format(resource, cluster))

    return _get_page(host, "/clusters/{0}/resourceGroups/{1}".format(cluster,
                                                                     resource))


def get_ideal_state(host, cluster, resource):
    """ gets the ideal state of the specified resource group of the
    current cluster"""

    if resource not in get_resource_groups(host, cluster):
        raise HelixException(
            "{0} is not a resource group of {1}".format(resource, cluster))

    return _get_page(host, "/clusters/{0}/resourceGroups/{1}/idealState".
                     format(cluster, resource))["mapFields"]


def get_external_view(host, cluster, resource):
    """return the external view for a given cluster and resource"""
    if resource not in get_resource_groups(host, cluster):
        raise HelixException(
            "{0} is not a resource group of {1}".format(resource, cluster))

    return _get_page(host,
                     "/clusters/{0}/resourceGroups/{1}/externalView".format(
                         cluster, resource))["mapFields"]


def get_instances(host, cluster):
    """get list of instances registered to the cluster"""
    if not cluster:
        raise HelixException("Cluster must be set before "
                             "calling this function")

    return _get_page(host, "/clusters/{0}/instances".format(cluster))[
        "instanceInfo"]


def get_instance_detail(host, cluster, name):
    """get details of an instance"""
    return _get_page(host, "/clusters/{0}/instances/{1}".format(cluster, name))


def get_config(host, cluster, config):
    """get requested config"""
    return _get_page(host, "/clusters/{0}/configs/{1}".format(cluster, config))


def add_cluster(host, cluster):
    """add a cluster to helix"""
    if cluster in get_clusters(host):
        raise HelixAlreadyExistsException(
            "Cluster {0} already exists".format(cluster))

    data = {"command": "addCluster",
            "clusterName": cluster}

    page = _post_payload(host, "/clusters", data)
    return page


def add_instance(host, cluster, instances, port):
    """add a list of instances to a cluster"""
    if cluster not in get_clusters(host):
        raise HelixDoesNotExistException(
            "Cluster {0} does not exist".format(cluster))

    if not isinstance(instances, list):
        instances = [instances]
    instances = ["{0}:{1}".format(instance, port) for instance in instances]
    try:
        newinstances = set(instances)
        oldinstances = set(
            [x["id"].replace('_', ':') for x in get_instances(host, cluster)])
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
        page = _post_payload(host, instance_path, data)
        return page

    else:
        raise HelixAlreadyExistsException(
            "All instances given already exist in cluster")


def rebalance(host, cluster, resource, replicas, key=""):
    """rebalance the given resource group"""
    if resource not in get_resource_groups(host, cluster):
        raise HelixException(
            "{0} is not a resource group of {1}".format(resource, cluster))

    data = {"command": "rebalance",
            "replicas": replicas}

    if key:
        data["key"] = key
    page = _post_payload(host,
                         "/clusters/{0}/resourceGroups/{1}/idealState".format(
                             cluster, resource), data)
    return page


def activate_cluster(host, cluster, grand_cluster, enabled=True):
    """activate the cluster with the grand cluster"""
    if grand_cluster not in get_clusters(host):
        raise HelixException(
            "grand cluster {0} does not exist".format(grand_cluster))

    data = {'command': 'activateCluster',
            'grandCluster': grand_cluster}

    if enabled:
        data["enabled"] = "true"
    else:
        data["enabled"] = "false"

    page = _post_payload(host, "/clusters/{0}".format(cluster), data)
    return page


def deactivate_cluster(host, cluster, grand_cluster):
    """deactivate the cluster with the grand cluster"""
    return activate_cluster(host, cluster, grand_cluster, enabled=False)


def add_resource(host, cluster, resource, partitions,
                 state_model_def, mode=""):
    """Add given resource group"""
    if resource in get_resource_groups(host, cluster):
        raise HelixAlreadyExistsException(
            "ResourceGroup {0} already exists".format(resource))

    data = {"command": "addResource",
            "resourceGroupName": resource,
            "partitions": partitions,
            "stateModelDefRef": state_model_def}

    if mode:
        data["mode"] = mode

    return _post_payload(host, "/clusters/{0}/resourceGroups".format(cluster),
                         data)


def enable_resource(host, cluster, resource, enabled=True):
    """enable or disable specified resource"""
    data = {"command": "enableResource"}
    if enabled:
        data["enabled"] = "true"
    else:
        data["enabled"] = "false"

    return _post_payload(host, "/clusters/{0}/resourceGroups/{1}".format(
        cluster, resource), data)


def disable_resource(host, cluster, resource):
    """function for disabling resources"""
    return enable_resource(host, cluster, resource, enabled=False)


def alter_ideal_state(host, cluster, resource, newstate):
    """alter ideal state"""
    data = {"command": "alterIdealState"}
    return _post_payload(host,
                         "/clusters/{0}/resourceGroups/{1}/idealState".format(
                             cluster, resource), data,
                         newIdealState=newstate)


def enable_instance(host, cluster, instance, enabled=True):
    """enable instance within cluster"""
    data = {"command": "enableInstance"}
    if enabled:
        data["enabled"] = "true"
    else:
        data["enabled"] = "false"

    return _post_payload(host, "/clusters/{0}/instances/{1}".format(cluster,
                                                                    instance),
                         data)


def disable_instance(host, cluster, instance):
    """wrapper for ease of use for disabling an instance"""
    return enable_instance(host, cluster, instance, enabled=False)


def swap_instance(host, cluster, old, new):
    """swap instance"""
    data = {"command": "swapInstance",
            "oldInstance": old,
            "newInstance": new}

    return _post_payload(host, "/cluster/{0}/instances".format(cluster), data)


def enable_partition(host, cluster, resource, partition, instance,
                     enabled=True):
    """enable Partition """
    if resource not in get_resource_groups(host, cluster):
        raise HelixDoesNotExistException(
            "ResourceGroup {0} does not exist".format(resource))

    data = {"command": "enablePartition",
            "resource": resource,
            "partition": partition,
            "enabled": enabled}
    return _post_payload(host, "/clusters/{0}/instances/{1}".format(cluster,
                                                                    instance),
                         data)


def disable_partition(host, cluster, resource, partitions, instance):
    """disable Partition """
    return enable_partition(host, cluster, resource, partitions, instance,
                            enabled=False)


def reset_partition(host, cluster, resource, partitions, instance):
    """reset partition"""
    if resource not in get_resource_groups(host, cluster):
        raise HelixDoesNotExistException(
            "ResourceGroup {0} does not exist".format(resource))

    data = {"command": "resetPartition",
            "resource": resource,
            "partition": " ".join(partitions)}
    return _post_payload(host, "/clusters/{0}/instances/{1}".format(cluster,
                                                                    instance),
                         data)


def reset_resource(host, cluster, resource):
    """reset resource"""
    if resource not in get_resource_groups(host, cluster):
        raise HelixDoesNotExistException(
            "ResourceGroup {0} does not exist".format(resource))

    data = {"command": "resetResource"}
    return _post_payload(host,
                         "/clusters/{0}/resourceGroups/{1}".format(cluster,
                                                                   resource),
                         data)


def reset_instance(host, cluster, instance):
    """reset instance"""
    if instance not in get_instances(host, cluster):
        raise HelixDoesNotExistException(
            "Instance {0} does not exist".format(instance))

    data = {"command": "resetInstance"}
    return _post_payload(host, "/clusters/{0}/instances/{1}".format(cluster,
                                                                    instance),
                         data)


def add_instance_tag(host, cluster, instance, tag):
    """add tag to an instance"""
    data = {"command": "addInstanceTag",
            "instanceGroupTag": tag}
    return _post_payload(host,
                         "/clusters/{0}/instances/{1}".format(
                             cluster, instance), data)


def del_instance_tag(host, cluster, instance, tag):
    """remove tag from instance"""
    data = {"command": "removeInstanceTag",
            "instanceGroupTag": tag}
    return _post_payload(host,
                         "/clusters/{0}/instances/{1}".format(
                             cluster, instance), data)


def add_resource_tag(host, cluster, resource, tag):
    """add tag to resource group"""
    if resource not in get_resource_groups(host, cluster):
        raise HelixDoesNotExistException(
            "ResourceGroup {0} does not exist".format(resource))

    data = {"command": "addResourceProperty",
            "INSTANCE_GROUP_TAG": tag}
    return _post_payload(host,
                         "/clusters/{0}/resourceGroups/{1}/idealState".format(
                             cluster, resource), data)


"""
del resource currently does not exist in helix api
def del_resource_tag(host, cluster, resource, tag):
    if resource not in get_resource_groups(host, cluster):
        raise HelixDoesNotExistException(
            "ResourceGroup {0} does not exist".format(resource))

    data = {"command": "removeResourceProperty",
            "INSTANCE_GROUP_TAG": tag}
    return _post_payload(host,
                         "/clusters/{0}/resourceGroups/{1}/idealState".format(
                             cluster, resource), data)
"""


def get_instance_taginfo(host, cluster):
    return _get_page(host, "/clusters/{0}/instances".format(
        cluster))["tagInfo"]


def expand_cluster(host, cluster):
    """expand cluster"""
    data = {"command": "expandCluster"}

    return _post_payload(host, "/clusters/{0}/".format(cluster), data)


def expand_resource(host, cluster, resource):
    """expand resource"""
    data = {"command": "expandResource"}

    return _post_payload(host,
                         "/clusters/{0}/resourceGroup/{1}/idealState".format(
                             cluster, resource), data)


def add_resource_property(host, cluster, resource, properties):
    """add resource property properties must be a dictionary of properties"""
    properties["command"] = "addResourceProperty"

    return _post_payload(host,
                         "/clusters/{0}/resourceGroup/{1}/idealState".format(
                             cluster, resource), properties)


def _handle_config(host, cluster, configs, command, participant=None,
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

    return _post_payload(host, address, data)


def set_config(host, cluster, configs, participant=None, resource=None):
    """sets config in helix"""
    return _handle_config(host, cluster, configs, "set", participant, resource)


def remove_config(host, cluster, configs, participant=None, resource=None):
    """sets config in helix"""
    return _handle_config(host, "remove", cluster, configs, participant,
                          resource)


def get_zk_path(host, path):
    """get zookeeper path"""
    return _get_page(host, "zkPath/{0}".format(path))


def del_zk_path(host, path):
    """delete zookeeper path"""
    return _delete_page(host, "zkPath/{0}".format(path))


def get_zk_child(host, path):
    """get zookeeper child"""
    return _get_page(host, "zkChild/{0}".format(path))


def del_zk_child(host, path):
    """delete zookeeper child"""
    return _delete_page(host, "zkChild/{0}".format(path))


def add_state_model(host, cluster, newstate):
    """add state model"""
    data = {"command": "addStateModel"}

    return _post_payload(host, "/clusters/{0}/StateModelDefs".format(cluster),
                         data, newStateModelDef=newstate)


def del_instance(host, cluster, instance):
    """delete instance"""
    if instance not in [x["id"] for x in get_instances(host, cluster)]:
        raise HelixDoesNotExistException(
            "Instance {0} does not exist.".format(instance))

    page = _delete_page(host,
                        "/clusters/{0}/instances/{1}".format(cluster,
                                                             instance))
    return page


def del_resource(host, cluster, resource):
    """delete specified resource from cluster"""
    if resource not in get_resource_groups(host, cluster):
        raise HelixDoesNotExistException(
            "ResourceGroup {0} does not exist".format(resource))

    page = _delete_page(host, "/clusters/{0}/resourceGroups/{1}".format(
        cluster, resource))
    return page


def del_cluster(host, cluster):
    """delete cluster"""
    page = _delete_page(host, "/clusters/{0}".format(cluster))

    return page


def send_message(host, cluster, path, **kwargs):
    pass
