#!/usr/bin/env python2.6

from helixexceptions import HelixException
from helixexceptions import HelixAlreadyExistsException
from helixexceptions import HelixDoesNotExistException

from participant import Participant
from partition import Partition
from resourcegroup import ResourceGroup

from functions import RestHelixFunctions
from zkfunctions import ZookeeperHelixFunctions

from cluster import ZKCluster
from cluster import Cluster

import pytest
import random

INSTANCE_ID = "fake_12345"
INSTANCE_NAME = "fake"
INSTANCE_PORT = 12345
PARTITION_COUNT = 5
REPLICA_COUNT = 1
STATEMODELDEF = "LeaderStandby"
REBALANCE_MODE = "FULL_AUTO"
RESOURCE_NAME = "fake_resource"
TAG_NAME = "fake_tag"

CLUSTER_ID = "helix_{id}"
ZOOKEEPER_ROOT = "/testing_helix"
ZOOKEEPER_HOST = "localhost:2181"
REST_HOST = "localhost:8100"

class TestHelixAdmin(object):
    #@pytest.mark.int
    def test_zookeeper_cluster(self):
        cluster = ZKCluster(ZOOKEEPER_HOST, ZOOKEEPER_ROOT, self._get_cluster_name())
        self._cluster_actions(cluster)

    #@pytest.mark.int
    def test_rest_cluster(self):
        cluster = Cluster(REST_HOST, self._get_cluster_name())
        self._cluster_actions(cluster)

    def _get_cluster_name(self):
        return CLUSTER_ID.format(id=random.randint(1, 1000000))

    def _cluster_actions(self, cluster):
        cluster.add_cluster()
        cluster.add_resource(RESOURCE_NAME, PARTITION_COUNT, STATEMODELDEF, mode=REBALANCE_MODE)
        cluster.add_instance(INSTANCE_NAME, INSTANCE_PORT)
        cluster.add_instance_tag(INSTANCE_ID, TAG_NAME)
        cluster.add_resource_tag(RESOURCE_NAME, TAG_NAME)
        cluster.rebalance(RESOURCE_NAME, REPLICA_COUNT)
        participant = cluster.participants.get(INSTANCE_ID)
        cluster.del_instance(participant)
        resource = cluster.resources.get(RESOURCE_NAME)
        cluster.del_resource(resource)
        cluster.del_cluster()
