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

"""base class for anything that connects to helix"""

import partition
from helixexceptions import HelixException


class ResourceGroup(object):
    """Object to deal with resource groups"""

    def __init__(self, name, count, replicas, statemode, data):
        super(ResourceGroup, self).__init__()
        self.name = name
        self.count = count
        self.replicas = replicas
        self.state_model_def_ref = statemode
        self.data = data
        self.partitions = {}

    def __str__(self):
        return "Resource: {0} - Count: {1}".format(self.name, self.count)

    def __repr__(self):
        return "{0}('{1}', {2}, {3}, {4}, {5})".format(self.__class__.__name__,
                                                       self.name, self.count,
                                                       self.replicas,
                                                       self.
                                                       state_model_def_ref,
                                                       self.data)

    def add_partition(self, part):
        """add a partition to this resource group"""
        if not isinstance(part, partition.Partition):
            raise HelixException("Argument part must be Partition or subclass")
        self.partitions[part.name] = part
