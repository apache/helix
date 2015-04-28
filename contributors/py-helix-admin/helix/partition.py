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
