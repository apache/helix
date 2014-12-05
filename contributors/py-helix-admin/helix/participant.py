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
from helixexceptions import HelixException


class Participant(object):
    """Basic model for a helix participant"""

    def __init__(self, ident, alive, enabled, data):
        super(Participant, self).__init__()
        self.ident = ident
        self.hostname, self.port = ident.split("_")
        self.partitions = {}
        self.data = data
        self.enabled = None
        self._tags = []
        self._disabled_partitions = []

        if isinstance(enabled, str) or isinstance(enabled, unicode):
            if enabled == "true":
                self.enabled = True
            else:
                self.enabled = False
        elif isinstance(enabled, bool):
            self.enabled = enabled

        self.alive = bool(alive)
        self.update()

    def __repr__(self):
        return "{0}('{1}', {2}, {3}, {4})".format(self.__class__.__name__,
                                                  self.ident, self.alive,
                                                  self.enabled, self.data)

    def __str__(self):
        return "Id: {0} Enabled: {1} Alive: {2}".format(self.ident,
                                                        self.enabled,
                                                        self.alive)

    def update(self, data=None):
        """update data for participant then update values"""
        if data:
            self.data = data

        if "TAG_LIST" in self.data["listFields"]:
            self._tags = self.data["listFields"]["TAG_LIST"]

        if "HELIX_DISABLED_PARTITION" in self.data["listFields"]:
            self._disabled_partitions = \
                self.data["listFields"]["HELIX_DISABLED_PARTITION"]

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, value):
        """ensure an exception is raise on an attempt to set tags this way"""
        raise HelixException("Tags must be set on a cluster object")

    @property
    def disabled_partitions(self):
        return self._disabled_partitions

    @disabled_partitions.setter
    def disabled_partitions(self, value):
        raise HelixException("Partitions must be disabled on a cluster object")
