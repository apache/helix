<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Helix Tutorial: Customizing Health Checks
-----------------------------------------

In this chapter, we\'ll learn how to customize the health check, based on metrics of your distributed system.

### Health Checks

Note: _this in currently in development mode, not yet ready for production_

Helix provides the ability for each node in the system to report health metrics on a periodic basis.

Helix supports multiple ways to aggregate these metrics:

* SUM
* AVG
* EXPONENTIAL DECAY
* WINDOW

Helix persists the aggregated value only.

Applications can define a threshold on the aggregate values according to the SLAs, and when the SLA is violated Helix will fire an alert.
Currently Helix only fires an alert, but in a future release we plan to use these metrics to either mark the node dead or load balance the partitions.
This feature will be valuable for distributed systems that support multi-tenancy and have a large variation in work load patterns.  In addition, this can be used to detect skewed partitions (hotspots) and rebalance the cluster.

