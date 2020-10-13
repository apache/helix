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

<head>
  <title>Tutorial - Throttling</title>
</head>

## [Helix Tutorial](./Tutorial.html): Throttling

In this chapter, we\'ll learn how to control the parallel execution of cluster tasks.  Only a centralized cluster manager with global knowledge (i.e. Helix) is capable of coordinating this decision.

### Throttling

Since all state changes in the system are triggered through transitions, Helix can control the number of transitions that can happen in parallel. Some of the transitions may be lightweight, but some might involve moving data, which is quite expensive from a network and IOPS perspective.

Helix allows applications to set a threshold on transitions. The threshold can be set at multiple scopes:

* MessageType e.g STATE_TRANSITION
* TransitionType e.g SLAVE-MASTER
* Resource e.g database
* Node i.e per-node maximum transitions in parallel


