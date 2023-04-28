package org.apache.helix.controller.dataproviders;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;


/**
 * An interface to provide resource partition weights data. The result should quantify the resource utilization for
 * bootstrapping transition, the burden made to the hosting instance.
 * See also {@link InstanceStateTransitionCapacityProvider}
 */
public interface ResourceWeightsDataProvider {

  /**
   * Get the partition weights as a map, keyed by the attribute name.
   * @param resource resource name
   * @param partition partition name
   * @return <str, int> capacity pairs of all defined attributes for the resource partition
   */
  Map<String, Integer> getPartitionWeights(String resource, String partition);
}
