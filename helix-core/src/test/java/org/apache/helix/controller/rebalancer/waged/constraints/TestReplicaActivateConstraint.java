package org.apache.helix.controller.rebalancer.waged.constraints;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestReplicaActivateConstraint {

  private static final String TEST_RESOURCE = "testResource";
  private static final String TEST_PARTITION = "testPartition";

  private final AssignableReplica _testReplica = Mockito.mock(AssignableReplica.class);
  private final AssignableNode _testNode = Mockito.mock(AssignableNode.class);
  private final ClusterContext _clusterContext = Mockito.mock(ClusterContext.class);

  private final HardConstraint _faultZoneAwareConstraint = new ReplicaActivateConstraint();

  @Test
  public void validWhenEmptyDisabledReplicaMap() {
    Map<String, List<String>> disabledReplicaMap = new HashMap<>();
    disabledReplicaMap.put(TEST_RESOURCE, new ArrayList<>());

    when(_testReplica.getResourceName()).thenReturn(TEST_RESOURCE);
    when(_testReplica.getPartitionName()).thenReturn(TEST_PARTITION);
    when(_testNode.getDisabledPartitionsMap()).thenReturn(disabledReplicaMap);

    Assert.assertTrue(_faultZoneAwareConstraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

  @Test
  public void invalidWhenPartitionIsDisabled() {
    Map<String, List<String>> disabledReplicaMap = new HashMap<>();
    disabledReplicaMap.put(TEST_RESOURCE, Collections.singletonList(TEST_PARTITION));

    when(_testReplica.getResourceName()).thenReturn(TEST_RESOURCE);
    when(_testReplica.getPartitionName()).thenReturn(TEST_PARTITION);
    when(_testNode.getDisabledPartitionsMap()).thenReturn(disabledReplicaMap);

    Assert.assertFalse(_faultZoneAwareConstraint.isAssignmentValid(_testNode, _testReplica, _clusterContext));
  }

}
