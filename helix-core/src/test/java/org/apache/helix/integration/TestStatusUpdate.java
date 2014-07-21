package org.apache.helix.integration;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.util.StatusUpdateUtil;
import org.testng.Assert;

public class TestStatusUpdate extends ZkStandAloneCMTestBase {
  // For now write participant StatusUpdates to log4j.
  // TODO: Need to investigate another data channel to report to controller and re-enable
  // this test
  // @Test
  public void testParticipantStatusUpdates() throws Exception {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();

    List<ExternalView> extViews = accessor.getChildValues(keyBuilder.externalViews());
    Assert.assertNotNull(extViews);

    for (ExternalView extView : extViews) {
      String resourceName = extView.getResourceName();
      Set<String> partitionSet = extView.getPartitionSet();
      for (String partition : partitionSet) {
        Map<String, String> stateMap = extView.getStateMap(partition);
        for (String instance : stateMap.keySet()) {
          String state = stateMap.get(instance);
          StatusUpdateUtil.StatusUpdateContents statusUpdates =
              StatusUpdateUtil.StatusUpdateContents.getStatusUpdateContents(accessor, instance,
                  resourceName, partition);

          Map<String, StatusUpdateUtil.TaskStatus> taskMessages = statusUpdates.getTaskMessages();
          List<StatusUpdateUtil.Transition> transitions = statusUpdates.getTransitions();
          if (state.equals("MASTER")) {
            Assert.assertEquals(transitions.size() >= 2, true, "Invalid number of transitions");
            StatusUpdateUtil.Transition lastTransition = transitions.get(transitions.size() - 1);
            StatusUpdateUtil.Transition prevTransition = transitions.get(transitions.size() - 2);
            Assert.assertEquals(taskMessages.get(lastTransition.getMsgID()),
                StatusUpdateUtil.TaskStatus.COMPLETED, "Incomplete transition");
            Assert.assertEquals(taskMessages.get(prevTransition.getMsgID()),
                StatusUpdateUtil.TaskStatus.COMPLETED, "Incomplete transition");
            Assert.assertEquals(lastTransition.getFromState(), "SLAVE", "Invalid State");
            Assert.assertEquals(lastTransition.getToState(), "MASTER", "Invalid State");
          } else if (state.equals("SLAVE")) {
            Assert.assertEquals(transitions.size() >= 1, true, "Invalid number of transitions");
            StatusUpdateUtil.Transition lastTransition = transitions.get(transitions.size() - 1);
            Assert.assertEquals(lastTransition.getFromState().equals("MASTER")
                || lastTransition.getFromState().equals("OFFLINE"), true, "Invalid transition");
            Assert.assertEquals(lastTransition.getToState(), "SLAVE", "Invalid State");
          }
        }
      }
    }
  }
}
