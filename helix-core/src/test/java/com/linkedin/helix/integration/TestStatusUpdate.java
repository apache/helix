package com.linkedin.helix.integration;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.util.StatusUpdateUtil;

public class TestStatusUpdate extends ZkStandAloneCMTestBase
{
  @Test
  public void testParticipantStatusUpdates() throws Exception
  {
    ZkClient zkClient = new ZkClient(ZkIntegrationTestBase.ZK_ADDR);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    DataAccessor accessor = new ZKDataAccessor(CLUSTER_NAME, zkClient);

    List<ExternalView> extViews =
        accessor.getChildValues(ExternalView.class, PropertyType.EXTERNALVIEW);
    Assert.assertNotNull(extViews);

    for (ExternalView extView : extViews)
    {
      String resourceName = extView.getResourceName();
      Set<String> partitionSet = extView.getPartitionSet();
      for (String partition : partitionSet)
      {
        Map<String, String> stateMap = extView.getStateMap(partition);
        for (String instance : stateMap.keySet())
        {
          String state = stateMap.get(instance);
          StatusUpdateUtil.StatusUpdateContents statusUpdates =
              StatusUpdateUtil.StatusUpdateContents.getStatusUpdateContents(accessor,
                                                                            instance,
                                                                            resourceName,
                                                                            partition);

          Map<String, StatusUpdateUtil.TaskStatus> taskMessages =
              statusUpdates.getTaskMessages();
          List<StatusUpdateUtil.Transition> transitions = statusUpdates.getTransitions();
          if (state.equals("MASTER"))
          {
            Assert.assertEquals(transitions.size() >= 2,
                                true,
                                "Invalid number of transitions");
            StatusUpdateUtil.Transition lastTransition =
                transitions.get(transitions.size() - 1);
            StatusUpdateUtil.Transition prevTransition =
                transitions.get(transitions.size() - 2);
            Assert.assertEquals(taskMessages.get(lastTransition.getMsgID()),
                                StatusUpdateUtil.TaskStatus.COMPLETED,
                                "Incomplete transition");
            Assert.assertEquals(taskMessages.get(prevTransition.getMsgID()),
                                StatusUpdateUtil.TaskStatus.COMPLETED,
                                "Incomplete transition");
            Assert.assertEquals(lastTransition.getFromState(), "SLAVE", "Invalid State");
            Assert.assertEquals(lastTransition.getToState(), "MASTER", "Invalid State");
          }
          else if (state.equals("SLAVE"))
          {
            Assert.assertEquals(transitions.size() >= 1,
                                true,
                                "Invalid number of transitions");
            StatusUpdateUtil.Transition lastTransition =
                transitions.get(transitions.size() - 1);
            Assert.assertEquals(lastTransition.getFromState().equals("MASTER")
                                    || lastTransition.getFromState().equals("OFFLINE"),
                                true,
                                "Invalid transition");
            Assert.assertEquals(lastTransition.getToState(), "SLAVE", "Invalid State");
          }
        }
      }
    }
  }
}
