package com.linkedin.helix.integration;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.util.StatusUpdateUtil;
import com.linkedin.helix.*;

public class TestStatusUpdate extends ZkStandAloneCMTestBase
{

  static class Transition implements Comparable<Transition>
  {
    private final String _msgID;
    private final long   _timeStamp;
    private final String _from;
    private final String _to;

    public Transition(String msgID, long timeStamp, String from, String to)
    {
      this._msgID = msgID;
      this._timeStamp = timeStamp;
      this._from = from;
      this._to = to;
    }

    @Override
    public int compareTo(Transition t)
    {
      if (_timeStamp < t._timeStamp)
        return -1;
      else if (_timeStamp > t._timeStamp)
        return 1;
      else
        return 0;
    }

    public boolean equals(Transition t)
    {
      return (_timeStamp == t._timeStamp && _from.equals(t._from) && _to.equals(t._to));
    }

    public String getFromState()
    {
      return _from;
    }

    public String getToState()
    {
      return _to;
    }
    
    public String getMsgID()
    {
      return _msgID;
    }
    
    @Override
    public String toString() 
    {
      return _msgID + ":" + _timeStamp + ":" + _from + "->" + _to;
    }
  }

  enum TaskStatus
  {
    UNKNOWN, SCHEDULED, INVOKING, COMPLETED, FAILED
  }

  static class StatusUpdateContents
  {
    private final List<Transition>  _transitions;
    private final Map<String, TaskStatus> _taskMessages;

    private StatusUpdateContents(List<Transition> transitions,
                                 Map<String, TaskStatus> taskMessages)
    {
      this._transitions = transitions;
      this._taskMessages = taskMessages;
    }

    // TODO: We should build a map and return the key instead of searching everytime
    // for an (instance, resourceGroup, partition) tuple.
    // But such a map is very similar to what exists in ZNRecord
    public static StatusUpdateContents getStatusUpdateContents(DataAccessor accessor,
                                                               String instance,
                                                               String resourceGroup,
                                                               String partition)
    {
      List<ZNRecord> instances = accessor.getChildValues(PropertyType.CONFIGS,
          ConfigScopeProperty.PARTICIPANT.toString());
      List<ZNRecord> partitionRecords = new ArrayList<ZNRecord>();
      for (ZNRecord znRecord : instances)
      {
        String instanceName = znRecord.getId();
        if (!instanceName.equals(instance))
        {
          continue;
        }

        List<String> sessions =
            accessor.getChildNames(PropertyType.STATUSUPDATES, instanceName);
        for (String session : sessions)
        {
          List<String> resourceGroups =
              accessor.getChildNames(PropertyType.STATUSUPDATES, instanceName, session);
          for (String resourceGroupName : resourceGroups)
          {
            if (!resourceGroupName.equals(resourceGroup))
            {
              continue;
            }
            
            List<String> partitionStrings =
                accessor.getChildNames(PropertyType.STATUSUPDATES, instanceName, session, resourceGroupName);
            
            for (String partitionString : partitionStrings)
            {
              ZNRecord partitionRecord = accessor.getProperty(PropertyType.STATUSUPDATES, instanceName, 
                                                              session, resourceGroupName, partitionString);
              String partitionName = partitionString.split("_")[1];
              if(!partitionName.equals(partition))
              {
                continue;
              }
              partitionRecords.add(partitionRecord);
            }
          }
        }
      }

      return new StatusUpdateContents(getSortedTransitions(partitionRecords),
                                      getTaskMessages(partitionRecords));
    }

    public List<Transition> getTransitions()
    {
      return _transitions;
    }

    public Map<String, TaskStatus> getTaskMessages()
    {
      return _taskMessages;
    }

    // input: List<ZNRecord> corresponding to (instance, database,
    // partition) tuples across all sessions
    // return list of transitions sorted from earliest to latest
    private static List<Transition> getSortedTransitions(List<ZNRecord> partitionRecords)
    {
      List<Transition> transitions = new ArrayList<Transition>();
      for (ZNRecord partition : partitionRecords)
      {
        Map<String, Map<String, String>> mapFields = partition.getMapFields();
        for (String key : mapFields.keySet())
        {
          if (key.startsWith("MESSAGE"))
          {
            Map<String, String> m = mapFields.get(key);
            long createTimeStamp = 0;
            try
            {
              createTimeStamp = Long.parseLong(m.get("CREATE_TIMESTAMP"));
            }
            catch (Exception e)
            {
            }
            transitions.add(new Transition(m.get("MSG_ID"), 
                                           createTimeStamp,
                                           m.get("FROM_STATE"),
                                           m.get("TO_STATE")));
          }
        }
      }
      Collections.sort(transitions);
      return transitions;
    }

    private static Map<String, TaskStatus> getTaskMessages(List<ZNRecord> partitionRecords)
    {
      Map<String, TaskStatus> taskMessages = new HashMap<String, TaskStatus>();
      for (ZNRecord partition : partitionRecords)
      {
        Map<String, Map<String, String>> mapFields = partition.getMapFields();
        //iterate over the task status updates in the order they occurred 
        //so that the last status can be recorded
        for (String key : mapFields.keySet())
        {
          if (key.contains("STATE_TRANSITION"))
          {
            Map<String, String> m = mapFields.get(key);
            String id = m.get("MSG_ID");
            String statusString = m.get("AdditionalInfo");
            TaskStatus status = TaskStatus.UNKNOWN;
            if (statusString.contains("scheduled"))
              status = TaskStatus.SCHEDULED;
            else if (statusString.contains("invoking"))
              status = TaskStatus.INVOKING;
            else if (statusString.contains("completed"))
              status = TaskStatus.COMPLETED;

            taskMessages.put(id, status);
          }
        }
      }
      return taskMessages;
    }
  }

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
