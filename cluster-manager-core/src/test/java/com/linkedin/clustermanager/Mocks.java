package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.clustermanager.messaging.handling.CMTaskExecutor;
import com.linkedin.clustermanager.messaging.handling.CMTaskResult;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelInfo;
import com.linkedin.clustermanager.participant.statemachine.Transition;
import com.linkedin.clustermanager.store.PropertyStore;
import com.linkedin.clustermanager.util.ZNRecordUtil;

public class Mocks
{
  public static class MockStateModel extends StateModel
  {
    boolean stateModelInvoked = false;

    public void onBecomeMasterFromSlave(Message msg, NotificationContext context)
    {
      stateModelInvoked = true;
    }

    public void onBecomeSlaveFromOffline(Message msg,
        NotificationContext context)
    {
      stateModelInvoked = true;
    }
  }

  @StateModelInfo(states = "{'OFFLINE','SLAVE','MASTER'}", initialState = "OFFINE")
  public static class MockStateModelAnnotated extends StateModel
  {
    boolean stateModelInvoked = false;

    @Transition(from = "SLAVE", to = "MASTER")
    public void slaveToMaster(Message msg, NotificationContext context)
    {
      stateModelInvoked = true;
    }

    @Transition(from = "OFFLINE", to = "SLAVE")
    public void offlineToSlave(Message msg, NotificationContext context)
    {
      stateModelInvoked = true;
    }
  }

  public static class MockCMTaskExecutor extends CMTaskExecutor
  {
    boolean completionInvoked = false;

    @Override
    protected void reportCompletion(String msgId)
    {
      System.out.println("Mocks.MockCMTaskExecutor.reportCompletion()");
      completionInvoked = true;
    }

    public boolean isDone(String msgId)
    {
      Future<CMTaskResult> future = _taskMap.get(msgId);
      if (future != null)
      {
        return future.isDone();
      }
      return false;
    }
  }

  public static class MockManager implements ClusterManager
  {
    MockAccessor accessor = new MockAccessor();

    @Override
    public void disconnect()
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addIdealStateChangeListener(IdealStateChangeListener listener)
        throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addLiveInstanceChangeListener(
        LiveInstanceChangeListener listener)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addConfigChangeListener(ConfigChangeListener listener)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addMessageListener(MessageListener listener, String instanceName)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addCurrentStateChangeListener(
        CurrentStateChangeListener listener, String instanceName,
        String sessionId)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addExternalViewChangeListener(
        ExternalViewChangeListener listener)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public ClusterDataAccessor getDataAccessor()
    {
      return accessor;
    }

    @Override
    public String getClusterName()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getInstanceName()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void connect()
    {
      // TODO Auto-generated method stub

    }

    @Override
    public String getSessionId()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean isConnected()
    {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public long getLastNotificationTime()
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void addControllerListener(ControllerChangeListener listener)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean removeListener(Object listener)
    {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public ClusterManagementService getClusterManagmentTool()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ClusterMessagingService getMessagingService()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ParticipantHealthReportCollector getHealthReportCollector()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public InstanceType getInstanceType()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public PropertyStore<ZNRecord> getPropertyStore()
    {
      // TODO Auto-generated method stub
      return null;
    }
  }

  public static class MockAccessor implements ClusterDataAccessor
  {
    Map<ClusterPropertyType, Map<String, ZNRecord>> clusterPropertyMap = new HashMap<ClusterPropertyType, Map<String, ZNRecord>>();
    Map<InstancePropertyType, Map<String, ZNRecord>> instancePropertyMap = new HashMap<InstancePropertyType, Map<String, ZNRecord>>();
    Map<InstancePropertyType, Map<String, Map<String, ZNRecord>>> instancePropertyWithSubpathMap = new HashMap<InstancePropertyType, Map<String, Map<String, ZNRecord>>>();
    Map<ControllerPropertyType, Map<String, ZNRecord>> controllerPropertyMap = new HashMap<ControllerPropertyType, Map<String, ZNRecord>>();
    Map<ControllerPropertyType, Map<String, Map<String, ZNRecord>>> controllerPropertyWithSubpathMap = new HashMap<ControllerPropertyType, Map<String, Map<String, ZNRecord>>>();

    @Override
    public void setClusterProperty(ClusterPropertyType clusterProperty,
        String key, ZNRecord value)
    {
      if (!clusterPropertyMap.containsKey(clusterProperty))
      {
        clusterPropertyMap
            .put(clusterProperty, new HashMap<String, ZNRecord>());
      }
      clusterPropertyMap.get(clusterProperty).put(key, value);
    }

    @Override
    public void removeClusterProperty(ClusterPropertyType clusterProperty,
        String key)
    {
      clusterPropertyMap.remove(key);

    }

    @Override
    public void setClusterProperty(ClusterPropertyType clusterProperty,
        String key, ZNRecord value, CreateMode mode)
    {
      setClusterProperty(clusterProperty, key, value);
    }

    @Override
    public void updateClusterProperty(ClusterPropertyType clusterProperty,
        String key, ZNRecord value)
    {
      if (!clusterPropertyMap.containsKey(clusterProperty))
      {
        clusterPropertyMap
            .put(clusterProperty, new HashMap<String, ZNRecord>());
      }
      clusterPropertyMap.get(clusterProperty).put(key, value);

    }

    @Override
    public ZNRecord getClusterProperty(ClusterPropertyType clusterProperty,
        String key)
    {
      if (!clusterPropertyMap.containsKey(clusterProperty))
      {
        clusterPropertyMap
            .put(clusterProperty, new HashMap<String, ZNRecord>());
      }
      return clusterPropertyMap.get(clusterProperty).get(key);
    }

    @Override
    public List<ZNRecord> getClusterPropertyList(
        ClusterPropertyType clusterProperty)
    {
      if (clusterPropertyMap.containsKey(clusterProperty))
      {
        return ZNRecordUtil.convertMapToList(clusterPropertyMap
            .get(clusterProperty));
      }
      return Collections.emptyList();
    }

    @Override
    public void setInstanceProperty(String instanceName,
        InstancePropertyType clusterProperty, String key, ZNRecord value)
    {
      if (!instancePropertyMap.containsKey(clusterProperty))
      {
        instancePropertyMap.put(clusterProperty,
            new HashMap<String, ZNRecord>());
      }
      instancePropertyMap.get(clusterProperty).put(instanceName + "|" + key,
          value);

    }

    @Override
    public ZNRecord getInstanceProperty(String instanceName,
        InstancePropertyType clusterProperty, String key)
    {
      if (!instancePropertyMap.containsKey(clusterProperty))
      {
        instancePropertyMap.put(clusterProperty,
            new HashMap<String, ZNRecord>());
      }
      return instancePropertyMap.get(clusterProperty).get(
          instanceName + "|" + key);
    }

    @Override
    public List<ZNRecord> getInstancePropertyList(String instanceName,
        InstancePropertyType clusterProperty)
    {
      if (!instancePropertyMap.containsKey(clusterProperty))
      {
        instancePropertyMap.put(clusterProperty,
            new HashMap<String, ZNRecord>());
      }
      Map<String, ZNRecord> map = instancePropertyMap.get(clusterProperty);
      Map<String, ZNRecord> newmap = filter(instanceName, map);
      return ZNRecordUtil.convertMapToList(newmap);

    }

    private Map<String, ZNRecord> filter(String prefix,
        Map<String, ZNRecord> map)
    {
      Map<String, ZNRecord> newmap = new HashMap<String, ZNRecord>();
      for (String key : map.keySet())
      {
        if (key.startsWith(prefix))
        {
          newmap.put(key, map.get(key));
        }
      }
      return newmap;
    }

    @Override
    public void removeInstanceProperty(String instanceName,
        InstancePropertyType type, String key)
    {
      instancePropertyMap.remove(instanceName + "|" + key);

    }

    @Override
    public void updateInstanceProperty(String instanceName,
        InstancePropertyType type, String key, ZNRecord value)
    {
      if (!instancePropertyMap.containsKey(type))
      {
        instancePropertyMap.put(type, new HashMap<String, ZNRecord>());
      }
      if (type.mergeOnUpdate)
      {
        ZNRecord current = instancePropertyMap.get(type).get(
            instanceName + "|" + key);
        ZNRecord record = new ZNRecord(value.getId());
        record.merge(current);
        record.merge(value);
        instancePropertyMap.get(type).put(instanceName + "|" + key, record);
      } else
      {
        instancePropertyMap.get(type).put(instanceName + "|" + key, value);
      }
    }

    @Override
    public void setInstanceProperty(String instanceName,
        InstancePropertyType instanceProperty, String subPath, String key,
        ZNRecord value)
    {
      setInstanceProperty(instanceName, instanceProperty, subPath + "|" + key,
          value);
    }

    @Override
    public void updateInstanceProperty(String instanceName,
        InstancePropertyType instanceProperty, String subPath, String key,
        ZNRecord value)
    {
      updateInstanceProperty(instanceName, instanceProperty, subPath + "|"
          + key, value);

    }

    @Override
    public List<ZNRecord> getInstancePropertyList(String instanceName,
        String subPath, InstancePropertyType instanceProperty)
    {
      if (!instancePropertyMap.containsKey(instanceProperty))
      {
        instancePropertyMap.put(instanceProperty,
            new HashMap<String, ZNRecord>());
      }
      Map<String, ZNRecord> map = instancePropertyMap.get(instanceProperty);
      Map<String, ZNRecord> newmap = filter(instanceName + "|" + subPath, map);
      return ZNRecordUtil.convertMapToList(newmap);
    }

    @Override
    public ZNRecord getInstanceProperty(String instanceName,
        InstancePropertyType instanceProperty, String subPath, String key)
    {
      return getInstanceProperty(instanceName, instanceProperty, subPath + "|" + key);
    }

    @Override
    public List<String> getInstancePropertySubPaths(String instanceName,
        InstancePropertyType instanceProperty)
    {
      if (!instancePropertyMap.containsKey(instanceProperty))
      {
        instancePropertyMap.put(instanceProperty,
            new HashMap<String, ZNRecord>());
      }
      Map<String, ZNRecord> map = instancePropertyMap.get(instanceProperty);
      List<String> subPaths = new ArrayList<String>();
      for(String key:map.keySet()){
        String[] split = key.split("\\|");
        if(split.length==3){
          subPaths.add(split[1]);
        }
      }
      return subPaths;
    }

    @Override
    public void substractInstanceProperty(String instanceName,
        InstancePropertyType instanceProperty, String subPath, String key,
        ZNRecord value)
    {
      
    }

    @Override
    public void createControllerProperty(
        ControllerPropertyType controllerProperty, ZNRecord value,
        CreateMode mode)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void removeControllerProperty(
        ControllerPropertyType controllerProperty)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void setControllerProperty(
        ControllerPropertyType controllerProperty, ZNRecord value,
        CreateMode mode)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public ZNRecord getControllerProperty(
        ControllerPropertyType controllerProperty)
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public PropertyStore<ZNRecord> getStore()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void removeControllerProperty(ControllerPropertyType messages,
        String id)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void setControllerProperty(
        ControllerPropertyType controllerProperty, String subPath,
        ZNRecord value, CreateMode mode)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public ZNRecord getControllerProperty(
        ControllerPropertyType controllerProperty, String subPath)
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean setProperty(PropertyType type, ZNRecord value,
        String... keys)
    {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean updateProperty(PropertyType type, ZNRecord value,
        String... keys)
    {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public ZNRecord getProperty(PropertyType type, String... keys)
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean removeProperty(PropertyType type, String... keys)
    {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public List<String> getChildNames(PropertyType type, String... keys)
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<ZNRecord> getChildValues(PropertyType type, String... keys)
    {
      // TODO Auto-generated method stub
      return null;
    }
  }
}
