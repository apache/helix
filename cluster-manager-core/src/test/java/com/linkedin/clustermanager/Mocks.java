package com.linkedin.clustermanager;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.healthcheck.HealthReportProvider;
import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.clustermanager.messaging.AsyncCallback;
import com.linkedin.clustermanager.messaging.handling.CMTaskExecutor;
import com.linkedin.clustermanager.messaging.handling.CMTaskResult;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelInfo;
import com.linkedin.clustermanager.participant.statemachine.Transition;
import com.linkedin.clustermanager.store.PropertyStore;

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
    MockAccessor accessor;

    private final String _clusterName;
    private final String sessionId;
    String _instanceName;
    ClusterMessagingService _msgSvc ;
    public MockManager()
    {
      this("testCluster-" + Math.random() * 10000 % 999);
    }

    public MockManager(String clusterName)
    {
      _clusterName = clusterName;
      accessor = new MockAccessor(clusterName);
      sessionId =  UUID.randomUUID().toString();
      _instanceName = "testInstanceName";
      _msgSvc = new MockClusterMessagingService();
    }

    @Override
    public void disconnect()
    {

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
      return _instanceName;
    }

    @Override
    public void connect()
    {
      // TODO Auto-generated method stub

    }

    @Override
    public String getSessionId()
    {
      return sessionId;
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
      return _msgSvc;
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
    private final String _clusterName;
    Map<String, ZNRecord> data = new HashMap<String, ZNRecord>();

    public MockAccessor()
    {
      this("testCluster-" + Math.random() * 10000 % 999);
    }

    public MockAccessor(String clusterName)
    {
      _clusterName = clusterName;
    }

    Map<String, ZNRecord> map = new HashMap<String, ZNRecord>();

    @Override
    public boolean setProperty(PropertyType type, ZNRecord value,
        String... keys)
    {
      String path = PropertyPathConfig.getPath(type, _clusterName, keys);
      data.put(path, value);
      return true;
    }

    @Override
    public boolean updateProperty(PropertyType type, ZNRecord value,
        String... keys)
    {
      String path = PropertyPathConfig.getPath(type, _clusterName, keys);
      if (type.updateOnlyOnExists)
      {
        if (data.containsKey(path))
        {
          if (type.mergeOnUpdate)
          {
            ZNRecord znRecord = new ZNRecord(data.get(path));
            znRecord.merge(value);
            data.put(path, znRecord);
          } else
          {
            data.put(path, value);
          }
        }
      } else
      {
        if (type.mergeOnUpdate)
        {
          if (data.containsKey(path))
          {
            ZNRecord znRecord = new ZNRecord(data.get(path));
            znRecord.merge(value);
            data.put(path, znRecord);
          } else
          {
            data.put(path, value);
          }
        } else
        {
          data.put(path, value);
        }
      }

      return true;
    }

    @Override
    public ZNRecord getProperty(PropertyType type, String... keys)
    {
      String path = PropertyPathConfig.getPath(type, _clusterName, keys);
      return data.get(path);
    }

    @Override
    public boolean removeProperty(PropertyType type, String... keys)
    {
      String path = PropertyPathConfig.getPath(type, _clusterName, keys);
      data.remove(path);
      return true;
    }

    @Override
    public List<String> getChildNames(PropertyType type, String... keys)
    {
      List<String> child = new ArrayList<String>();
      String path = PropertyPathConfig.getPath(type, _clusterName, keys);
      for (String key : data.keySet())
      {
        if (key.startsWith(path))
        {
          String[] keySplit = key.split("\\/");
          String[] pathSplit = path.split("\\/");
          if (keySplit.length > pathSplit.length)
          {
            child.add(keySplit[pathSplit.length + 1]);
          }
        }
      }
      return child;
    }

    @Override
    public List<ZNRecord> getChildValues(PropertyType type, String... keys)
    {
      List<ZNRecord> child = new ArrayList<ZNRecord>();
      String path = PropertyPathConfig.getPath(type, _clusterName, keys);
      for (String key : data.keySet())
      {
        if (key.startsWith(path))
        {
          String[] keySplit = key.split("\\/");
          String[] pathSplit = path.split("\\/");
          if (keySplit.length - pathSplit.length==1)
          {
            child.add(data.get(key));
          }else{
            System.out.println("keySplit:"+ Arrays.toString(keySplit) );
            System.out.println("pathSplit:"+ Arrays.toString(pathSplit) );
          }
        }
      }
      return child;
    }

    @Override
    public PropertyStore<ZNRecord> getStore()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public <T extends ZNRecordAndStat> void refreshChildValues(Map<String, T> childValues,
        Class<T> clazz, PropertyType type, String... keys)
    {
      if (childValues == null)
      {
        throw new IllegalArgumentException("should provide non-null map that holds old child records "
            + " (empty map if no old values)");
      }

      childValues.clear();
      List<ZNRecord> childRecords = this.getChildValues(type, keys);
      for (ZNRecord record : childRecords)
      {
        try
        {
          Constructor<T> constructor = clazz.getConstructor(new Class[] { ZNRecord.class, Stat.class });
          childValues.put(record.getId(), constructor.newInstance(record, null));
        }
        catch (Exception e)
        {
          // logger.error("Error creating an Object of type:" + clazz.getCanonicalName(), e);
          e.printStackTrace();
        }
      }
    }
  }

  public static class MockHealthReportProvider extends HealthReportProvider
  {

    @Override
    public Map<String, String> getRecentHealthReport()
    {
      // TODO Auto-generated method stub
      return null;
    }

	@Override
	public void resetStats() {
		// TODO Auto-generated method stub

	}

  }

  public static class MockClusterMessagingService implements ClusterMessagingService
  {

    @Override
    public int send(Criteria recipientCriteria, Message message)
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int send(Criteria receipientCriteria, Message message,
        AsyncCallback callbackOnReply, int timeOut)
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int sendAndWait(Criteria receipientCriteria, Message message,
        AsyncCallback callbackOnReply, int timeOut)
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void registerMessageHandlerFactory(String type,
        MessageHandlerFactory factory)
    {
      // TODO Auto-generated method stub

    }

  }
}
