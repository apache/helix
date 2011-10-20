package com.linkedin.clustermanager;

import java.util.List;
import java.util.concurrent.Future;

import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.clustermanager.messaging.handling.CMTaskExecutor;
import com.linkedin.clustermanager.messaging.handling.CMTaskResult;
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

    @Override
    public PropertyStore<ZNRecord> getStore()
    {
      // TODO Auto-generated method stub
      return null;
    }
  }
}
