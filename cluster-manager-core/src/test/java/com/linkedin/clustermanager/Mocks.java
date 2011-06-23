package com.linkedin.clustermanager;

import java.util.List;
import java.util.concurrent.Future;

import com.linkedin.clustermanager.core.ClusterDataAccessor;
import com.linkedin.clustermanager.core.ClusterManager;
import com.linkedin.clustermanager.core.NotificationContext;
import com.linkedin.clustermanager.core.listeners.ConfigChangeListener;
import com.linkedin.clustermanager.core.listeners.CurrentStateChangeListener;
import com.linkedin.clustermanager.core.listeners.ExternalViewChangeListener;
import com.linkedin.clustermanager.core.listeners.IdealStateChangeListener;
import com.linkedin.clustermanager.core.listeners.LiveInstanceChangeListener;
import com.linkedin.clustermanager.core.listeners.MessageListener;
import com.linkedin.clustermanager.model.ClusterView;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ZNRecord;
import com.linkedin.clustermanager.statemachine.CMTaskExecutor;
import com.linkedin.clustermanager.statemachine.CMTaskResult;
import com.linkedin.clustermanager.statemachine.StateModel;
import com.linkedin.clustermanager.statemachine.StateModelInfo;
import com.linkedin.clustermanager.statemachine.Transition;

public class Mocks
{
    public static class MockStateModel extends StateModel
    {
        boolean stateModelInvoked = false;

        public void onBecomeMasterFromSlave(Message msg,
                NotificationContext context)
        {
            stateModelInvoked = true;
        }
        public void onBecomeSlaveFromOffline(Message msg,
                NotificationContext context)
        {
            stateModelInvoked = true;
        }
    }
    @StateModelInfo(states="{'OFFLINE','SLAVE','MASTER'}",initialState="OFFINE")
    public static class MockStateModelAnnotated extends StateModel
    {
    	boolean stateModelInvoked = false;
    	
    	@Transition(from="SLAVE", to="MASTER")
    	public void slaveToMaster(Message msg,
    			NotificationContext context)
    	{
    		stateModelInvoked = true;
    	}
    	@Transition(from="OFFLINE", to="SLAVEa")
    	public void offlineToSlave(Message msg,
    			NotificationContext context)
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
        public void addIdealStateChangeListener(
                IdealStateChangeListener listener) throws Exception
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
        public void addMessageListener(MessageListener listener,
                String instanceName)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void addCurrentStateChangeListener(
                CurrentStateChangeListener listener, String instanceName)
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
        public ClusterDataAccessor getClient()
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
        public void start()
        {
            // TODO Auto-generated method stub

        }

        @Override
        public String getSessionId()
        {
            // TODO Auto-generated method stub
            return null;
        }

    }

    public static class MockAccessor implements ClusterDataAccessor
    {

      @Override
      public void setClusterProperty(ClusterPropertyType clusterProperty,
          String key, ZNRecord value)
      {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void updateClusterProperty(ClusterPropertyType clusterProperty,
          String key, ZNRecord value)
      {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void setClusterPropertyList(ClusterPropertyType clusterProperty,
          List<ZNRecord> values)
      {
        // TODO Auto-generated method stub
        
      }

      @Override
      public ZNRecord getClusterProperty(ClusterPropertyType clusterProperty,
          String key)
      {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public List<ZNRecord> getClusterPropertyList(
          ClusterPropertyType clusterProperty)
      {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public void setInstanceProperty(String instanceName,
          InstancePropertyType clusterProperty, String key, ZNRecord value)
      {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void setInstancePropertyList(String instanceName,
          InstancePropertyType clusterProperty, List<ZNRecord> values)
      {
        // TODO Auto-generated method stub
        
      }

      @Override
      public ZNRecord getInstanceProperty(String instanceName,
          InstancePropertyType clusterProperty, String key)
      {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public List<ZNRecord> getInstancePropertyList(String instanceName,
          InstancePropertyType clusterProperty)
      {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public void removeInstanceProperty(String instanceName,
          InstancePropertyType type, String key)
      {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void updateInstanceProperty(String instanceName,
          InstancePropertyType type, String hey, ZNRecord value)
      {
        // TODO Auto-generated method stub
        
      }

      @Override
      public ClusterView getClusterView()
      {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public void setEphemeralClusterProperty(
          ClusterPropertyType clusterProperty, String key, ZNRecord value)
      {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void removeClusterProperty(ClusterPropertyType clusterProperty,
          String key)
      {
        // TODO Auto-generated method stub
        
      }

        
    }
}
