package com.linkedin.clustermanager.participant;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManagementService;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterMessagingService;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.ControllerChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.HealthStateChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.clustermanager.store.PropertyStore;

public class MockZkClusterManager implements ClusterManager
{
  private final ZKDataAccessor _accessor;
  private final String _instanceName;
  private final String _clusterName;
  private final InstanceType _type;

  public MockZkClusterManager(String clusterName, String instanceName, InstanceType type, ZkClient zkClient)
  {
    _instanceName = instanceName;
    _clusterName = clusterName;
    _type = type;
    _accessor = new ZKDataAccessor(clusterName, zkClient);
  }
  
  @Override
  public void connect() throws Exception
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean isConnected()
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void disconnect()
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addConfigChangeListener(ConfigChangeListener listener) throws Exception
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addMessageListener(MessageListener listener, String instanceName) throws Exception
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
                                            String instanceName,
                                            String sessionId) throws Exception
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception
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
  public ClusterDataAccessor getDataAccessor()
  {
    return _accessor;
  }

  @Override
  public String getClusterName()
  {
    return _clusterName;
  }

  @Override
  public String getInstanceName()
  {
    return _instanceName;
  }

  @Override
  public String getSessionId()
  {
    // TODO Auto-generated method stub
    return null;
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
  public ClusterManagementService getClusterManagmentTool()
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
    return _type;
  }

@Override
public void addHealthStateChangeListener(HealthStateChangeListener listener,
		String instanceName) throws Exception {
	// TODO Auto-generated method stub
	
}

}
