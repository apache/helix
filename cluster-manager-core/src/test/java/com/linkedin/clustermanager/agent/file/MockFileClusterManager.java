package com.linkedin.clustermanager.agent.file;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManagementService;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterMessagingService;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.ControllerChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.clustermanager.store.PropertyStore;
import com.linkedin.clustermanager.store.file.FilePropertyStore;

public class MockFileClusterManager implements ClusterManager
{
  private final FileBasedDataAccessor _accessor;
  private final String _instanceName;
  private final String _clusterName;
  private final InstanceType _type;

  public MockFileClusterManager(String clusterName, String instanceName, InstanceType type, 
                              FilePropertyStore<ZNRecord> store)
  {
    _instanceName = instanceName;
    _clusterName = clusterName;
    _type = type;
    _accessor = new FileBasedDataAccessor(store, clusterName);
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

}
