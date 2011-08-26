package com.linkedin.clustermanager.monitoring.mbeans;

import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.monitoring.ClusterManagerContollerMonitor;

public class ResourceGroupMonitor implements ResourceGroupMonitorMBean
{
  int _numOfResourceKeys;
  int _numOfResourceKeysInExternalView;
  int _numOfErrorResourceKeys;
  int _externalViewIdealStateDiff;
  private static final Logger LOG = Logger.getLogger(ClusterManagerContollerMonitor.class);

  
  public ResourceGroupMonitor()
  {
  }
  @Override
  public long getNumberOfResourceKeys()
  {
    return _numOfResourceKeys;
  }

  @Override
  public long getNumberOfErrorResouceKeys()
  {
    return _numOfErrorResourceKeys;
  }

  @Override
  public long getDifferenceNumberWithIdealState()
  {
    return _externalViewIdealStateDiff;
  }
  
  public void onExternalViewChange(ZNRecord externalView, NotificationContext changeContext)
  {
    if(externalView == null)
    {
      LOG.warn(" external view is null");
      return;
    }
    String resourceGroup = externalView.getId();
    ClusterDataAccessor accessor = changeContext.getManager().getDataAccessor();
    ZNRecord idealState = null;
    
    try
    {
      idealState = accessor.getClusterProperty(ClusterPropertyType.IDEALSTATES, resourceGroup);
    }
    catch(Exception e)
    {
      // ideal state is gone. Should report 0.
      LOG.warn("ideal state is null for "+resourceGroup, e);
      _numOfErrorResourceKeys = 0;
      _externalViewIdealStateDiff = 0;
      _numOfResourceKeysInExternalView = 0;
      return;
    }
    if(idealState == null)
    {
      LOG.warn("ideal state is null for "+resourceGroup);
      _numOfErrorResourceKeys = 0;
      _externalViewIdealStateDiff = 0;
      _numOfResourceKeysInExternalView = 0;
      return;
    }
    
    assert(resourceGroup.equals(idealState.getId()));
    
    int numOfErrorResourceKeys = 0;
    int numOfDiff = 0;
    
    if(_numOfResourceKeys == 0)
    {
      _numOfResourceKeys = idealState.getMapFields().size();
    }
    
    for(String resourceKey : idealState.getMapFields().keySet())
    {
      Map<String, String> idealRecord = idealState.getMapField(resourceKey);
      Map<String, String> externalViewRecord = externalView.getMapField(resourceKey);
      
      if(externalViewRecord == null)
      {
        numOfDiff += idealRecord.size();
        continue;
      }
      for(String host : idealRecord.keySet())
      {
        if(!externalViewRecord.containsKey(host) || 
           !externalViewRecord.get(host).equals(idealRecord.get(host)))
        {
          numOfDiff++;
        }
      }
      
      for(String host : externalViewRecord.keySet())
      {
        if(externalViewRecord.get(host).equalsIgnoreCase("ERROR"))
        {
          numOfErrorResourceKeys++;
        }
      }
    }
    _numOfErrorResourceKeys = numOfErrorResourceKeys;
    _externalViewIdealStateDiff = numOfDiff;
    _numOfResourceKeysInExternalView = externalView.getMapFields().size();
  }
  
  public long getNumberOfResourceKeysInExternalView()
  {
    return _numOfResourceKeysInExternalView;
  }
}
