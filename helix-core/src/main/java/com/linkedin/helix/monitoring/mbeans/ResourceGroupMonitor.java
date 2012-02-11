package com.linkedin.helix.monitoring.mbeans;

import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixAgent;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;

public class ResourceGroupMonitor implements ResourceGroupMonitorMBean
{
  int _numOfResourceKeys;
  int _numOfResourceKeysInExternalView;
  int _numOfErrorResourceKeys;
  int _externalViewIdealStateDiff;
  private static final Logger LOG = Logger.getLogger(ClusterStatusMonitor.class);

  String _resourceGroup, _clusterName;
  public ResourceGroupMonitor(String clusterName, String resourceGroup)
  {
    _clusterName = clusterName;
    _resourceGroup = resourceGroup;
  }

  @Override
  public long getResourceKeyGauge()
  {
    return _numOfResourceKeys;
  }

  @Override
  public long getErrorResouceKeyGauge()
  {
    return _numOfErrorResourceKeys;
  }

  @Override
  public long getDifferenceWithIdealStateGauge()
  {
    return _externalViewIdealStateDiff;
  }

  public void onExternalViewChange(ExternalView externalView, HelixAgent manager)
  {
    if(externalView == null)
    {
      LOG.warn("external view is null");
      return;
    }
    String resourceGroup = externalView.getId();
    DataAccessor accessor = manager.getDataAccessor();
    IdealState idealState = null;

    try
    {
      idealState = accessor.getProperty(IdealState.class, PropertyType.IDEALSTATES, resourceGroup);
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
      _numOfResourceKeys = idealState.getRecord().getMapFields().size();
    }

    // TODO fix this; IdealState shall have either map fields (CUSTOM mode)
    //  or list fields (AUDO mode)
    for(String resourceKey : idealState.getRecord().getMapFields().keySet())
    {
      Map<String, String> idealRecord = idealState.getInstanceStateMap(resourceKey);
      Map<String, String> externalViewRecord = externalView.getStateMap(resourceKey);

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
//    System.out.println(_numOfErrorResourceKeys + " "
//        + _externalViewIdealStateDiff + " " + _numOfResourceKeysInExternalView);
    _numOfErrorResourceKeys = numOfErrorResourceKeys;
    _externalViewIdealStateDiff = numOfDiff;
    _numOfResourceKeysInExternalView = externalView.getResourceKeys().size();
  }

  @Override
  public long getExternalViewResourceKeyGauge()
  {
    return _numOfResourceKeysInExternalView;
  }

  public String getBeanName()
  {
    return _clusterName+" "+_resourceGroup;
  }
}
