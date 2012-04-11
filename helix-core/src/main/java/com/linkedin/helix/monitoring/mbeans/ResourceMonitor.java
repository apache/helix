package com.linkedin.helix.monitoring.mbeans;

import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;

public class ResourceMonitor implements ResourceMonitorMBean
{
  int _numOfPartitions;
  int _numOfPartitionsInExternalView;
  int _numOfErrorPartitions;
  int _externalViewIdealStateDiff;
  private static final Logger LOG = Logger.getLogger(ResourceMonitor.class);

  String _resourceName, _clusterName;
  public ResourceMonitor(String clusterName, String resourceName)
  {
    _clusterName = clusterName;
    _resourceName = resourceName;
  }

  @Override
  public long getPartitionGauge()
  {
    return _numOfPartitions;
  }

  @Override
  public long getErrorPartitionGauge()
  {
    return _numOfErrorPartitions;
  }

  @Override
  public long getDifferenceWithIdealStateGauge()
  {
    return _externalViewIdealStateDiff;
  }
  
  public String getSensorName()
  {
    return "ResourceStatus" + "_" + _clusterName + "_" + _resourceName;
  }

  public void updateExternalView(ExternalView externalView, IdealState idealState)
  {
    if(externalView == null)
    {
      LOG.warn("external view is null");
      return;
    }
    String resourceName = externalView.getId();

    if(idealState == null)
    {
      LOG.warn("ideal state is null for "+resourceName);
      _numOfErrorPartitions = 0;
      _externalViewIdealStateDiff = 0;
      _numOfPartitionsInExternalView = 0;
      return;
    }

    assert(resourceName.equals(idealState.getId()));

    int numOfErrorPartitions = 0;
    int numOfDiff = 0;

    if(_numOfPartitions == 0)
    {
      _numOfPartitions = idealState.getRecord().getMapFields().size();
    }

    // TODO fix this; IdealState shall have either map fields (CUSTOM mode)
    //  or list fields (AUDO mode)
    for(String partitionName : idealState.getRecord().getMapFields().keySet())
    {
      Map<String, String> idealRecord = idealState.getInstanceStateMap(partitionName);
      Map<String, String> externalViewRecord = externalView.getStateMap(partitionName);

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
          numOfErrorPartitions++;
        }
      }
    }
    _numOfErrorPartitions = numOfErrorPartitions;
    _externalViewIdealStateDiff = numOfDiff;
    _numOfPartitionsInExternalView = externalView.getPartitionSet().size();
  }

  @Override
  public long getExternalViewPartitionGauge()
  {
    return _numOfPartitionsInExternalView;
  }

  public String getBeanName()
  {
    return _clusterName+" "+_resourceName;
  }
}
