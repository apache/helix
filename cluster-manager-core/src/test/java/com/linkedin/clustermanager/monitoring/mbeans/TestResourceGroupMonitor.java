package com.linkedin.clustermanager.monitoring.mbeans;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.Mocks;
import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;

public class TestResourceGroupMonitor
{
  
  String _clusterName = "Test-cluster";
  String _dbName = "TestDB";
    int _replicas = 3;
    int _partitions = 50;

  
  class MockClusterManager extends Mocks.MockManager
  {
    class MockDataAccessor extends Mocks.MockAccessor
    {
      @Override
      public List<ZNRecord> getChildValues(PropertyType type, String... keys)
      {
        List<ZNRecord> result = new ArrayList<ZNRecord>();
        if(type == PropertyType.EXTERNALVIEW)
        {
          result.add(_externalView);
          return result;
        }
        else if(type == PropertyType.LIVEINSTANCES)
        {
          return _liveInstances;
        }
        
        return result;
      }
      
      @Override
      public ZNRecord getProperty(PropertyType type, String... keys)
      {
        if(type == PropertyType.EXTERNALVIEW)
        {
          return _externalView;
        }
        else if(type == PropertyType.IDEALSTATES)
        {
          return _externalView;
        }
        return null;
      }
    }
    
    ClusterDataAccessor _accessor = new MockDataAccessor();
    ZNRecord _externalView;
    List<String> _instances;
    List<ZNRecord> _liveInstances;
    String _db = "DB";
    public MockClusterManager()
    {
      _liveInstances = new ArrayList<ZNRecord>();
      _instances = new ArrayList<String>();
      for(int i = 0;i<5; i++)
      {
        String instance = "localhost_"+(12918+i);
        _instances.add(instance);
        ZNRecord metaData = new ZNRecord(instance);
        metaData.setSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString(),
            UUID.randomUUID().toString());
        
      }
      _externalView = IdealStateCalculatorForStorageNode.calculateIdealState(
          _instances, _partitions, _replicas, _dbName, "MASTER", "SLAVE");
      
      
    }
    public ClusterDataAccessor getDataAccessor()
    {
      return _accessor;
    }
    
  }
  @Test(groups={ "unitTest" })
  public void TestReportData()
  {
    
    MockClusterManager manager = new MockClusterManager();
    ResourceGroupMonitor monitor = new ResourceGroupMonitor(_clusterName, _dbName);
    
    ZNRecord externalView = new ZNRecord(manager.getDataAccessor().getProperty(PropertyType.EXTERNALVIEW, _dbName));
    monitor.onExternalViewChange(externalView, manager);
    
    AssertJUnit.assertEquals(monitor.getDifferenceWithIdealStateGauge(), 0);
    AssertJUnit.assertEquals(monitor.getErrorResouceKeyGauge(), 0);
    AssertJUnit.assertEquals(monitor.getExternalViewResourceKeyGauge(), _partitions);
    AssertJUnit.assertEquals(monitor.getResourceKeyGauge(), _partitions);
    monitor.getBeanName();
    
    int n = 4;
    for(int i = 0;i<n; i++)
    {
      Map<String ,String> map = externalView.getMapField(_dbName+"_"+3*i);
      String key = map.keySet().toArray()[0].toString();
      map.put(key, "ERROR");
      externalView.setMapField(_dbName+"_"+3*i, map);
    }
    
    
    monitor.onExternalViewChange(externalView, manager);
    AssertJUnit.assertEquals(monitor.getDifferenceWithIdealStateGauge(), 0);
    AssertJUnit.assertEquals(monitor.getErrorResouceKeyGauge(), n);
    AssertJUnit.assertEquals(monitor.getExternalViewResourceKeyGauge(), _partitions);
    AssertJUnit.assertEquals(monitor.getResourceKeyGauge(), _partitions);
    
    n = 5;
    for(int i = 0;i<n; i++)
    {
      externalView.getMapFields().remove(_dbName+"_"+4*i);
    }
    
    monitor.onExternalViewChange(externalView, manager);
    AssertJUnit.assertEquals(monitor.getDifferenceWithIdealStateGauge(), n*(_replicas + 1));
    AssertJUnit.assertEquals(monitor.getErrorResouceKeyGauge(), 3);
    AssertJUnit.assertEquals(monitor.getExternalViewResourceKeyGauge(), _partitions - n);
    AssertJUnit.assertEquals(monitor.getResourceKeyGauge(), _partitions);
  }
}
