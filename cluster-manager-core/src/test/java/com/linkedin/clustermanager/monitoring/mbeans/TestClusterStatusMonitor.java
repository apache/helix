package com.linkedin.clustermanager.monitoring.mbeans;

import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.testng.annotations.Test;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.Mocks;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;


public class TestClusterStatusMonitor
{
  List<String> _instances;
  List<ZNRecord> _liveInstances;
  String _db = "DB";
  String _db2 = "TestDB";
  int _replicas = 3;
  int _partitions = 50;
  ZNRecord _externalView, _externalView2; 
  
  class MockDataAccessor extends Mocks.MockAccessor
  {
    public MockDataAccessor()
    {
      _instances = new ArrayList<String>();
      for(int i = 0;i < 5; i++)
      {
        String instance = "localhost_"+(12918+i);
        _instances.add(instance);
      }
      ZNRecord externalView = IdealStateCalculatorForStorageNode.calculateIdealState(
          _instances, _partitions, _replicas, _db, "MASTER", "SLAVE");
      
      ZNRecord externalView2 = IdealStateCalculatorForStorageNode.calculateIdealState(
          _instances, 80, 2, _db2, "MASTER", "SLAVE");
      
    }
    public ZNRecord getProperty(PropertyType type, String resourceGroup)
    {
      if(type == PropertyType.IDEALSTATES || type == PropertyType.EXTERNALVIEW)
      {
        if(resourceGroup.equals(_db))
        {
          return _externalView;
        }
        else if(resourceGroup.equals(_db2))
        {
          return _externalView2;
        }
      }
      return null;
    }
  }
  class MockClusterManager extends Mocks.MockManager
  {
    MockDataAccessor _accessor = new MockDataAccessor();
    
    public ClusterDataAccessor getDataAccessor()
    {
      return _accessor;
    }
    
  }
  @Test(groups={ "unitTest" })
  public void TestReportData()
  {
    List<String> _instances;
    List<ZNRecord> _liveInstances = new ArrayList<ZNRecord>();
    String _db = "DB";
    int _replicas = 3;
    int _partitions = 50;
    
    _instances = new ArrayList<String>();
    for(int i = 0;i < 5; i++)
    {
      String instance = "localhost_"+(12918+i);
      _instances.add(instance);
      ZNRecord metaData = new ZNRecord(instance);
      metaData.setSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString(),
          UUID.randomUUID().toString());
      _liveInstances.add(metaData);
    }
    ZNRecord externalView = IdealStateCalculatorForStorageNode.calculateIdealState(
        _instances, _partitions, _replicas, _db, "MASTER", "SLAVE");
    
    ZNRecord externalView2 = IdealStateCalculatorForStorageNode.calculateIdealState(
        _instances, 80, 2, "TestDB", "MASTER", "SLAVE");
    
    List<ZNRecord> externalViews = new ArrayList<ZNRecord>();
    externalViews.add(externalView);
    externalViews.add(externalView2);
    
    ClusterStatusMonitor monitor = new ClusterStatusMonitor("cluster1", _instances.size());
    MockClusterManager manager = new MockClusterManager();
    NotificationContext context = new NotificationContext(manager);
    
    monitor.onExternalViewChange(externalViews, context);
    
    monitor.onLiveInstanceChange(_liveInstances, context);
    
  }
}
