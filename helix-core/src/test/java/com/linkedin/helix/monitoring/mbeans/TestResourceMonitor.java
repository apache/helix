package com.linkedin.helix.monitoring.mbeans;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.Mocks;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.LiveInstance.LiveInstanceProperty;
import com.linkedin.helix.monitoring.mbeans.ResourceMonitor;
import com.linkedin.helix.tools.IdealStateCalculatorForStorageNode;

public class TestResourceMonitor
{
  String _clusterName = "Test-cluster";
  String _dbName = "TestDB";
  int _replicas = 3;
  int _partitions = 50;

  class MockHelixManager extends Mocks.MockManager
  {
    class MockDataAccessor extends Mocks.MockAccessor
    {
      @Override
      public List<ZNRecord> getChildValues(PropertyType type, String... keys)
      {
        List<ZNRecord> result = new ArrayList<ZNRecord>();
        if (type == PropertyType.EXTERNALVIEW)
        {
          result.add(_externalView);
          return result;
        }
        else if (type == PropertyType.LIVEINSTANCES)
        {
          return _liveInstances;
        }

        return result;
      }

      @Override
      public ZNRecord getProperty(PropertyType type, String... keys)
      {
        if (type == PropertyType.EXTERNALVIEW)
        {
          return _externalView;
        }
        else if (type == PropertyType.IDEALSTATES)
        {
          return _idealState;
        }
        return null;
      }
    }

    DataAccessor _accessor = new MockDataAccessor();
    ZNRecord _idealState;
    ZNRecord _externalView;
    List<String> _instances;
    List<ZNRecord> _liveInstances;
    String _db = "DB";

    public MockHelixManager()
    {
      _liveInstances = new ArrayList<ZNRecord>();
      _instances = new ArrayList<String>();
      for(int i = 0;i<5; i++)
      {
        String instance = "localhost_"+(12918+i);
        _instances.add(instance);
        ZNRecord metaData = new ZNRecord(instance);
        metaData.setSimpleField(LiveInstanceProperty.SESSION_ID.toString(),
            UUID.randomUUID().toString());

      }
      _idealState= IdealStateCalculatorForStorageNode.calculateIdealState(_instances,
                                                                          _partitions,
                                                                          _replicas,
                                                                          _dbName,
                                                                          "MASTER",
                                                                          "SLAVE");
      _externalView = new ZNRecord(_idealState);
    }

    @Override
    public DataAccessor getDataAccessor()
    {
      return _accessor;
    }

  }

  @Test()
  public void TestReportData()
  {
    MockHelixManager manager = new MockHelixManager();
    ResourceMonitor monitor = new ResourceMonitor(_clusterName, _dbName);

    ExternalView externalView = new ExternalView(
        manager.getDataAccessor().getProperty(PropertyType.EXTERNALVIEW,_dbName));

    monitor.onExternalViewChange(externalView, manager);

    AssertJUnit.assertEquals(monitor.getDifferenceWithIdealStateGauge(), 0);
    AssertJUnit.assertEquals(monitor.getErrorPartitionGauge(), 0);
    AssertJUnit.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions);
    AssertJUnit.assertEquals(monitor.getPartitionGauge(), _partitions);
    monitor.getBeanName();

    int n = 4;
    for (int i = 0; i < n; i++)
    {
      Map<String, String> map = externalView.getStateMap(_dbName + "_" + 3 * i);
      String key = map.keySet().toArray()[0].toString();
      map.put(key, "ERROR");
      externalView.setStateMap(_dbName + "_" + 3 * i, map);
    }

    monitor.onExternalViewChange(externalView, manager);
    AssertJUnit.assertEquals(monitor.getDifferenceWithIdealStateGauge(), 0);
    AssertJUnit.assertEquals(monitor.getErrorPartitionGauge(), n);
    AssertJUnit.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions);
    AssertJUnit.assertEquals(monitor.getPartitionGauge(), _partitions);

    n = 5;
    for (int i = 0; i < n; i++)
    {
      externalView.getRecord().getMapFields().remove(_dbName + "_" + 4 * i);
    }

    monitor.onExternalViewChange(externalView, manager);
    AssertJUnit.assertEquals(monitor.getDifferenceWithIdealStateGauge(), n
        * (_replicas + 1));
    AssertJUnit.assertEquals(monitor.getErrorPartitionGauge(), 3);
    AssertJUnit.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions - n);
    AssertJUnit.assertEquals(monitor.getPartitionGauge(), _partitions);
  }
}
