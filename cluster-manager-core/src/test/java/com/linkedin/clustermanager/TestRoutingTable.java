package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.Mocks.MockAccessor;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.spectator.RoutingTableProvider;

public class TestRoutingTable
{
  NotificationContext changeContext = null;

  @BeforeTest
  public void setup()
  {

    final String[] array = new String[]
    { "localhost_8900", "localhost_8901" };
    ClusterManager manager = new Mocks.MockManager()
    {
      private MockAccessor _mockAccessor;

      @Override
      public ClusterDataAccessor getDataAccessor()
      {
        if (_mockAccessor == null)
        {
          _mockAccessor = new Mocks.MockAccessor()
          {
            public List<ZNRecord> getClusterPropertyList(
                ClusterPropertyType clusterProperty)
            {
              if (clusterProperty == ClusterPropertyType.CONFIGS)
              {
                List<ZNRecord> configs = new ArrayList<ZNRecord>();
                for (String instanceName : array)
                {
                  ZNRecord config = new ZNRecord(instanceName);
                  String[] splits = instanceName.split("_");
                  config.setSimpleField(
                      CMConstants.ZNAttribute.HOST.toString(), splits[0]);
                  config.setSimpleField(
                      CMConstants.ZNAttribute.PORT.toString(), splits[1]);
                  configs.add(config);
                }
                return configs;
              }
              return Collections.emptyList();
            };
          };
        }
        return _mockAccessor;
      }
    };
    changeContext = new NotificationContext(manager);
  }

  @Test
  public void testNullAndEmpty()
  {

    RoutingTableProvider routingTable = new RoutingTableProvider();
    routingTable.onExternalViewChange(null, changeContext);
    List<ZNRecord> list = Collections.emptyList();
    routingTable.onExternalViewChange(list, changeContext);

  }

  @Test
  public void testSimple()
  {
    List<InstanceConfig> instances;
    RoutingTableProvider routingTable = new RoutingTableProvider();
    List<ZNRecord> externalViewList = new ArrayList<ZNRecord>();
    ZNRecord record = new ZNRecord("TESTDB");
    externalViewList.add(record);
    // one master
    add(record, "TESTDB_0", "localhost_8900", "MASTER");
    routingTable.onExternalViewChange(externalViewList, changeContext);

    instances = routingTable.getInstances("TESTDB", "TESTDB_0", "MASTER");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(), 1);

    // additions
    add(record, "TESTDB_0", "localhost_8901", "MASTER");
    add(record, "TESTDB_1", "localhost_8900", "SLAVE");

    routingTable.onExternalViewChange(externalViewList, changeContext);
    instances = routingTable.getInstances("TESTDB", "TESTDB_0", "MASTER");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(), 2);

    instances = routingTable.getInstances("TESTDB", "TESTDB_1", "SLAVE");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(), 1);

    // updates
    add(record, "TESTDB_0", "localhost_8901", "SLAVE");
    routingTable.onExternalViewChange(externalViewList, changeContext);
    instances = routingTable.getInstances("TESTDB", "TESTDB_0", "SLAVE");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(), 1);
  }

  @Test
  public void testStateUnitGroupDeletion()
  {
    List<InstanceConfig> instances;
    RoutingTableProvider routingTable = new RoutingTableProvider();

    List<ZNRecord> externalViewList = new ArrayList<ZNRecord>();
    ZNRecord record = new ZNRecord("TESTDB");
    externalViewList.add(record);
    // one master
    add(record, "TESTDB_0", "localhost_8900", "MASTER");
    routingTable.onExternalViewChange(externalViewList, changeContext);
    instances = routingTable.getInstances("TESTDB", "TESTDB_0", "MASTER");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(), 1);

    externalViewList.clear();
    routingTable.onExternalViewChange(externalViewList, changeContext);
    instances = routingTable.getInstances("TESTDB", "TESTDB_0", "MASTER");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(), 0);
  }

  @Test
  public void testGetInstanceForAllStateUnits()
  {
    List<InstanceConfig> instancesList;
    Set<InstanceConfig> instancesSet;
    InstanceConfig instancesArray[];
    RoutingTableProvider routingTable = new RoutingTableProvider();
    List<ZNRecord> externalViewList = new ArrayList<ZNRecord>();
    ZNRecord record = new ZNRecord("TESTDB");
    externalViewList.add(record);
    // one master
    add(record, "TESTDB_0", "localhost_8900", "MASTER");
    add(record, "TESTDB_1", "localhost_8900", "MASTER");
    add(record, "TESTDB_2", "localhost_8900", "MASTER");
    add(record, "TESTDB_3", "localhost_8900", "SLAVE");
    add(record, "TESTDB_4", "localhost_8900", "SLAVE");
    add(record, "TESTDB_5", "localhost_8900", "SLAVE");

    add(record, "TESTDB_0", "localhost_8901", "SLAVE");
    add(record, "TESTDB_1", "localhost_8901", "SLAVE");
    add(record, "TESTDB_2", "localhost_8901", "SLAVE");
    add(record, "TESTDB_3", "localhost_8901", "MASTER");
    add(record, "TESTDB_4", "localhost_8901", "MASTER");
    add(record, "TESTDB_5", "localhost_8901", "MASTER");
    routingTable.onExternalViewChange(externalViewList, changeContext);
    instancesList = routingTable.getInstances("TESTDB", "TESTDB_0", "MASTER");
    Assert.assertNotNull(instancesList);
    Assert.assertEquals(instancesList.size(), 1);
    instancesSet = routingTable.getInstances("TESTDB", "MASTER");
    Assert.assertNotNull(instancesSet);
    Assert.assertEquals(instancesSet.size(), 2);
    instancesSet = routingTable.getInstances("TESTDB", "SLAVE");
    Assert.assertNotNull(instancesSet);
    Assert.assertEquals(instancesSet.size(), 2);
    instancesArray = new InstanceConfig[instancesSet.size()];
    instancesSet.toArray(instancesArray);
    Assert.assertEquals(instancesArray[0].getHostName(), "localhost");
    Assert.assertEquals(instancesArray[0].getPort(), "8900");
    Assert.assertEquals(instancesArray[1].getHostName(), "localhost");
    Assert.assertEquals(instancesArray[1].getPort(), "8901");

  }

  @Test
  public void testMultiThread() throws Exception
  {
    final RoutingTableProvider routingTable = new RoutingTableProvider();
    List<ZNRecord> externalViewList = new ArrayList<ZNRecord>();
    ZNRecord record = new ZNRecord("TESTDB");
    externalViewList.add(record);
    for (int i = 0; i < 1000; i++)
    {
      add(record, "TESTDB_" + i, "localhost_8900", "MASTER");
    }
    routingTable.onExternalViewChange(externalViewList, changeContext);
    Callable<Boolean> runnable = new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {

        try
        {
          int count = 0;
          while (count < 100)
          {
            List<InstanceConfig> instancesList = routingTable.getInstances(
                "TESTDB", "TESTDB_0", "MASTER");
            Assert.assertEquals(instancesList.size(), 1);
            // System.out.println(System.currentTimeMillis() + "-->"
            // + instancesList.size());

            Thread.sleep(5);

            count++;
          }
        } catch (InterruptedException e)
        {
          // e.printStackTrace();
        }
        return true;
      }
    };
    ScheduledExecutorService executor = Executors
        .newSingleThreadScheduledExecutor();
    Future<Boolean> submit = executor.submit(runnable);
    int count = 0;
    while (count < 10)
    {
      try
      {
        Thread.sleep(10);
      } catch (InterruptedException e)
      {
        e.printStackTrace();
      }
      routingTable.onExternalViewChange(externalViewList, changeContext);
      count++;
    }

    Boolean result = submit.get(60, TimeUnit.SECONDS);
    Assert.assertEquals(result, Boolean.TRUE);

  }

  private void add(ZNRecord record, String stateUnitKey, String instanceName,
      String state)
  {
    Map<String, String> stateUnitKeyMap = record.getMapField(stateUnitKey);
    if (stateUnitKeyMap == null)
    {
      stateUnitKeyMap = new HashMap<String, String>();
      record.setMapField(stateUnitKey, stateUnitKeyMap);
    }
    stateUnitKeyMap.put(instanceName, state);
    record.setMapField(stateUnitKey, stateUnitKeyMap);
  }
}
