package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.spectator.RoutingTableProvider;

public class TestRoutingTable
{
  @Test
  public void testNullAndEmpty()
  {

    RoutingTableProvider routingTable = new RoutingTableProvider();
    NotificationContext changeContext = null;
    routingTable.onExternalViewChange(null, changeContext);
    List<ZNRecord> list = Collections.emptyList();
    routingTable.onExternalViewChange(list, changeContext);

  }

  @Test
 public  void testSimple()
  {
    List<InstanceConfig> instances;
    RoutingTableProvider routingTable = new RoutingTableProvider();
    NotificationContext changeContext = null;
    List<ZNRecord> externalViewList = new ArrayList<ZNRecord>();
    ZNRecord record = new ZNRecord();
    externalViewList.add(record);
    //one master
    record.setId("TESTDB");
    add(record, "TESTDB_0", "localhost_8900", "MASTER");
    routingTable.onExternalViewChange(externalViewList, changeContext);
    
    instances = routingTable.getInstances("TESTDB",
        "TESTDB_0", "MASTER");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(), 1);
    
    //additions 
    add(record, "TESTDB_0", "localhost_8901", "MASTER");
    add(record, "TESTDB_1", "localhost_8900", "SLAVE");
    
    routingTable.onExternalViewChange(externalViewList, changeContext);
    instances = routingTable.getInstances("TESTDB",
        "TESTDB_0", "MASTER");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(), 2);
    
    instances = routingTable.getInstances("TESTDB",
        "TESTDB_1", "SLAVE");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(), 1);
    
    //updates
    add(record, "TESTDB_0", "localhost_8901", "SLAVE");
    routingTable.onExternalViewChange(externalViewList, changeContext);
    instances = routingTable.getInstances("TESTDB",
        "TESTDB_0", "SLAVE");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(), 1);
  }
  @Test
  public void testStateUnitGroupDeletion(){
    List<InstanceConfig> instances;
    RoutingTableProvider routingTable = new RoutingTableProvider();
    NotificationContext changeContext = null;
    List<ZNRecord> externalViewList = new ArrayList<ZNRecord>();
    ZNRecord record = new ZNRecord();
    externalViewList.add(record);
    //one master
    record.setId("TESTDB");
    add(record, "TESTDB_0", "localhost_8900", "MASTER");
    routingTable.onExternalViewChange(externalViewList, changeContext);
    instances = routingTable.getInstances("TESTDB",
        "TESTDB_0", "MASTER");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(), 1);
    
    externalViewList.clear();
    routingTable.onExternalViewChange(externalViewList, changeContext);
    instances = routingTable.getInstances("TESTDB",
        "TESTDB_0", "MASTER");
    Assert.assertNotNull(instances);
    Assert.assertEquals(instances.size(),0);
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
