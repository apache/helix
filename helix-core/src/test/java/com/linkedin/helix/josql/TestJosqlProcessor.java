package com.linkedin.helix.josql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.ClusterManagerException;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.integration.ZkStandAloneCMTestBase;
import com.linkedin.helix.josql.ClusterJosqlQueryProcessor;

public class TestJosqlProcessor extends ZkStandAloneCMTestBase
{
  @Test (groups = {"integrationTest"})
  public void testJosqlQuery() throws Exception
  {
    ClusterManager manager = ((TestHelper.StartCMResult) (_startCMResultMap.values().toArray()[0]))._manager;
    
    // Find the instance name that contains partition TestDB_2 and state is 'MASTER'
    String SQL = "SELECT id  " + 
        "FROM LIVEINSTANCES " + 
        "WHERE getMapFieldValue( :IDEALSTATES, :partitionName, :_currObj.id)='MASTER'";
    Map<String, Object> bindVariables = new HashMap<String, Object>();
    bindVariables.put("partitionName", "TestDB_2");
    
    ClusterJosqlQueryProcessor p = new ClusterJosqlQueryProcessor(manager);
    List<Object> result = p.runJoSqlQuery(SQL, "TestDB", bindVariables);
    
    Assert.assertEquals(result.size(), 1);
    List<Object> firstList = (List<Object>) result.get(0);
    Assert.assertTrue(((String)(firstList.get(0))).equalsIgnoreCase("localhost_12919"));
    
    // Find the live instances names that hosts Partition TestDB_10 according to idealstate
    
    SQL = "SELECT id  " + 
        "FROM LIVEINSTANCES " + 
        "WHERE hasMapFieldKey( :IDEALSTATES, :partitionName, :_currObj.id)='true'";
    p = new ClusterJosqlQueryProcessor(manager);
    bindVariables.put("partitionName", "TestDB_10");
    result = p.runJoSqlQuery(SQL, "TestDB", bindVariables);
    
    Assert.assertEquals(result.size(), 3);
    Set<String> hosts = new HashSet<String>();
    for(Object o : result)
    {
      String val = (String) ((List<Object>)o).get(0);
      hosts.add(val);
    }
    Assert.assertTrue(hosts.contains("localhost_12918"));
    Assert.assertTrue(hosts.contains("localhost_12919"));
    Assert.assertTrue(hosts.contains("localhost_12922"));
    
    // Find the partitions on host localhost_12919 and is on MASTER state
    SQL = "SELECT id  " + 
        "FROM PARTITIONS " + 
        "WHERE getMapFieldValue( :EXTERNALVIEW, id, :instanceName)='MASTER'";
    p = new ClusterJosqlQueryProcessor(manager);
    bindVariables.clear();
    bindVariables.put("instanceName", "localhost_12919");
    result = p.runJoSqlQuery(SQL, "TestDB", bindVariables);
    
    Assert.assertEquals(result.size(), 4);
    Set<String> partitions = new HashSet<String>();
    for(Object o : result)
    {
      String val = (String) ((List<Object>)o).get(0);
      partitions.add(val);
    }
    Assert.assertTrue(partitions.contains("TestDB_0"));
    Assert.assertTrue(partitions.contains("TestDB_1"));
    Assert.assertTrue(partitions.contains("TestDB_16"));
    Assert.assertTrue(partitions.contains("TestDB_2"));

    // Find the partitions on host localhost_12919 and is on MASTER state
    // Same as above but according to currentstates
    SQL = "SELECT id  " + 
        "FROM PARTITIONS " + 
        "WHERE getMapFieldValue( getZNRecordFromMap(:CURRENTSTATES, :instanceName), :_currObj.id, :mapFieldKey)=:partitionState";
    
    p = new ClusterJosqlQueryProcessor(manager);
    bindVariables.clear();
    bindVariables.put("instanceName", "localhost_12919");
    bindVariables.put("mapFieldKey", "CURRENT_STATE");
    bindVariables.put("partitionState", "MASTER");
    
    result = p.runJoSqlQuery(SQL, "TestDB", bindVariables);
    
    Assert.assertEquals(result.size(), 4);
    partitions.clear();
    partitions = new HashSet<String>();
    for(Object o : result)
    {
      String val = (String) ((List<Object>)o).get(0);
      partitions.add(val);
    }
    Assert.assertTrue(partitions.contains("TestDB_0"));
    Assert.assertTrue(partitions.contains("TestDB_1"));
    Assert.assertTrue(partitions.contains("TestDB_16"));
    Assert.assertTrue(partitions.contains("TestDB_2"));
    
    // get node name that hosts a certain partition with certain state
    
    SQL = "SELECT id  " + 
        "FROM LIVEINSTANCES " + 
        "WHERE getMapFieldValue( getZNRecordFromMap(:CURRENTSTATES, id), :partitionName, :mapFieldKey)=:partitionState";
    
    p = new ClusterJosqlQueryProcessor(manager);
    bindVariables.clear();
    bindVariables.put("partitionName", "TestDB_8");
    bindVariables.put("mapFieldKey", "CURRENT_STATE");
    bindVariables.put("partitionState", "SLAVE");
    
    result = p.runJoSqlQuery(SQL, "TestDB", bindVariables);
    
    Assert.assertEquals(result.size(), 2);
    partitions.clear();
    partitions = new HashSet<String>();
    for(Object o : result)
    {
      String val = (String) ((List<Object>)o).get(0);
      partitions.add(val);
    }
    Assert.assertTrue(partitions.contains("localhost_12919"));
    Assert.assertTrue(partitions.contains("localhost_12920"));
  }
  
  @Test (groups = {"unitTest"})
  public void parseFromTarget() 
  {
    ClusterJosqlQueryProcessor p = new ClusterJosqlQueryProcessor(null);
    String sql = "SELECT id  " + 
        "FROM LIVEINSTANCES ";
    String from = p.parseFromTarget(sql);
    Assert.assertTrue(from.equals("LIVEINSTANCES"));
    
    sql = "SELECT id      " + 
        "FROM    LIVEINSTANCES  WHERE 1=2";
    
    from = p.parseFromTarget(sql);
    Assert.assertTrue(from.equals("LIVEINSTANCES"));
    
    sql = "SELECT id      " + 
        "FROM LIVEINSTANCES";
    
    from = p.parseFromTarget(sql);
    Assert.assertTrue(from.equals("LIVEINSTANCES"));
    
    sql = "SELECT id      " + 
        " LIVEINSTANCES where tt=00";
    boolean exceptionThrown = false;
    try
    {
      from = p.parseFromTarget(sql);
    }
    catch(ClusterManagerException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }
}
