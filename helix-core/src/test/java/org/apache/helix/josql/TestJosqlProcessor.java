package org.apache.helix.josql;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.ZkStandAloneCMTestBase;
import org.apache.helix.josql.ClusterJosqlQueryProcessor;
import org.apache.helix.model.LiveInstance.LiveInstanceProperty;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestJosqlProcessor extends ZkStandAloneCMTestBase
{
  @Test (groups = {"integrationTest"})
  public void testJosqlQuery() throws Exception
  {
    HelixManager manager = ((TestHelper.StartCMResult) (_startCMResultMap.values().toArray()[0]))._manager;
    
    // Find the instance name that contains partition TestDB_2 and state is 'MASTER'
    String SQL = "SELECT id  " + 
        "FROM LIVEINSTANCES " + 
        "WHERE getMapFieldValue( getZNRecordFromMap(:IDEALSTATES , 'TestDB'), :partitionName, :_currObj.id)='MASTER'";
    Map<String, Object> bindVariables = new HashMap<String, Object>();
    bindVariables.put("partitionName", "TestDB_2");
    
    ClusterJosqlQueryProcessor p = new ClusterJosqlQueryProcessor(manager);
    List<Object> result = p.runJoSqlQuery(SQL, bindVariables, null);
    
    Assert.assertEquals(result.size(), 1);
    List<Object> firstList = (List<Object>) result.get(0);
    Assert.assertTrue(((String)(firstList.get(0))).equalsIgnoreCase("localhost_12921"));
    
    // Find the live instances names that hosts Partition TestDB_10 according to idealstate
    
    SQL = "SELECT id  " + 
        "FROM LIVEINSTANCES " + 
        "WHERE hasMapFieldKey( getZNRecordFromMap(:IDEALSTATES, 'TestDB'), :partitionName, :_currObj.id)='true'";
    p = new ClusterJosqlQueryProcessor(manager);
    bindVariables.put("partitionName", "TestDB_10");
    result = p.runJoSqlQuery(SQL, bindVariables, null);
    
    Assert.assertEquals(result.size(), 3);
    Set<String> hosts = new HashSet<String>();
    for(Object o : result)
    {
      String val = (String) ((List<Object>)o).get(0);
      hosts.add(val);
    }
    Assert.assertTrue(hosts.contains("localhost_12918"));
    Assert.assertTrue(hosts.contains("localhost_12920"));
    Assert.assertTrue(hosts.contains("localhost_12921"));
    
    // Find the partitions on host localhost_12919 and is on MASTER state
    SQL = "SELECT id  " + 
        "FROM PARTITIONS " + 
        "WHERE getMapFieldValue( getZNRecordFromMap(:EXTERNALVIEW, 'TestDB'), id, :instanceName)='MASTER'";
    p = new ClusterJosqlQueryProcessor(manager);
    bindVariables.clear();
    bindVariables.put("instanceName", "localhost_12919");
    result = p.runJoSqlQuery(SQL, bindVariables, null);
    
    Assert.assertEquals(result.size(), 4);
    Set<String> partitions = new HashSet<String>();
    for(Object o : result)
    {
      String val = (String) ((List<Object>)o).get(0);
      partitions.add(val);
    }
    Assert.assertTrue(partitions.contains("TestDB_6"));
    Assert.assertTrue(partitions.contains("TestDB_7"));
    Assert.assertTrue(partitions.contains("TestDB_9"));
    Assert.assertTrue(partitions.contains("TestDB_14"));

    // Find the partitions on host localhost_12919 and is on MASTER state
    // Same as above but according to currentstates
    SQL = "SELECT id  " + 
        "FROM PARTITIONS " + 
        "WHERE getMapFieldValue( getZNRecordFromMap(:CURRENTSTATES, :instanceName, 'TestDB'), :_currObj.id, :mapFieldKey)=:partitionState";
    
    p = new ClusterJosqlQueryProcessor(manager);
    bindVariables.clear();
    bindVariables.put("instanceName", "localhost_12919");
    bindVariables.put("mapFieldKey", "CURRENT_STATE");
    bindVariables.put("partitionState", "MASTER");
    
    result = p.runJoSqlQuery(SQL,  bindVariables, null);
    
    Assert.assertEquals(result.size(), 4);
    partitions.clear();
    partitions = new HashSet<String>();
    for(Object o : result)
    {
      String val = (String) ((List<Object>)o).get(0);
      partitions.add(val);
    }
    Assert.assertTrue(partitions.contains("TestDB_6"));
    Assert.assertTrue(partitions.contains("TestDB_7"));
    Assert.assertTrue(partitions.contains("TestDB_9"));
    Assert.assertTrue(partitions.contains("TestDB_14"));
    
    // get node name that hosts a certain partition with certain state
    
    SQL = "SELECT id  " + 
        "FROM LIVEINSTANCES " + 
        "WHERE getMapFieldValue( getZNRecordFromMap(:CURRENTSTATES, id, 'TestDB'), :partitionName, :mapFieldKey)=:partitionState";
    
    p = new ClusterJosqlQueryProcessor(manager);
    bindVariables.clear();
    bindVariables.put("partitionName", "TestDB_8");
    bindVariables.put("mapFieldKey", "CURRENT_STATE");
    bindVariables.put("partitionState", "SLAVE");
    
    result = p.runJoSqlQuery(SQL,  bindVariables, null);
    
    Assert.assertEquals(result.size(), 2);
    partitions.clear();
    partitions = new HashSet<String>();
    for(Object o : result)
    {
      String val = (String) ((List<Object>)o).get(0);
      partitions.add(val);
    }
    Assert.assertTrue(partitions.contains("localhost_12918"));
    Assert.assertTrue(partitions.contains("localhost_12922"));
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
    catch(HelixException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }
  
  @Test (groups=("unitTest"))
  public void testOrderby() throws Exception
  {
    HelixManager manager = ((TestHelper.StartCMResult) (_startCMResultMap.values().toArray()[0]))._manager;
    
    Map<String, ZNRecord> scnMap = new HashMap<String, ZNRecord>();
    for(int i = 0;i < NODE_NR; i++)
    {
      String instance = "localhost_"+(12918+i);
      ZNRecord metaData = new ZNRecord(instance);
      metaData.setSimpleField(LiveInstanceProperty.SESSION_ID.toString(),
          UUID.randomUUID().toString());
      metaData.setMapField("SCN", new HashMap<String, String>());
      for(int j = 0;j < _PARTITIONS; j++)
      {
        metaData.getMapField("SCN").put(TEST_DB+"_"+j, ""+i);
      }
      scnMap.put(instance, metaData);
    }
    Map<String, Object> bindVariables = new HashMap<String, Object>();
    bindVariables.put("scnMap", scnMap);
    String SQL = 
        " SELECT DISTINCT mapSubKey AS 'subkey', mapValue AS 'mapValue' , getMapFieldValue(getZNRecordFromMap(:scnMap, mapSubKey), 'SCN', mapKey) AS 'SCN'" +
        " FROM EXTERNALVIEW.Table " + 
        " WHERE mapKey LIKE 'TestDB_1' " +
          " AND mapSubKey LIKE '%' " +
          " AND mapValue LIKE 'SLAVE' " +
          " AND mapSubKey IN ((SELECT [*]id FROM :LIVEINSTANCES)) " +
          " ORDER BY parseInt(getMapFieldValue(getZNRecordFromMap(:scnMap, mapSubKey), 'SCN', mapKey))";

    ClusterJosqlQueryProcessor p = new ClusterJosqlQueryProcessor(manager);
    List<Object> result = p.runJoSqlQuery(SQL, bindVariables, null);
    int prevSCN = -1;
    for(Object row : result)
    {
      List<String> stringRow = (List<String>)row;
      Assert.assertTrue(stringRow.get(1).equals("SLAVE"));
      int scn = Integer.parseInt(stringRow.get(2));
      Assert.assertTrue(scn > prevSCN);
      prevSCN = scn;
    }
  }
}
