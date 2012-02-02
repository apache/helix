package com.linkedin.helix.josql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.josql.Query;
import org.josql.QueryExecutionException;
import org.josql.QueryParseException;
import org.josql.QueryResults;
import org.testng.annotations.Test;

import com.linkedin.helix.Criteria;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.josql.ZNRecordJosqlFunctionHandler;
import com.linkedin.helix.josql.ZNRecordRow;
import com.linkedin.helix.model.LiveInstance.LiveInstanceProperty;
import com.linkedin.helix.tools.IdealStateCalculatorForStorageNode;

public class TestClusterJosqlQueryProcessor
{
  @Test (groups = {"unitTest"})
  public void queryClusterDataSample() 
  {
    List<ZNRecord> liveInstances = new ArrayList<ZNRecord>();
    Map<String, ZNRecord> liveInstanceMap = new HashMap<String, ZNRecord>();
    List<String> instances = new ArrayList<String>();
    for(int i = 0;i<5; i++)
    {
      String instance = "localhost_"+(12918+i);
      instances.add(instance);
      ZNRecord metaData = new ZNRecord(instance);
      metaData.setSimpleField(LiveInstanceProperty.SESSION_ID.toString(),
          UUID.randomUUID().toString());
      metaData.setSimpleField("SCN", "" + (10-i));
      liveInstances.add(metaData);
      liveInstanceMap.put(instance, metaData);
    }
    
    //liveInstances.remove(0);
    ZNRecord externalView = IdealStateCalculatorForStorageNode.calculateIdealState(
        instances, 21, 3, "TestDB", "MASTER", "SLAVE");
    
    
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResourceGroup("TestDB");
    criteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    criteria.setResourceKey("TestDB_2%");
    criteria.setResourceState("SLAVE");
    
    String josql = 
      " SELECT DISTINCT mapSubKey AS 'subkey', mapValue AS 'mapValue' , getSimpleFieldValue(getZNRecordFromMap(:LIVEINSTANCESMAP, mapSubKey), 'SCN') AS 'SCN'" +
      " FROM com.linkedin.clustermanager.josql.ZNRecordRow " + 
      " WHERE mapKey LIKE 'TestDB_2%' " +
        " AND mapSubKey LIKE '%' " +
        " AND mapValue LIKE 'SLAVE' " +
        " AND mapSubKey IN ((SELECT [*]id FROM :LIVEINSTANCES)) " +
        " ORDER BY parseInt(getSimpleFieldValue(getZNRecordFromMap(:LIVEINSTANCESMAP, mapSubKey), 'SCN'))";
    
    Query josqlQuery = new Query();
    josqlQuery.setVariable("LIVEINSTANCES", liveInstances);
    josqlQuery.setVariable("LIVEINSTANCESMAP", liveInstanceMap);
    josqlQuery.addFunctionHandler(new ZNRecordRow());
    josqlQuery.addFunctionHandler(new ZNRecordJosqlFunctionHandler());
    josqlQuery.addFunctionHandler(new Integer(0));
    try
    {
      josqlQuery.parse(josql);
      QueryResults qr = josqlQuery.execute(ZNRecordRow.convertMapFields(externalView));
      @SuppressWarnings({ "unchecked", "unused" })
      List<Object> result = qr.getResults();
      
    } 
    catch (QueryParseException e)
    {
      e.printStackTrace();
    } catch (QueryExecutionException e)
    {
      e.printStackTrace();
    }

  }
}
