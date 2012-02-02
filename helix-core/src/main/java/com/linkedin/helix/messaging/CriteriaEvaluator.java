package com.linkedin.helix.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.josql.Query;
import org.josql.QueryExecutionException;
import org.josql.QueryParseException;
import org.josql.QueryResults;

import com.linkedin.helix.ClusterDataAccessor;
import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.Criteria;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.Criteria.DataSource;
import com.linkedin.helix.josql.ClusterJosqlQueryProcessor;
import com.linkedin.helix.josql.ZNRecordRow;

public class CriteriaEvaluator
{
  private static Logger logger = Logger.getLogger(CriteriaEvaluator.class);
  
  public List<Map<String, String>> evaluateCriteria(Criteria recipientCriteria, ClusterManager manager)
  {
    List<Map<String, String>> selected = new ArrayList<Map<String, String>>();
    
    String queryFields = 
        (!recipientCriteria.getInstanceName().equals("")  ? " " + ZNRecordRow.MAP_SUBKEY  : " ''") +","+
        (!recipientCriteria.getResourceGroup().equals("") ? " " + ZNRecordRow.ZNRECORD_ID : " ''") +","+
        (!recipientCriteria.getResourceKey().equals("")   ? " " + ZNRecordRow.MAP_KEY   : " ''") +","+
        (!recipientCriteria.getResourceState().equals("") ? " " + ZNRecordRow.MAP_VALUE : " '' ");
    
    String matchCondition = 
        ZNRecordRow.MAP_SUBKEY   + " LIKE '" + (!recipientCriteria.getInstanceName().equals("") ? (recipientCriteria.getInstanceName() +"'") :   "%' ") + " AND "+
        ZNRecordRow.ZNRECORD_ID+ " LIKE '" + (!recipientCriteria.getResourceGroup().equals("") ? (recipientCriteria.getResourceGroup() +"'") : "%' ") + " AND "+
        ZNRecordRow.MAP_KEY   + " LIKE '" + (!recipientCriteria.getResourceKey().equals("")   ? (recipientCriteria.getResourceKey()  +"'") :  "%' ") + " AND "+
        ZNRecordRow.MAP_VALUE  + " LIKE '" + (!recipientCriteria.getResourceState().equals("") ? (recipientCriteria.getResourceState()+"'") :  "%' ") + " AND "+
        ZNRecordRow.MAP_SUBKEY   + " IN ((SELECT [*]id FROM :LIVEINSTANCES))";
        
    
    String queryTarget = recipientCriteria.getDataSource().toString() + ClusterJosqlQueryProcessor.FLATTABLE;
    
    String josql = "SELECT DISTINCT " + queryFields
                 + " FROM " + queryTarget + " WHERE "
                 + matchCondition;
    ClusterJosqlQueryProcessor p = new ClusterJosqlQueryProcessor(manager);
    List<Object> result = new ArrayList<Object>();
    try
    {
      logger.info("JOSQL query: " + josql);
      result = p.runJoSqlQuery(josql, null, null);
    } 
    catch (Exception e)
    {
      logger.error("", e);
      return selected;
    } 
    
    for(Object o : result)
    {
      Map<String, String> resultRow = new HashMap<String, String>();
      List<Object> row = (List<Object>)o;
      resultRow.put("instanceName", (String)(row.get(0)));
      resultRow.put("resourceGroup", (String)(row.get(1)));
      resultRow.put("resourceKey", (String)(row.get(2)));
      resultRow.put("resourceState", (String)(row.get(3)));
      selected.add(resultRow);
    }
    return selected;
  }
}