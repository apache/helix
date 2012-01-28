package com.linkedin.helix.josql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.josql.Query;
import org.josql.QueryExecutionException;
import org.josql.QueryParseException;
import org.josql.QueryResults;

import com.linkedin.helix.ClusterDataAccessor;
import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.ClusterManagerException;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.LiveInstance.LiveInstanceProperty;

public class ClusterJosqlQueryProcessor
{
  public static final String PARTITIONS = "PARTITIONS";
  public static final String FLATTABLE = ".Table";

  ClusterManager _manager;
  private static Logger _logger = Logger
  .getLogger(ClusterJosqlQueryProcessor.class);


  public ClusterJosqlQueryProcessor(ClusterManager manager)
  {
    _manager = manager;
  }

  String parseFromTarget(String sql)
  {
    // We need to find out the "FROM" target, and replace it with liveInstances / partitions etc
    int fromIndex = sql.indexOf("FROM");
    if(fromIndex == -1)
    {
      throw new ClusterManagerException("Query must contain FROM target. Query: "+ sql);
    }
    // Per JoSql, select FROM <target> the target must be a object class that corresponds to a "table row"
    // In out case, the row is always a ZNRecord

    int nextSpace = sql.indexOf(" ", fromIndex);
    while(sql.charAt(nextSpace) == ' ')
    {
      nextSpace ++;
    }
    int nextnextSpace = sql.indexOf(" ", nextSpace);
    if(nextnextSpace == -1)
    {
      nextnextSpace = sql.length();
    }
    String fromTarget = sql.substring(nextSpace, nextnextSpace).trim();

    if(fromTarget.length() == 0)
    {
      throw new ClusterManagerException("FROM target in the query cannot be empty. Query: " + sql);
    }
    return fromTarget;
  }
  
  public List<Object> runJoSqlQuery(String josql, String resourceGroupName, Map<String, Object> bindVariables, List queryTarget)
    throws QueryParseException, QueryExecutionException
  {
    Query josqlQuery = prepareQuery(resourceGroupName, bindVariables);    
    
    josqlQuery.parse(josql);
    QueryResults qr = josqlQuery.execute(queryTarget);
    
    return qr.getResults();
  }
  
  Query prepareQuery(String resourceGroupName, Map<String, Object> bindVariables)
  {
    ClusterDataAccessor accessor = _manager.getDataAccessor();
    List<ZNRecord> instanceConfigs = accessor.getChildValues(PropertyType.CONFIGS);
    List<ZNRecord> liveInstances = accessor.getChildValues(PropertyType.LIVEINSTANCES);
    List<ZNRecord> stateModelDefs = accessor.getChildValues(PropertyType.STATEMODELDEFS);
    // TODO: healthcheck info as part of default bind variables
    ZNRecord idealState = accessor.getProperty(PropertyType.IDEALSTATES, resourceGroupName);
    // Partition list: for selecting partitions from
    List<ZNRecord> partitions = new ArrayList<ZNRecord>();
    for(String partitionName : idealState.getMapFields().keySet())
    {
      partitions.add(new ZNRecord(partitionName));
    }

    ZNRecord externalView = accessor.getProperty(PropertyType.EXTERNALVIEW, resourceGroupName);

    Map<String, ZNRecord> currentStates = new HashMap<String, ZNRecord>();
    for(ZNRecord instance : liveInstances)
    {
      String host = instance.getId();
      String sessionId = instance.getSimpleField(LiveInstanceProperty.SESSION_ID.toString());
      ZNRecord currentState = accessor.getProperty(PropertyType.CURRENTSTATES, host, sessionId, resourceGroupName);
      if(currentState == null)
      {
        _logger.warn("ResourceGroup "+ resourceGroupName +" has null currentState");
        currentState = new ZNRecord(resourceGroupName);
      }
      currentStates.put(host, currentState);
    }
    
    Query josqlQuery = new Query();

    // Set the default bind variables
    josqlQuery.setVariable(PropertyType.CONFIGS.toString(), instanceConfigs);
    josqlQuery.setVariable(PropertyType.IDEALSTATES.toString(), idealState);
    josqlQuery.setVariable(PropertyType.LIVEINSTANCES.toString(), liveInstances);
    josqlQuery.setVariable(PropertyType.STATEMODELDEFS.toString(), stateModelDefs);
    josqlQuery.setVariable(PropertyType.EXTERNALVIEW.toString(), externalView);
    josqlQuery.setVariable(PropertyType.CURRENTSTATES.toString(), currentStates);
    josqlQuery.setVariable(PARTITIONS, partitions);
    
    // Flat version of ZNRecords
    josqlQuery.setVariable(PropertyType.CONFIGS.toString()+FLATTABLE, ZNRecordRow.flat(instanceConfigs));
    josqlQuery.setVariable(PropertyType.IDEALSTATES.toString()+FLATTABLE, ZNRecordRow.flat(idealState));
    josqlQuery.setVariable(PropertyType.LIVEINSTANCES.toString()+FLATTABLE, ZNRecordRow.flat(liveInstances));
    josqlQuery.setVariable(PropertyType.STATEMODELDEFS.toString()+FLATTABLE, ZNRecordRow.flat(stateModelDefs));
    josqlQuery.setVariable(PropertyType.EXTERNALVIEW.toString()+FLATTABLE, ZNRecordRow.flat(externalView));
    josqlQuery.setVariable(PropertyType.CURRENTSTATES.toString()+FLATTABLE, ZNRecordRow.flat(currentStates.values()));
    josqlQuery.setVariable(PARTITIONS+FLATTABLE, ZNRecordRow.flat(partitions));
    // Set additional bind variables
    if(bindVariables != null)
    {
      for(String key : bindVariables.keySet())
      {
        josqlQuery.setVariable(key, bindVariables.get(key));
      }
    }

    josqlQuery.addFunctionHandler(new ZNRecordJosqlFunctionHandler());
    josqlQuery.addFunctionHandler(new ZNRecordRow());
    return josqlQuery;
  }
  public List<Object> runJoSqlQuery(String josql, String resourceGroupName, Map<String, Object> bindVariables)
      throws QueryParseException, QueryExecutionException
  {
    Query josqlQuery = prepareQuery(resourceGroupName, bindVariables);    
    
    // Per JoSql, select FROM <target> the target must be a object class that corresponds to a "table row",
    // while the table (list of Objects) are put in the query by query.execute(List<Object>). In the input,
    // In out case, the row is always a ZNRecord. But in SQL, the from target is a "table name".

    String fromTargetString = parseFromTarget(josql);

    List fromTarget = null;
    if(fromTargetString.equalsIgnoreCase(PARTITIONS))
    {
      fromTarget  = (List<ZNRecord>)(josqlQuery.getVariable(PARTITIONS.toString()));
    }
    else if(fromTargetString.equalsIgnoreCase(PropertyType.LIVEINSTANCES.toString()))
    {
      fromTarget = (List<ZNRecord>)(josqlQuery.getVariable(PropertyType.LIVEINSTANCES.toString()));
    }
    else if(fromTargetString.equalsIgnoreCase(PropertyType.CONFIGS.toString()))
    {
      fromTarget = (List<ZNRecord>)(josqlQuery.getVariable(PropertyType.CONFIGS.toString()));
    }
    else if (fromTargetString.equalsIgnoreCase(PropertyType.STATEMODELDEFS.toString()))
    {
      fromTarget = (List<ZNRecord>)(josqlQuery.getVariable(PropertyType.STATEMODELDEFS.toString()));
    }
    else if (fromTargetString.equalsIgnoreCase(PropertyType.EXTERNALVIEW.toString()))
    {
      fromTarget = (List<ZNRecord>)(josqlQuery.getVariable(PropertyType.EXTERNALVIEW.toString()));
    }
    else if(fromTargetString.equalsIgnoreCase(PARTITIONS+FLATTABLE))
    {
      fromTarget  = (List<ZNRecordRow>)(josqlQuery.getVariable(PARTITIONS.toString()+FLATTABLE));
    }
    else if(fromTargetString.equalsIgnoreCase(PropertyType.LIVEINSTANCES.toString()+FLATTABLE))
    {
      fromTarget = (List<ZNRecordRow>)(josqlQuery.getVariable(PropertyType.LIVEINSTANCES.toString()+FLATTABLE));
    }
    else if(fromTargetString.equalsIgnoreCase(PropertyType.CONFIGS.toString()+FLATTABLE))
    {
      fromTarget = (List<ZNRecordRow>)(josqlQuery.getVariable(PropertyType.CONFIGS.toString()+FLATTABLE));
    }
    else if (fromTargetString.equalsIgnoreCase(PropertyType.STATEMODELDEFS.toString()+FLATTABLE))
    {
      fromTarget = (List<ZNRecordRow>)(josqlQuery.getVariable(PropertyType.STATEMODELDEFS.toString()+FLATTABLE));
    }
    else if (fromTargetString.equalsIgnoreCase(PropertyType.EXTERNALVIEW.toString()+FLATTABLE))
    {
      fromTarget = (List<ZNRecordRow>)(josqlQuery.getVariable(PropertyType.EXTERNALVIEW.toString()+FLATTABLE));
    }
    else
    {
      throw new ClusterManagerException("Unknown query target " + fromTargetString
          + ". Target should be PARTITIONS, LIVEINSTANCES, CONFIGS, STATEMODELDEFS and corresponding flat Tables");
    }

    // Per JoSql, select FROM <target> the target must be a object class that corresponds to a "table row"
    // In out case, the row is always a ZNRecord
    josql = josql.replaceFirst(fromTargetString, fromTargetString.endsWith(FLATTABLE) ? 
        "com.linkedin.clustermanager.josql.ZNRecordRow" :"com.linkedin.clustermanager.ZNRecord");
    josqlQuery.parse(josql);
    QueryResults qr = josqlQuery.execute(fromTarget);
    return qr.getResults();
  }
  
  
}
