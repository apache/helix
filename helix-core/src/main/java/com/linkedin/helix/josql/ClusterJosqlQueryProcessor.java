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
  
  public List<Object> runJoSqlQuery(String josql, 
      Map<String, Object> bindVariables, 
      List<Object> additionalFunctionHandlers, 
      List queryTarget)
    throws QueryParseException, QueryExecutionException
  {
    Query josqlQuery = prepareQuery(bindVariables, additionalFunctionHandlers);    
    
    josqlQuery.parse(josql);
    QueryResults qr = josqlQuery.execute(queryTarget);
    
    return qr.getResults();
  }
  
  Query prepareQuery(Map<String, Object> bindVariables,  List<Object> additionalFunctionHandlers)
  {
    ClusterDataAccessor accessor = _manager.getDataAccessor();
    // Get all the ZNRecords in the cluster and set them as bind variables
    List<ZNRecord> instanceConfigs = accessor.getChildValues(PropertyType.CONFIGS);
    List<ZNRecord> liveInstances = accessor.getChildValues(PropertyType.LIVEINSTANCES);
    List<ZNRecord> stateModelDefs = accessor.getChildValues(PropertyType.STATEMODELDEFS);
    // Idealstates are stored in a map from resourceGroup name to idealState ZNRecord
    List<ZNRecord> idealStateList = accessor.getChildValues(PropertyType.IDEALSTATES);
    Map<String, ZNRecord> idealStatesMap = new HashMap<String, ZNRecord>();
    for(ZNRecord idealState : idealStateList)
    {
      idealStatesMap.put(idealState.getId(), idealState);
    }
    // Make up the partition list: for selecting partitions
    List<ZNRecord> partitions = new ArrayList<ZNRecord>();
    for(ZNRecord idealState : idealStateList)
    {
      for(String partitionName : idealState.getMapFields().keySet())
      {
        partitions.add(new ZNRecord(partitionName));
      }
    }
    List<ZNRecord> externalViewList = accessor.getChildValues(PropertyType.EXTERNALVIEW);
    // ExternalViews are stored in a map from resourceGroup name to idealState ZNRecord
    Map<String, ZNRecord> externalViewMap = new HashMap<String, ZNRecord>();
    for(ZNRecord externalView : externalViewList)
    {
      externalViewMap.put(externalView.getId(), externalView);
    }
    // Map from instance name to a map from resourcegroup to current state ZNRecord
    Map<String,Map<String, ZNRecord>> currentStatesMap = new HashMap<String, Map<String, ZNRecord>>();
    // Map from instance name to a list of combined flat ZNRecordRow
    Map<String, List<ZNRecordRow>> flatCurrentStateMap = new HashMap<String, List<ZNRecordRow>>();
    
    for(ZNRecord instance : liveInstances)
    {
      String host = instance.getId();
      String sessionId = instance.getSimpleField(LiveInstanceProperty.SESSION_ID.toString());
      Map<String, ZNRecord> currentStates = new HashMap<String, ZNRecord>();
      List<ZNRecord> instanceCurrentStateList = new ArrayList<ZNRecord>();
      for(ZNRecord idealState : idealStateList)
      {  
        String resourceGroupName = idealState.getId();
        ZNRecord currentState = accessor.getProperty(PropertyType.CURRENTSTATES, host, sessionId, resourceGroupName);
        if(currentState == null)
        {
          _logger.warn("ResourceGroup "+ resourceGroupName +" has null currentState");
          currentState = new ZNRecord(resourceGroupName);
        }
        currentStates.put(resourceGroupName, currentState);
        instanceCurrentStateList.add(currentState);
      }
      currentStatesMap.put(host, currentStates);
      flatCurrentStateMap.put(host, ZNRecordRow.flatten(instanceCurrentStateList));
    }
    Query josqlQuery = new Query();

    // Set the default bind variables
    josqlQuery.setVariable(PropertyType.CONFIGS.toString(), instanceConfigs);
    josqlQuery.setVariable(PropertyType.IDEALSTATES.toString(), idealStatesMap);
    josqlQuery.setVariable(PropertyType.LIVEINSTANCES.toString(), liveInstances);
    josqlQuery.setVariable(PropertyType.STATEMODELDEFS.toString(), stateModelDefs);
    josqlQuery.setVariable(PropertyType.EXTERNALVIEW.toString(), externalViewMap);
    josqlQuery.setVariable(PropertyType.CURRENTSTATES.toString(), currentStatesMap);
    josqlQuery.setVariable(PARTITIONS, partitions);
    
    // Flat version of ZNRecords
    josqlQuery.setVariable(PropertyType.CONFIGS.toString()+FLATTABLE, ZNRecordRow.flatten(instanceConfigs));
    josqlQuery.setVariable(PropertyType.IDEALSTATES.toString()+FLATTABLE, ZNRecordRow.flatten(idealStateList));
    josqlQuery.setVariable(PropertyType.LIVEINSTANCES.toString()+FLATTABLE, ZNRecordRow.flatten(liveInstances));
    josqlQuery.setVariable(PropertyType.STATEMODELDEFS.toString()+FLATTABLE, ZNRecordRow.flatten(stateModelDefs));
    josqlQuery.setVariable(PropertyType.EXTERNALVIEW.toString()+FLATTABLE, ZNRecordRow.flatten(externalViewList));
    josqlQuery.setVariable(PropertyType.CURRENTSTATES.toString()+FLATTABLE, flatCurrentStateMap.values());
    josqlQuery.setVariable(PARTITIONS+FLATTABLE, ZNRecordRow.flatten(partitions));
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
    josqlQuery.addFunctionHandler(new Integer(0));
    if(additionalFunctionHandlers != null)
    {
      for(Object functionHandler : additionalFunctionHandlers)
      {
        josqlQuery.addFunctionHandler(functionHandler);
      }
    }
    return josqlQuery;
  }
  
  public List<Object> runJoSqlQuery(String josql, Map<String, Object> bindVariables, List<Object> additionalFunctionHandlers)
      throws QueryParseException, QueryExecutionException
  {
    Query josqlQuery = prepareQuery(bindVariables, additionalFunctionHandlers);    
    
    // Per JoSql, select FROM <target> the target must be a object class that corresponds to a "table row",
    // while the table (list of Objects) are put in the query by query.execute(List<Object>). In the input,
    // In out case, the row is always a ZNRecord. But in SQL, the from target is a "table name".

    String fromTargetString = parseFromTarget(josql);

    List fromTargetList = null;
    Object fromTarget = null;
    if(fromTargetString.equalsIgnoreCase(PARTITIONS))
    {
      fromTarget  = josqlQuery.getVariable(PARTITIONS.toString());
    }
    else if(fromTargetString.equalsIgnoreCase(PropertyType.LIVEINSTANCES.toString()))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.LIVEINSTANCES.toString());
    }
    else if(fromTargetString.equalsIgnoreCase(PropertyType.CONFIGS.toString()))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.CONFIGS.toString());
    }
    else if (fromTargetString.equalsIgnoreCase(PropertyType.STATEMODELDEFS.toString()))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.STATEMODELDEFS.toString());
    }
    else if (fromTargetString.equalsIgnoreCase(PropertyType.EXTERNALVIEW.toString()))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.EXTERNALVIEW.toString());
    }
    else if(fromTargetString.equalsIgnoreCase(PARTITIONS+FLATTABLE))
    {
      fromTarget  = josqlQuery.getVariable(PARTITIONS.toString()+FLATTABLE);
    }
    else if(fromTargetString.equalsIgnoreCase(PropertyType.LIVEINSTANCES.toString()+FLATTABLE))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.LIVEINSTANCES.toString()+FLATTABLE);
    }
    else if(fromTargetString.equalsIgnoreCase(PropertyType.CONFIGS.toString()+FLATTABLE))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.CONFIGS.toString()+FLATTABLE);
    }
    else if (fromTargetString.equalsIgnoreCase(PropertyType.STATEMODELDEFS.toString()+FLATTABLE))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.STATEMODELDEFS.toString()+FLATTABLE);
    }
    else if (fromTargetString.equalsIgnoreCase(PropertyType.EXTERNALVIEW.toString()+FLATTABLE))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.EXTERNALVIEW.toString()+FLATTABLE);
    }
    else
    {
      throw new ClusterManagerException("Unknown query target " + fromTargetString
          + ". Target should be PARTITIONS, LIVEINSTANCES, CONFIGS, STATEMODELDEFS and corresponding flat Tables");
    }
    
    fromTargetList = fromTargetString.endsWith(FLATTABLE) ? ((List<ZNRecordRow>)fromTarget) : ((List<ZNRecord>)fromTarget);

    // Per JoSql, select FROM <target> the target must be a object class that corresponds to a "table row"
    // In out case, the row is always a ZNRecord
    josql = josql.replaceFirst(fromTargetString, fromTargetString.endsWith(FLATTABLE) ? 
        ZNRecordRow.class.getName() :ZNRecord.class.getName());
    josqlQuery.parse(josql);
    QueryResults qr = josqlQuery.execute(fromTargetList);
    return qr.getResults();
  }
}
