package com.linkedin.clustermanager.josql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.josql.Query;
import org.josql.QueryExecutionException;
import org.josql.QueryParseException;
import org.josql.QueryResults;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.LiveInstance.LiveInstanceProperty;

public class ClusterJosqlQueryProcessor
{
  public static final String PARTITIONS = "PARTITIONS";

  ClusterManager _manager;

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

  public List<Object> runJoSqlQuery(String josql, String resourceGroupName, Map<String, Object> bindVariables)
      throws QueryParseException, QueryExecutionException
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
      currentStates.put(host, accessor.getProperty(PropertyType.CURRENTSTATES, host, sessionId, resourceGroupName));
    }

    Query josqlQuery = new Query();
    // Set the default bind variables
    josqlQuery.setVariable(PropertyType.CONFIGS.toString(), instanceConfigs);
    josqlQuery.setVariable(PropertyType.IDEALSTATES.toString(), idealState);
    josqlQuery.setVariable(PropertyType.LIVEINSTANCES.toString(), liveInstances);
    josqlQuery.setVariable(PropertyType.STATEMODELDEFS.toString(), stateModelDefs);
    josqlQuery.setVariable(PropertyType.EXTERNALVIEW.toString(), externalView);
    josqlQuery.setVariable(PropertyType.CURRENTSTATES.toString(), currentStates);

    // Set additional bind variables
    if(bindVariables != null)
    {
      for(String key : bindVariables.keySet())
      {
        josqlQuery.setVariable(key, bindVariables.get(key));
      }
    }

    // Per JoSql, select FROM <target> the target must be a object class that corresponds to a "table row",
    // while the table (list of Objects) are put in the query by query.execute(List<Object>). In the input,
    // In out case, the row is always a ZNRecord. But in SQL, the from target is a "table name".

    String fromTargetString = parseFromTarget(josql);

    // Per JoSql, select FROM <target> the target must be a object class that corresponds to a "table row"
    // In out case, the row is always a ZNRecord
    josql = josql.replaceFirst(fromTargetString, "com.linkedin.clustermanager.ZNRecord");
    List<ZNRecord> fromTarget = null;
    if(fromTargetString.equalsIgnoreCase(PARTITIONS))
    {
      fromTarget = partitions;
    }
    else if(fromTargetString.equalsIgnoreCase(PropertyType.LIVEINSTANCES.toString()))
    {
      fromTarget = liveInstances;
    }
    else if(fromTargetString.equalsIgnoreCase(PropertyType.CONFIGS.toString()))
    {
      fromTarget = instanceConfigs;
    }
    else if (fromTargetString.equalsIgnoreCase(PropertyType.STATEMODELDEFS.toString()))
    {
      fromTarget = stateModelDefs;
    }
    else
    {
      throw new ClusterManagerException("Unknown query target " + fromTargetString
          + ". Target should be PARTITIONS, LIVEINSTANCES, CONFIGS, STATEMODELDEFS");
    }

    // Add function handler that takes care of ZNRecord operation functions
    // We should further investigate if it is possible to directly call parametric member
    // functions of ZNRecord
    josqlQuery.addFunctionHandler(new ZNRecordJosqlFunctionHandler());
    josqlQuery.parse(josql);
    QueryResults qr = josqlQuery.execute(fromTarget);
    return qr.getResults();
  }
}
