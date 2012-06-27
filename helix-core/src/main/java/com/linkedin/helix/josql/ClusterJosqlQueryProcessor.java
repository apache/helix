/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.LiveInstance.LiveInstanceProperty;

public class ClusterJosqlQueryProcessor
{
  public static final String PARTITIONS = "PARTITIONS";
  public static final String FLATTABLE = ".Table";

  HelixManager _manager;
  private static Logger _logger = Logger.getLogger(ClusterJosqlQueryProcessor.class);

  public ClusterJosqlQueryProcessor(HelixManager manager)
  {
    _manager = manager;
  }

  String parseFromTarget(String sql)
  {
    // We need to find out the "FROM" target, and replace it with liveInstances
    // / partitions etc
    int fromIndex = sql.indexOf("FROM");
    if (fromIndex == -1)
    {
      throw new HelixException("Query must contain FROM target. Query: " + sql);
    }
    // Per JoSql, select FROM <target> the target must be a object class that
    // corresponds to a "table row"
    // In out case, the row is always a ZNRecord

    int nextSpace = sql.indexOf(" ", fromIndex);
    while (sql.charAt(nextSpace) == ' ')
    {
      nextSpace++;
    }
    int nextnextSpace = sql.indexOf(" ", nextSpace);
    if (nextnextSpace == -1)
    {
      nextnextSpace = sql.length();
    }
    String fromTarget = sql.substring(nextSpace, nextnextSpace).trim();

    if (fromTarget.length() == 0)
    {
      throw new HelixException("FROM target in the query cannot be empty. Query: " + sql);
    }
    return fromTarget;
  }

  public List<Object> runJoSqlQuery(String josql, Map<String, Object> bindVariables,
      List<Object> additionalFunctionHandlers, List queryTarget) throws QueryParseException,
      QueryExecutionException
  {
    Query josqlQuery = prepareQuery(bindVariables, additionalFunctionHandlers);

    josqlQuery.parse(josql);
    QueryResults qr = josqlQuery.execute(queryTarget);

    return qr.getResults();
  }

  Query prepareQuery(Map<String, Object> bindVariables, List<Object> additionalFunctionHandlers)
  {
    // DataAccessor accessor = _manager.getDataAccessor();
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    
    // Get all the ZNRecords in the cluster and set them as bind variables
    Builder keyBuilder = accessor.keyBuilder();
//    List<ZNRecord> instanceConfigs = accessor.getChildValues(PropertyType.CONFIGS,
//        ConfigScopeProperty.PARTICIPANT.toString());
    
    List<ZNRecord> instanceConfigs = HelixProperty.convertToList(accessor.getChildValues(keyBuilder.instanceConfigs()));

    List<ZNRecord> liveInstances = HelixProperty.convertToList(accessor.getChildValues(keyBuilder.liveInstances()));
    List<ZNRecord> stateModelDefs = HelixProperty.convertToList(accessor.getChildValues(keyBuilder.stateModelDefs()));
    
    // Idealstates are stored in a map from resource name to idealState ZNRecord
    List<ZNRecord> idealStateList = HelixProperty.convertToList(accessor.getChildValues(keyBuilder.idealStates()));
    
    Map<String, ZNRecord> idealStatesMap = new HashMap<String, ZNRecord>();
    for (ZNRecord idealState : idealStateList)
    {
      idealStatesMap.put(idealState.getId(), idealState);
    }
    // Make up the partition list: for selecting partitions
    List<ZNRecord> partitions = new ArrayList<ZNRecord>();
    for (ZNRecord idealState : idealStateList)
    {
      for (String partitionName : idealState.getMapFields().keySet())
      {
        partitions.add(new ZNRecord(partitionName));
      }
    }
    
    List<ZNRecord> externalViewList = HelixProperty.convertToList(accessor.getChildValues(keyBuilder.externalViews()));
    // ExternalViews are stored in a map from resource name to idealState
    // ZNRecord
    Map<String, ZNRecord> externalViewMap = new HashMap<String, ZNRecord>();
    for (ZNRecord externalView : externalViewList)
    {
      externalViewMap.put(externalView.getId(), externalView);
    }
    // Map from instance name to a map from resource to current state ZNRecord
    Map<String, Map<String, ZNRecord>> currentStatesMap = new HashMap<String, Map<String, ZNRecord>>();
    // Map from instance name to a list of combined flat ZNRecordRow
    Map<String, List<ZNRecordRow>> flatCurrentStateMap = new HashMap<String, List<ZNRecordRow>>();

    for (ZNRecord instance : liveInstances)
    {
      String host = instance.getId();
      String sessionId = instance.getSimpleField(LiveInstanceProperty.SESSION_ID.toString());
      Map<String, ZNRecord> currentStates = new HashMap<String, ZNRecord>();
      List<ZNRecord> instanceCurrentStateList = new ArrayList<ZNRecord>();
      for (ZNRecord idealState : idealStateList)
      {
        String resourceName = idealState.getId();
        
        HelixProperty property = accessor.getProperty(keyBuilder.currentState(host, sessionId, resourceName));
        ZNRecord currentState =null;
        if (property == null)
        {
          _logger.warn("Resource " + resourceName + " has null currentState");
          currentState = new ZNRecord(resourceName);
        }else{
          currentState = property.getRecord();
        }
        currentStates.put(resourceName, currentState);
        instanceCurrentStateList.add(currentState);
      }
      currentStatesMap.put(host, currentStates);
      flatCurrentStateMap.put(host, ZNRecordRow.flatten(instanceCurrentStateList));
    }
    Query josqlQuery = new Query();

    // Set the default bind variables
    josqlQuery
.setVariable(
        PropertyType.CONFIGS.toString() + "/" + ConfigScopeProperty.PARTICIPANT.toString(),
            instanceConfigs);
    josqlQuery.setVariable(PropertyType.IDEALSTATES.toString(), idealStatesMap);
    josqlQuery.setVariable(PropertyType.LIVEINSTANCES.toString(), liveInstances);
    josqlQuery.setVariable(PropertyType.STATEMODELDEFS.toString(), stateModelDefs);
    josqlQuery.setVariable(PropertyType.EXTERNALVIEW.toString(), externalViewMap);
    josqlQuery.setVariable(PropertyType.CURRENTSTATES.toString(), currentStatesMap);
    josqlQuery.setVariable(PARTITIONS, partitions);

    // Flat version of ZNRecords
    josqlQuery.setVariable(
        PropertyType.CONFIGS.toString() + "/" + ConfigScopeProperty.PARTICIPANT.toString()
            + FLATTABLE,
        ZNRecordRow.flatten(instanceConfigs));
    josqlQuery.setVariable(PropertyType.IDEALSTATES.toString() + FLATTABLE,
        ZNRecordRow.flatten(idealStateList));
    josqlQuery.setVariable(PropertyType.LIVEINSTANCES.toString() + FLATTABLE,
        ZNRecordRow.flatten(liveInstances));
    josqlQuery.setVariable(PropertyType.STATEMODELDEFS.toString() + FLATTABLE,
        ZNRecordRow.flatten(stateModelDefs));
    josqlQuery.setVariable(PropertyType.EXTERNALVIEW.toString() + FLATTABLE,
        ZNRecordRow.flatten(externalViewList));
    josqlQuery.setVariable(PropertyType.CURRENTSTATES.toString() + FLATTABLE,
        flatCurrentStateMap.values());
    josqlQuery.setVariable(PARTITIONS + FLATTABLE, ZNRecordRow.flatten(partitions));
    // Set additional bind variables
    if (bindVariables != null)
    {
      for (String key : bindVariables.keySet())
      {
        josqlQuery.setVariable(key, bindVariables.get(key));
      }
    }

    josqlQuery.addFunctionHandler(new ZNRecordJosqlFunctionHandler());
    josqlQuery.addFunctionHandler(new ZNRecordRow());
    josqlQuery.addFunctionHandler(new Integer(0));
    if (additionalFunctionHandlers != null)
    {
      for (Object functionHandler : additionalFunctionHandlers)
      {
        josqlQuery.addFunctionHandler(functionHandler);
      }
    }
    return josqlQuery;
  }

  public List<Object> runJoSqlQuery(String josql, Map<String, Object> bindVariables,
      List<Object> additionalFunctionHandlers) throws QueryParseException, QueryExecutionException
  {
    Query josqlQuery = prepareQuery(bindVariables, additionalFunctionHandlers);

    // Per JoSql, select FROM <target> the target must be a object class that
    // corresponds to a "table row",
    // while the table (list of Objects) are put in the query by
    // query.execute(List<Object>). In the input,
    // In out case, the row is always a ZNRecord. But in SQL, the from target is
    // a "table name".

    String fromTargetString = parseFromTarget(josql);

    List fromTargetList = null;
    Object fromTarget = null;
    if (fromTargetString.equalsIgnoreCase(PARTITIONS))
    {
      fromTarget = josqlQuery.getVariable(PARTITIONS.toString());
    } else if (fromTargetString.equalsIgnoreCase(PropertyType.LIVEINSTANCES.toString()))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.LIVEINSTANCES.toString());
    } else if (fromTargetString.equalsIgnoreCase(PropertyType.CONFIGS.toString() + "/"
        + ConfigScopeProperty.PARTICIPANT.toString()))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.CONFIGS.toString() + "/"
          + ConfigScopeProperty.PARTICIPANT.toString());
    } else if (fromTargetString.equalsIgnoreCase(PropertyType.STATEMODELDEFS.toString()))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.STATEMODELDEFS.toString());
    } else if (fromTargetString.equalsIgnoreCase(PropertyType.EXTERNALVIEW.toString()))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.EXTERNALVIEW.toString());
    } else if (fromTargetString.equalsIgnoreCase(PARTITIONS + FLATTABLE))
    {
      fromTarget = josqlQuery.getVariable(PARTITIONS.toString() + FLATTABLE);
    } else if (fromTargetString.equalsIgnoreCase(PropertyType.LIVEINSTANCES.toString() + FLATTABLE))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.LIVEINSTANCES.toString() + FLATTABLE);
    } else if (fromTargetString.equalsIgnoreCase(PropertyType.CONFIGS.toString() + "/"
        + ConfigScopeProperty.PARTICIPANT.toString()
        + FLATTABLE))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.CONFIGS.toString() + "/"
          + ConfigScopeProperty.PARTICIPANT.toString() + FLATTABLE);
    } else if (fromTargetString
        .equalsIgnoreCase(PropertyType.STATEMODELDEFS.toString() + FLATTABLE))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.STATEMODELDEFS.toString() + FLATTABLE);
    } else if (fromTargetString.equalsIgnoreCase(PropertyType.EXTERNALVIEW.toString() + FLATTABLE))
    {
      fromTarget = josqlQuery.getVariable(PropertyType.EXTERNALVIEW.toString() + FLATTABLE);
    } else
    {
      throw new HelixException(
          "Unknown query target "
              + fromTargetString
              + ". Target should be PARTITIONS, LIVEINSTANCES, CONFIGS, STATEMODELDEFS and corresponding flat Tables");
    }

    fromTargetList = fromTargetString.endsWith(FLATTABLE) ? ((List<ZNRecordRow>) fromTarget)
        : ((List<ZNRecord>) fromTarget);

    // Per JoSql, select FROM <target> the target must be a object class that
    // corresponds to a "table row"
    // In out case, the row is always a ZNRecord
    josql = josql.replaceFirst(
        fromTargetString,
        fromTargetString.endsWith(FLATTABLE) ? ZNRecordRow.class.getName() : ZNRecord.class
            .getName());
    josqlQuery.parse(josql);
    QueryResults qr = josqlQuery.execute(fromTargetList);
    return qr.getResults();
  }
}
