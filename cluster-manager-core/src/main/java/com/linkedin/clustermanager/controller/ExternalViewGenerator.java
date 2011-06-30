package com.linkedin.clustermanager.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.Message;

/*
 * ZKRoutingInfoProvider keeps a copy of the routing table. Given a partition id,
 * it will return 
 * 
 * 1. The list of partition that can be read 
 * 2. the master partition, for write operation
 * 
 * The routing table is constructed from the currentState of each storage nodes.
 * The current state is a list of following pairs: partition-id:State(MASTER / SLAVE)
 * 
 * TODO: move the code as part of router process
 * TODO: add listeners to node current state changes
 * */
public class ExternalViewGenerator
{
  static Logger _logger = Logger.getLogger(ExternalViewGenerator.class);

  /*
   * Given a list of external view ZNRecord nodes(one for each cluster),
   * calculate the routing map.
   * 
   * The format of the routing map is like this:
   * 
   * Map<String, Map<String, Set<String>>> maps from a partitionName to its
   * states Map<String, List<String>> The second Map maps from a state
   * ("MASTER", "SLAVE"...) to a list of nodeNames
   * 
   * So that the we can query the map for the list of nodes by providing the
   * partition name and the expected state.
   */
  public Map<String, Map<String, Set<String>>> getRouterMapFromExternalView(
      List<ZNRecord> dbExternalViewList)
  {
    Map<String, Map<String, Set<String>>> result = new TreeMap<String, Map<String, Set<String>>>();

    for (ZNRecord dbNodeView : dbExternalViewList)
    {
      Map<String, Map<String, String>> dbNodeStateMap = dbNodeView
          .getMapFields();
      for (String partitionId : dbNodeStateMap.keySet())
      {
        if (!result.containsKey(partitionId))
        {
          result.put(partitionId, new TreeMap<String, Set<String>>());
        }
        Map<String, String> nodeStateMap = dbNodeStateMap.get(partitionId);
        for (String nodeName : nodeStateMap.keySet())
        {
          String state = (String) nodeStateMap.get(nodeName);
          if (!result.get(partitionId).containsKey(state))
          {
            result.get(partitionId).put(state, new TreeSet<String>());
          }
          result.get(partitionId).get(state).add(nodeName);
        }
      }
    }
    return result;
  }

  /*
   * The parameter is a map that maps the nodeName to a list of ZNRecords.
   */
  public List<ZNRecord> computeExternalView(
      Map<String, List<ZNRecord>> currentStates, List<ZNRecord> idealStates)
  {
    List<ZNRecord> resultList = new ArrayList<ZNRecord>();
    Map<String, ZNRecord> resultRoutingTable = new HashMap<String, ZNRecord>();
    // maps from dbName to another map : partition -> map <nodename,
    // master/slave>;
    // Fill the routing table with "empty" default state according to ideals
    // states
    // in the cluster
    if (idealStates != null)
    {
      for (ZNRecord idealState : idealStates)
      {
        ZNRecord defaultDBExternalView = new ZNRecord();
        defaultDBExternalView.setId(idealState.getId());
        resultRoutingTable.put(idealState.getId(), defaultDBExternalView);
      }
    } else
    {
      assert (!currentStates.isEmpty());
      return resultList;
    }
    for (String nodeName : currentStates.keySet())
    {
      List<ZNRecord> zndbStates = currentStates.get(nodeName);
      for (ZNRecord dbNodeStateRecord : zndbStates)
      {
        Map<String, Map<String, String>> dbStates = dbNodeStateRecord
            .getMapFields();
        for (String stateUnitKey : dbStates.keySet())
        {
          Map<String, String> dbPartitionStates = dbStates.get(stateUnitKey);
          String dbName = dbPartitionStates
              .get(Message.Attributes.STATE_UNIT_GROUP.toString());
          ZNRecord partitionStatus = resultRoutingTable.get(dbName);
          if (partitionStatus == null)
          {
            partitionStatus = new ZNRecord();
            partitionStatus.setId(dbName);
            resultRoutingTable.put(dbName, partitionStatus);
          }
          String currentStateKey = CMConstants.ZNAttribute.CURRENT_STATE
              .toString();

          if (!partitionStatus.getMapFields().containsKey(stateUnitKey))
          {
            partitionStatus.setMapField(stateUnitKey,
                new TreeMap<String, String>());
          }
          partitionStatus.getMapField(stateUnitKey).put(nodeName,
              dbPartitionStates.get(currentStateKey));

        }
      }
    }
    for (ZNRecord record : resultRoutingTable.values())
    {
      resultList.add(record);
    }
    return resultList;
  }
}
