package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import com.linkedin.clustermanager.agent.zk.ZkClient;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.agent.zk.ZKUtil;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.controller.ExternalViewGenerator;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.util.CMUtil;

public class TestZKRoutingInfoProvider
{
  public Map<String, List<ZNRecord>> createCurrentStates(String[] dbNames,
      String[] nodeNames, int[] partitions, int[] replicas)
  {
    Map<String, List<ZNRecord>> currentStates = new TreeMap<String, List<ZNRecord>>();
    Map<String, Map<String, ZNRecord>> currentStates2 = new TreeMap<String, Map<String, ZNRecord>>();

    Map<String, String> stateMaster = new TreeMap<String, String>();
    stateMaster.put(CMConstants.ZNAttribute.CURRENT_STATE.toString(), "MASTER");

    Map<String, String> stateSlave = new TreeMap<String, String>();
    stateSlave.put(CMConstants.ZNAttribute.CURRENT_STATE.toString(), "SLAVE");

    for (int i = 0; i < nodeNames.length; i++)
    {
      currentStates.put(nodeNames[i], new ArrayList<ZNRecord>());
      currentStates2.put(nodeNames[i], new TreeMap<String, ZNRecord>());
      for (int j = 0; j < dbNames.length; j++)
      {
        ZNRecord dbPartitionState = new ZNRecord(dbNames[j]);
        currentStates2.get(nodeNames[i]).put(dbNames[j], dbPartitionState);
      }
    }

    Random rand = new Random(1234);
    for (int j = 0; j < dbNames.length; j++)
    {
      int partition = partitions[j];
      ArrayList<Integer> randomArray = new ArrayList<Integer>();
      for (int i = 0; i < partition; i++)
      {
        randomArray.add(i);
      }
      Collections.shuffle(randomArray, rand);

      for (int i = 0; i < partition; i++)
      {
        stateMaster.put(Message.Attributes.STATE_UNIT_GROUP.toString(),
            dbNames[j]);
        stateSlave.put(Message.Attributes.STATE_UNIT_GROUP.toString(),
            dbNames[j]);
        int nodes = nodeNames.length;
        int master = randomArray.get(i) % nodes;
        String partitionName = dbNames[j] + ".partition-" + i;
        Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) (currentStates2
            .get(nodeNames[master]).get(dbNames[j]).getMapFields());
        assert (map != null);
        map.put(partitionName, stateMaster);

        for (int k = 1; k <= replicas[j]; k++)
        {
          int slave = (master + k) % nodes;
          Map<String, Map<String, String>> map2 = (Map<String, Map<String, String>>) currentStates2
              .get(nodeNames[slave]).get(dbNames[j]).getMapFields();

          map2.put(partitionName, stateSlave);
        }
      }
    }
    for (String nodeName : currentStates2.keySet())
    {
      Map<String, ZNRecord> recMap = currentStates2.get(nodeName);
      List<ZNRecord> list = new ArrayList<ZNRecord>();
      for (ZNRecord rec : recMap.values())
      {
        list.add(rec);
      }
      currentStates.put(nodeName, list);
    }
    return currentStates;
  }

  public void Verify(Map<String, List<ZNRecord>> currentStates,
      Map<String, Map<String, Set<String>>> routingMap)
  {
    int counter1 = 0;
    int counter2 = 0;
    for (String nodeName : currentStates.keySet())
    {
      List<ZNRecord> dbStateList = currentStates.get(nodeName);
      for (ZNRecord dbState : dbStateList)
      {
        Map<String, Map<String, String>> dbStateMap = dbState.getMapFields();
        for (String partitionName : dbStateMap.keySet())
        {
          Map<String, String> stateMap = (Map<String, String>) dbStateMap
              .get(partitionName);
          String state = (String) stateMap
              .get(CMConstants.ZNAttribute.CURRENT_STATE.toString());
          AssertJUnit.assertTrue(routingMap.get(partitionName).get(state)
              .contains(nodeName));
          counter1++;
        }
      }
    }

    for (String partitionName : routingMap.keySet())
    {
      Map<String, Set<String>> partitionState = routingMap.get(partitionName);
      for (String state : partitionState.keySet())
      {
        counter2 += partitionState.get(state).size();
      }
    }
    AssertJUnit.assertTrue(counter2 == counter1);
  }

  // public static void main(String[] args)
  @Test
  public void testInvocation() throws Exception
  {
    String[] dbNames = new String[3];
    for (int i = 0; i < dbNames.length; i++)
    {
      dbNames[i] = "DB_" + i;
    }
    String[] nodeNames = new String[6];
    for (int i = 0; i < nodeNames.length; i++)
    {
      nodeNames[i] = "LOCALHOST_100" + i;
    }

    int[] partitions = new int[dbNames.length];
    for (int i = 0; i < partitions.length; i++)
    {
      partitions[i] = (i + 1) * 10;
    }

    int[] replicas = new int[dbNames.length];
    for (int i = 0; i < replicas.length; i++)
    {
      replicas[i] = 3;
    }
    Map<String, List<ZNRecord>> currentStates = createCurrentStates(dbNames,
        nodeNames, partitions, replicas);
    ExternalViewGenerator provider = new ExternalViewGenerator();

    List<ZNRecord> mockIdealStates = new ArrayList<ZNRecord>();
    for (String dbName : dbNames)
    {
      ZNRecord rec = new ZNRecord(dbName);
      mockIdealStates.add(rec);
    }
    List<ZNRecord> externalView = provider.computeExternalView(currentStates,
        mockIdealStates);

    Map<String, Map<String, Set<String>>> routingMap = provider
        .getRouterMapFromExternalView(externalView);

    Verify(currentStates, routingMap);

    /* write current state and external view to ZK */
    /*
     * String clusterName = "test-cluster44"; ZkClient zkClient = new
     * ZkClient("localhost:2181"); zkClient.setZkSerializer(new
     * ZNRecordSerializer());
     * 
     * for(String nodeName : currentStates.keySet()) {
     * if(zkClient.exists(CMUtil.getCurrentStatePath(clusterName, nodeName))) {
     * zkClient.deleteRecursive(CMUtil.getCurrentStatePath(clusterName,
     * nodeName)); } ZKUtil.createChildren(zkClient,CMUtil.getCurrentStatePath
     * (clusterName, nodeName), currentStates.get(nodeName)); }
     * 
     * //List<ZNRecord> externalView =
     * ZKRoutingInfoProvider.computeExternalView(currentStates); String
     * routingTablePath = CMUtil.getExternalViewPath(clusterName);
     * if(zkClient.exists(routingTablePath)) {
     * zkClient.deleteRecursive(routingTablePath); }
     * 
     * ZKUtil.createChildren(zkClient, CMUtil.getExternalViewPath(clusterName),
     * externalView);
     */
  }
}
