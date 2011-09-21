package com.linkedin.clustermanager.mock.controller;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.util.CMUtil;

public class MockController
{
  private ZkClient client;
  private String srcName;
  private String clusterName;

  public MockController(String src, String zkServer, String cluster)
  {
    srcName = src;
    clusterName = cluster;
    client = new ZkClient(zkServer);
    client.setZkSerializer(new ZNRecordSerializer());
  }

  void sendMessage(String msgId, String instanceName, String fromState,
      String toState, String partitionKey, int partitionId)
      throws InterruptedException, JsonGenerationException,
      JsonMappingException, IOException
  {
    Message message = new Message(MessageType.STATE_TRANSITION);

    message.setId(msgId);
    message.setMsgId(msgId);
    message.setSrcName(srcName);
    message.setTgtName(instanceName);
    message.setMsgState("new");
    message.setFromState(fromState);
    message.setToState(toState);
    // message.setPartitionId(partitionId);
    message.setStateUnitKey(partitionKey);

    String path = CMUtil.getMessagePath(clusterName, instanceName) + "/"
        + message.getId();
    ObjectMapper mapper = new ObjectMapper();
    StringWriter sw = new StringWriter();
    mapper.writeValueUsingView(sw, message, Message.class);
    System.out.println(sw.toString());
    client.delete(path);

    Thread.sleep(10000);
    ZNRecord record = client.readData(CMUtil.getLiveInstancePath(clusterName,
        instanceName));
    message.setTgtSessionId(record.getSimpleField(
        CMConstants.ZNAttribute.SESSION_ID.toString()).toString());
    client.createPersistent(path, message);
  }

  public void createExternalView(List<String> instanceNames, int partitions,
      int replicas, String dbName, long randomSeed)
  {
    ClusterDataAccessor dataAccessor = new ZKDataAccessor(clusterName, client);

    ZNRecord externalView = computeRoutingTable(instanceNames, partitions,
        replicas, dbName, randomSeed);
    dataAccessor.setClusterProperty(ClusterPropertyType.EXTERNALVIEW, dbName,
        externalView);
  }

  public ZNRecord computeRoutingTable(List<String> instanceNames,
      int partitions, int replicas, String dbName, long randomSeed)
  {
    assert (instanceNames.size() > replicas);
    Collections.sort(instanceNames);

    ZNRecord result = new ZNRecord();
    result.setId(dbName);

    Map<String, Object> externalView = new TreeMap<String, Object>();

    List<Integer> partitionList = new ArrayList<Integer>(partitions);
    for (int i = 0; i < partitions; i++)
    {
      partitionList.add(new Integer(i));
    }
    Random rand = new Random(randomSeed);
    // Shuffle the partition list
    Collections.shuffle(partitionList, rand);

    for (int i = 0; i < partitionList.size(); i++)
    {
      int partitionId = partitionList.get(i);
      Map<String, String> partitionAssignment = new TreeMap<String, String>();
      int masterNode = i % instanceNames.size();
      // the first in the list is the node that contains the master
      partitionAssignment.put(instanceNames.get(masterNode), "MASTER");

      // for the jth replica, we put it on (masterNode + j) % nodes-th
      // node
      for (int j = 1; j <= replicas; j++)
      {
        partitionAssignment
            .put(instanceNames.get((masterNode + j) % instanceNames.size()),
                "SLAVE");
      }
      String partitionName = dbName + ".partition-" + partitionId;
      // externalView.put(partitionName, partitionAssignment);
      result.setMapField(partitionName, partitionAssignment);
    }
    // result.setMapField(dbName, externalView);
    result.setSimpleField("partitions", "" + partitions);
    return result;
  }
}
