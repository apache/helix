package com.linkedin.helix.mock.controller;

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

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.IdealState.IdealStateProperty;
import com.linkedin.helix.model.LiveInstance.LiveInstanceProperty;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.util.HelixUtil;

public class MockController
{
  private final ZkClient client;
  private final String srcName;
  private final String clusterName;

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
    Message message = new Message(MessageType.STATE_TRANSITION, msgId);
    message.setMsgId(msgId);
    message.setSrcName(srcName);
    message.setTgtName(instanceName);
    message.setMsgState("new");
    message.setFromState(fromState);
    message.setToState(toState);
    // message.setPartitionId(partitionId);
    message.setPartitionName(partitionKey);

    String path = HelixUtil.getMessagePath(clusterName, instanceName) + "/"
        + message.getId();
    ObjectMapper mapper = new ObjectMapper();
    StringWriter sw = new StringWriter();
    mapper.writeValueUsingView(sw, message, Message.class);
    System.out.println(sw.toString());
    client.delete(path);

    Thread.sleep(10000);
    ZNRecord record = client.readData(HelixUtil.getLiveInstancePath(clusterName,
        instanceName));
    message.setTgtSessionId(record.getSimpleField(
        LiveInstanceProperty.SESSION_ID.toString()).toString());
    client.createPersistent(path, message);
  }

  public void createExternalView(List<String> instanceNames, int partitions,
      int replicas, String dbName, long randomSeed)
  {
    DataAccessor dataAccessor = new ZKDataAccessor(clusterName, client);

    ExternalView externalView = new ExternalView(computeRoutingTable(instanceNames, partitions,
                                                                     replicas, dbName, randomSeed));

    dataAccessor.setProperty(PropertyType.EXTERNALVIEW, externalView, dbName);
  }

  public ZNRecord computeRoutingTable(List<String> instanceNames,
      int partitions, int replicas, String dbName, long randomSeed)
  {
    assert (instanceNames.size() > replicas);
    Collections.sort(instanceNames);

    ZNRecord result = new ZNRecord(dbName);

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
      result.setMapField(partitionName, partitionAssignment);
    }
    result.setSimpleField(IdealStateProperty.NUM_PARTITIONS.toString(), "" + partitions);
    return result;
  }
}
