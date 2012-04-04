package com.linkedin.helix.tools;

import java.util.UUID;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.LiveInstance.LiveInstanceProperty;
import com.linkedin.helix.model.Message.MessageState;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.util.HelixUtil;

public class MessagePoster
{
  public void post(String zkServer,
                   Message message,
                   String clusterName,
                   String instanceName)
  {
    ZkClient client = new ZkClient(zkServer);
    client.setZkSerializer(new ZNRecordSerializer());
    String path = HelixUtil.getMessagePath(clusterName, instanceName) + "/" + message.getId();
    client.delete(path);
    ZNRecord record = client.readData(HelixUtil.getLiveInstancePath(clusterName, instanceName));
    message.setTgtSessionId(record.getSimpleField(LiveInstanceProperty.SESSION_ID.toString()).toString());
    message.setTgtName(record.getId());
    //System.out.println(message);
    client.createPersistent(path, message.getRecord());
  }

  public void postFaultInjectionMessage(String zkServer,
                                        String clusterName,
                                        String instanceName,
                                        String payloadString,
                                        String partition)
  {
    Message message = new Message("FaultInjection", UUID.randomUUID().toString());
    if(payloadString != null)
    {
      message.getRecord().setSimpleField("faultType", payloadString);
    }
    if(partition != null)
    {
      message.setPartitionName(partition);
    }
    
    post(zkServer, message, clusterName, instanceName);
  }

  public void postTestMessage(String zkServer, String clusterName, String instanceName)
  {
    String msgSrc = "cm-instance-0";
    String msgId = "TestMessageId-2";

    Message message = new Message(MessageType.STATE_TRANSITION, msgId);
    message.setMsgId(msgId);
    message.setSrcName(msgSrc);
    message.setTgtName(instanceName);
    message.setMsgState(MessageState.NEW);
    message.setFromState("Slave");
    message.setToState("Master");
    message.setPartitionName("EspressoDB.partition-0." + instanceName);

    post(zkServer, message, clusterName, instanceName);
  }

  public static void main(String[] args)
  {
    if (args.length < 4 || args.length > 6)      
    {
      System.err.println("Usage: java " + MessagePoster.class.getName()
          + " zkServer cluster instance msgType [payloadString] [partition]");
      System.err.println("msgType can be one of test, fault");
      System.err.println("payloadString is sent along with the fault msgType");
      System.exit(1);
    }
    String zkServer = args[0];
    String cluster = args[1];
    String instance = args[2];
    String msgType = args[3];
    String payloadString = (args.length >= 5 ? args[4] : null);
    String partition = (args.length == 6 ? args[5] : null);

    MessagePoster messagePoster = new MessagePoster();
    if (msgType.equals("test"))
    {
      messagePoster.postTestMessage(zkServer, cluster, instance);
    }
    else if (msgType.equals("fault"))
    {
      messagePoster.postFaultInjectionMessage(zkServer,
                                              cluster,
                                              instance,
                                              payloadString,
                                              partition);
      System.out.println("Posted " + msgType);
    }
    else
    {
      System.err.println("Message was not posted. Unknown msgType:" + msgType);
    }
  }
}
