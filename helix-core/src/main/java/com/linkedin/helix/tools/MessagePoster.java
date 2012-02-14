package com.linkedin.helix.tools;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.LiveInstance.LiveInstanceProperty;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.util.HelixUtil;

public class MessagePoster
{
  public void post(Message message)
  {

  }

  public static void main(String[] args)
  {
    String instanceName = "localhost_8900";
    String serverstring = "kgopalak-mn:2181";
    String msgSrc = "cm-instance-0";
    String msgId = "TestMessageId-2";
    String clusterName = "test-cluster";

    ZkClient client = new ZkClient(serverstring);
    client.setZkSerializer(new ZNRecordSerializer());
    Message message = new Message(MessageType.STATE_TRANSITION,msgId);
    message.setMsgId(msgId);
    message.setSrcName(msgSrc);
    message.setTgtName(instanceName);
    message.setMsgState("new");
    message.setFromState("Slave");
    message.setToState("Master");
    message.setPartitionName("EspressoDB.partition-0." + instanceName);
    String path = HelixUtil.getMessagePath(clusterName, instanceName) + "/"
        + message.getId();
    client.delete(path);
    ZNRecord record = client.readData(HelixUtil.getLiveInstancePath(clusterName,
        instanceName));
    message.setTgtSessionId(record.getSimpleField(
        LiveInstanceProperty.SESSION_ID.toString()).toString());
    client.createPersistent(path, message);
  }
}
