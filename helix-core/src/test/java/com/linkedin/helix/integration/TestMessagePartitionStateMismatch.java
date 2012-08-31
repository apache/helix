package com.linkedin.helix.integration;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageState;
import com.linkedin.helix.model.Message.MessageType;

public class TestMessagePartitionStateMismatch extends ZkStandAloneCMTestBase
{
  @Test
  public void testStateMismatch() throws InterruptedException
  {
    String controllerName = CONTROLLER_PREFIX + "_0";
    
    HelixManager manager = _startCMResultMap.get(controllerName)._manager;
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder kb = accessor.keyBuilder();
    ExternalView ev = accessor.getProperty(kb.externalView(TEST_DB));
    Map<String, LiveInstance> liveinstanceMap = accessor.getChildValuesMap(accessor.keyBuilder().liveInstances());
    
    for(String instanceName : liveinstanceMap.keySet())
    {
      String sessionid = liveinstanceMap.get(instanceName).getSessionId();
      for(String partition : ev.getPartitionSet())
      {
        if(ev.getStateMap(partition).containsKey(instanceName))
        {
          String uuid = UUID.randomUUID().toString();
          Message message = new Message(MessageType.STATE_TRANSITION, uuid);
          boolean rand = new Random().nextInt(10) > 5;
          if(ev.getStateMap(partition).get(instanceName).equals("MASTER"))
          {
            message.setSrcName(manager.getInstanceName());
            message.setTgtName(instanceName);
            message.setMsgState(MessageState.NEW);
            message.setPartitionName(partition);
            message.setResourceName(TEST_DB);
            message.setFromState(rand ? "SLAVE" : "OFFLINE");
            message.setToState(rand ? "MASTER" : "SLAVE");
            message.setTgtSessionId(sessionid);
            message.setSrcSessionId(manager.getSessionId());
            message.setStateModelDef("MasterSlave");
            message.setStateModelFactoryName("DEFAULT"); 
          }
          else if (ev.getStateMap(partition).get(instanceName).equals("SLAVE"))
          {
            message.setSrcName(manager.getInstanceName());
            message.setTgtName(instanceName);
            message.setMsgState(MessageState.NEW);
            message.setPartitionName(partition);
            message.setResourceName(TEST_DB);
            message.setFromState(rand ? "MASTER" : "OFFLINE");
            message.setToState(rand ? "SLAVE" : "SLAVE");
            message.setTgtSessionId(sessionid);
            message.setSrcSessionId(manager.getSessionId());
            message.setStateModelDef("MasterSlave");
            message.setStateModelFactoryName("DEFAULT");
          }
          accessor.setProperty(accessor.keyBuilder().message(instanceName, message.getMsgId()), message);
        }
      }
    }
    Thread.sleep(3000);
    ExternalView ev2 = accessor.getProperty(kb.externalView(TEST_DB));
    Assert.assertTrue(ev.equals(ev2));
  }
}
