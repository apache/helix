package com.linkedin.clustermanager.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.Criteria;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.Mocks;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.messaging.handling.MessageHandler;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;

public class TestDefaultMessagingService
{
  class MockClusterManager extends Mocks.MockManager
  {
    class MockDataAccessor extends Mocks.MockAccessor
    {
      @Override
      public List<ZNRecord> getChildValues(PropertyType type, String... keys)
      {
        List<ZNRecord> result = new ArrayList<ZNRecord>();
        if(type == PropertyType.EXTERNALVIEW)
        {
          result.add(_externalView);
          return result;
        }
        else if(type == PropertyType.LIVEINSTANCES)
        {
          return _liveInstances;
        }
        
        return result;
      }
    }
    
    ClusterDataAccessor _accessor = new MockDataAccessor();
    ZNRecord _externalView;
    List<String> _instances;
    List<ZNRecord> _liveInstances;
    String _db = "DB";
    int _replicas = 3;
    int _partitions = 50;

    public MockClusterManager()
    {
      _liveInstances = new ArrayList<ZNRecord>();
      _instances = new ArrayList<String>();
      for(int i = 0;i<5; i++)
      {
        String instance = "localhost_"+(12918+i);
        _instances.add(instance);
        ZNRecord metaData = new ZNRecord(instance);
        metaData.setSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString(),
            UUID.randomUUID().toString());
        
      }
      _externalView = IdealStateCalculatorForStorageNode.calculateIdealState(
          _instances, _partitions, _replicas, _db, "MASTER", "SLAVE");
      
    }
    
    public boolean isConnected()
    {
      return true;
    }
    
    public ClusterDataAccessor getDataAccessor()
    {
      return _accessor;
    }
    
    
    public String getInstanceName()
    {
      return "localhost_12919";
    }
    
    public InstanceType getInstanceType()
    {
      return InstanceType.PARTICIPANT;
    }
  }
  
  class TestMessageHandlerFactory implements MessageHandlerFactory
  {
    class TestMessageHandler implements MessageHandler
    {

      @Override
      public void handleMessage(Message message, NotificationContext context,
          Map<String, String> resultMap) throws InterruptedException
      {
        // TODO Auto-generated method stub
        
      }
      
    }
    @Override
    public MessageHandler createHandler(Message message,
        NotificationContext context)
    {
      // TODO Auto-generated method stub
      return new TestMessageHandler();
    }

    @Override
    public String getMessageType()
    {
      // TODO Auto-generated method stub
      return "TestingMessageHandler";
    }

    @Override
    public void reset()
    {
      // TODO Auto-generated method stub
      
    }
  }
  
  @Test(groups =
  { "unitTest" })
  public void TestMessageSend()
  {
    ClusterManager manager = new MockClusterManager();
    DefaultMessagingService svc = new DefaultMessagingService(manager);
    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    svc.registerMessageHandlerFactory(factory.getMessageType(), factory);
    
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setInstanceName("localhost_12919");
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setSelfExcluded(true);
    
    Message template = new Message(factory.getMessageType(), UUID.randomUUID().toString());
    Assert.assertEquals(0, svc.send(recipientCriteria, template));
    
    recipientCriteria.setSelfExcluded(false);
    Assert.assertEquals(1, svc.send(recipientCriteria, template));
    
    
    recipientCriteria.setSelfExcluded(false);
    recipientCriteria.setInstanceName("*");
    recipientCriteria.setResourceGroup("DB");
    recipientCriteria.setResourceKey("*");
    Assert.assertEquals(200, svc.send(recipientCriteria, template));
    
    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("*");
    recipientCriteria.setResourceGroup("DB");
    recipientCriteria.setResourceKey("*");
    Assert.assertEquals(159, svc.send(recipientCriteria, template));
    
    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("*");
    recipientCriteria.setResourceGroup("DB");
    recipientCriteria.setResourceKey("*");
    Assert.assertEquals(159, svc.send(recipientCriteria, template));
    
    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("localhost_12920");
    recipientCriteria.setResourceGroup("DB");
    recipientCriteria.setResourceKey("*");
    Assert.assertEquals(39, svc.send(recipientCriteria, template));
    
    
    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("localhost_12920");
    recipientCriteria.setRecipientInstanceType(InstanceType.CONTROLLER);
    recipientCriteria.setResourceGroup("DB");
    recipientCriteria.setResourceKey("*");
    Assert.assertEquals(1, svc.send(recipientCriteria, template));
  }
}
