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
package com.linkedin.helix.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.Criteria;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.Mocks;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.messaging.DefaultMessagingService;
import com.linkedin.helix.messaging.handling.HelixTaskResult;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.messaging.handling.MessageHandlerFactory;
import com.linkedin.helix.messaging.handling.MessageHandler.ErrorCode;
import com.linkedin.helix.messaging.handling.MessageHandler.ErrorType;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.LiveInstance.LiveInstanceProperty;
import com.linkedin.helix.tools.IdealStateCalculatorForStorageNode;

public class TestDefaultMessagingService
{
  class MockHelixManager extends Mocks.MockManager
  {
    class MockDataAccessor extends Mocks.MockAccessor
    {
      @Override
      public ZNRecord getProperty(PropertyType type, String... keys)
      {
        if(type == PropertyType.EXTERNALVIEW || type == PropertyType.IDEALSTATES)
        {
          return _externalView;
        }
        return null;
      }

      @Override
      public List<ZNRecord> getChildValues(PropertyType type, String... keys)
//      public <T extends HelixProperty> List<T> getChildValues(Class<T> clazz, PropertyType type,
//                                                                  String... keys)
      {
        List<ZNRecord> result = new ArrayList<ZNRecord>();
//        List<T> result = new ArrayList<T>();

        if(type == PropertyType.EXTERNALVIEW || type == PropertyType.IDEALSTATES)
        {
//          result.add(HelixProperty.convertInstance(clazz, _externalView));
          result.add(_externalView);
          return result;
        }
        else if(type == PropertyType.LIVEINSTANCES)
        {
//          return HelixProperty.convertList(clazz, _liveInstances);
          return _liveInstances;
        }

        return result;
      }
    }

    DataAccessor _accessor = new MockDataAccessor();
    ZNRecord _externalView;
    List<String> _instances;
    List<ZNRecord> _liveInstances;
    String _db = "DB";
    int _replicas = 3;
    int _partitions = 50;

    public MockHelixManager()
    {
      _liveInstances = new ArrayList<ZNRecord>();
      _instances = new ArrayList<String>();
      for(int i = 0;i<5; i++)
      {
        String instance = "localhost_"+(12918+i);
        _instances.add(instance);
        ZNRecord metaData = new ZNRecord(instance);
        metaData.setSimpleField(LiveInstanceProperty.SESSION_ID.toString(),
            UUID.randomUUID().toString());
        _liveInstances.add(metaData);
      }
      _externalView = IdealStateCalculatorForStorageNode.calculateIdealState(
          _instances, _partitions, _replicas, _db, "MASTER", "SLAVE");

    }

    @Override
    public boolean isConnected()
    {
      return true;
    }

    @Override
    public DataAccessor getDataAccessor()
    {
      return _accessor;
    }


    @Override
    public String getInstanceName()
    {
      return "localhost_12919";
    }

    @Override
    public InstanceType getInstanceType()
    {
      return InstanceType.PARTICIPANT;
    }
  }

  class TestMessageHandlerFactory implements MessageHandlerFactory
  {
    class TestMessageHandler extends MessageHandler
    {

      public TestMessageHandler(Message message, NotificationContext context)
      {
        super(message, context);
        // TODO Auto-generated constructor stub
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException
      {
        HelixTaskResult result = new HelixTaskResult();
        result.setSuccess(true);
        return result;
      }

      @Override
      public void onError( Exception e, ErrorCode code, ErrorType type)
      {
        // TODO Auto-generated method stub
        
      }
    }
    @Override
    public MessageHandler createHandler(Message message,
        NotificationContext context)
    {
      // TODO Auto-generated method stub
      return new TestMessageHandler(message, context);
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

  @Test()
  public void TestMessageSend()
  {
    HelixManager manager = new MockHelixManager();
    DefaultMessagingService svc = new DefaultMessagingService(manager);
    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    svc.registerMessageHandlerFactory(factory.getMessageType(), factory);

    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setInstanceName("localhost_12919");
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setSelfExcluded(true);

    Message template = new Message(factory.getMessageType(), UUID.randomUUID().toString());
    AssertJUnit.assertEquals(0, svc.send(recipientCriteria, template));

    recipientCriteria.setSelfExcluded(false);
    AssertJUnit.assertEquals(1, svc.send(recipientCriteria, template));


    recipientCriteria.setSelfExcluded(false);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(200, svc.send(recipientCriteria, template));

    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(159, svc.send(recipientCriteria, template));

    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(159, svc.send(recipientCriteria, template));

    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("localhost_12920");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(39, svc.send(recipientCriteria, template));


    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("localhost_12920");
    recipientCriteria.setRecipientInstanceType(InstanceType.CONTROLLER);
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(1, svc.send(recipientCriteria, template));
  }
}
