package org.apache.helix.messaging;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.Mocks;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.controller.strategy.DefaultTwoStateStrategy;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.LiveInstance.LiveInstanceProperty;
import org.apache.helix.model.Message;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestDefaultMessagingService {
  class MockHelixManager extends Mocks.MockManager {
    class MockDataAccessor extends Mocks.MockAccessor {

      @Override
      public <T extends HelixProperty> T getProperty(PropertyKey key) {

        PropertyType type = key.getType();
        if (type == PropertyType.EXTERNALVIEW || type == PropertyType.IDEALSTATES) {
          return (T) new ExternalView(_externalView);
        }
        return null;
      }

      @Override
      public <T extends HelixProperty> List<T> getChildValues(PropertyKey key) {
        PropertyType type = key.getType();
        List<T> result = new ArrayList<T>();
        Class<? extends HelixProperty> clazz = key.getTypeClass();
        if (type == PropertyType.EXTERNALVIEW || type == PropertyType.IDEALSTATES) {
          HelixProperty typedInstance = HelixProperty.convertToTypedInstance(clazz, _externalView);
          result.add((T) typedInstance);
          return result;
        } else if (type == PropertyType.LIVEINSTANCES) {
          return (List<T>) HelixProperty.convertToTypedList(clazz, _liveInstances);
        }

        return result;
      }
    }

    HelixDataAccessor _accessor = new MockDataAccessor();
    ZNRecord _externalView;
    List<String> _instances;
    List<ZNRecord> _liveInstances;
    String _db = "DB";
    int _replicas = 3;
    int _partitions = 50;

    public MockHelixManager() {
      _liveInstances = new ArrayList<ZNRecord>();
      _instances = new ArrayList<String>();
      for (int i = 0; i < 5; i++) {
        String instance = "localhost_" + (12918 + i);
        _instances.add(instance);
        ZNRecord metaData = new ZNRecord(instance);
        metaData.setSimpleField(LiveInstanceProperty.SESSION_ID.toString(), UUID.randomUUID()
            .toString());
        _liveInstances.add(metaData);
      }
      _externalView =
          DefaultTwoStateStrategy.calculateIdealState(_instances, _partitions, _replicas, _db,
              "MASTER", "SLAVE");

    }

    @Override
    public boolean isConnected() {
      return true;
    }

    @Override
    public HelixDataAccessor getHelixDataAccessor() {
      return _accessor;
    }

    @Override
    public String getInstanceName() {
      return "localhost_12919";
    }

    @Override
    public InstanceType getInstanceType() {
      return InstanceType.PARTICIPANT;
    }
  }

  class TestMessageHandlerFactory implements MessageHandlerFactory {
    class TestMessageHandler extends MessageHandler {

      public TestMessageHandler(Message message, NotificationContext context) {
        super(message, context);
        // TODO Auto-generated constructor stub
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        HelixTaskResult result = new HelixTaskResult();
        result.setSuccess(true);
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        // TODO Auto-generated method stub

      }
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      // TODO Auto-generated method stub
      return new TestMessageHandler(message, context);
    }

    @Override
    public String getMessageType() {
      // TODO Auto-generated method stub
      return "TestingMessageHandler";
    }

    @Override
    public void reset() {
      // TODO Auto-generated method stub

    }
  }

  @Test()
  public void TestMessageSend() {
    HelixManager manager = new MockHelixManager();
    DefaultMessagingService svc = new DefaultMessagingService(manager);
    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    svc.registerMessageHandlerFactory(factory.getMessageType(), factory);

    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setInstanceName("localhost_12919");
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setSelfExcluded(true);

    Message template =
        new Message(factory.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
    AssertJUnit.assertEquals(0, svc.send(recipientCriteria, template));

    recipientCriteria.setSelfExcluded(false);
    AssertJUnit.assertEquals(1, svc.send(recipientCriteria, template));

    // all instances, all partitions
    recipientCriteria.setSelfExcluded(false);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(200, svc.send(recipientCriteria, template));

    // all instances, all partitions, use * instead of %
    recipientCriteria.setSelfExcluded(false);
    recipientCriteria.setInstanceName("*");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("*");
    AssertJUnit.assertEquals(200, svc.send(recipientCriteria, template));

    // tail pattern
    recipientCriteria.setSelfExcluded(false);
    recipientCriteria.setInstanceName("localhost%");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(200, svc.send(recipientCriteria, template));

    // exclude this instance, send to all others for all partitions
    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(159, svc.send(recipientCriteria, template));

    // single instance, all partitions
    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("localhost_12920");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(39, svc.send(recipientCriteria, template));

    // single character wildcards
    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("l_calhost_12_20");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(39, svc.send(recipientCriteria, template));

    // head pattern
    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("%12920");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(39, svc.send(recipientCriteria, template));

    // middle pattern
    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("l%_12920");
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(39, svc.send(recipientCriteria, template));

    // send to a controller
    recipientCriteria.setSelfExcluded(true);
    recipientCriteria.setInstanceName("localhost_12920");
    recipientCriteria.setRecipientInstanceType(InstanceType.CONTROLLER);
    recipientCriteria.setResource("DB");
    recipientCriteria.setPartition("%");
    AssertJUnit.assertEquals(1, svc.send(recipientCriteria, template));
  }
}
