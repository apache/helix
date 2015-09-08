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

import java.util.List;
import java.util.Map;

import java.util.Set;
import java.util.UUID;
import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.Mocks;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.strategy.DefaultTwoStateStrategy;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link CriteriaEvaluator}.
 *
 * @author liyinan926
 */
@Test(groups = { "org.apache.helix.messaging" })
public class CriteriaEvaluatorTest {

  private static final String RESOURCE_NAME = "DB";
  private static final int REPLICAS = 1;
  private static final int PARTITIONS = 20;
  private static final int PARTICIPANTS = 5;
  private static final String INSTANCE_NAME_PREFIX = "localhost_";

  private HelixManager _helixManager;

  @BeforeClass
  public void setUp() throws Exception {
    _helixManager = new MockHelixManager();
    _helixManager.connect();
  }

  @Test
  public void testEvaluateCriteria() {
    CriteriaEvaluator criteriaEvaluator = new CriteriaEvaluator();

    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource("%");
    recipientCriteria.setPartition("%");
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setDataSource(Criteria.DataSource.LIVEINSTANCES);

    List<Map<String, String>> result = criteriaEvaluator.evaluateCriteria(recipientCriteria, _helixManager);
    Assert.assertEquals(result.size(), 5);

    Set<String> expectedInstances = Sets.newHashSet();
    for (int i = 0; i < PARTICIPANTS; i++) {
      expectedInstances.add(INSTANCE_NAME_PREFIX + i);
    }

    Set<String> actualInstances = Sets.newHashSet();
    for (Map<String, String> row : result) {
      actualInstances.add(row.get("instanceName"));
    }

    Assert.assertEquals(actualInstances, expectedInstances);

    recipientCriteria.setInstanceName(INSTANCE_NAME_PREFIX + 2);
    result = criteriaEvaluator.evaluateCriteria(recipientCriteria, _helixManager);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).get("instanceName"), INSTANCE_NAME_PREFIX + 2);

    recipientCriteria.setInstanceName("%");
    recipientCriteria.setDataSource(Criteria.DataSource.IDEALSTATES);
    result = criteriaEvaluator.evaluateCriteria(recipientCriteria, _helixManager);
    Assert.assertEquals(result.size(), 40);

    recipientCriteria.setInstanceName(INSTANCE_NAME_PREFIX + 4);
    result = criteriaEvaluator.evaluateCriteria(recipientCriteria, _helixManager);
    Assert.assertEquals(result.size(), 8);
  }

  @AfterClass
  public void tearDown() {
    this._helixManager.disconnect();
  }

  private static class MockHelixManager extends Mocks.MockManager {

    @SuppressWarnings("unchecked")
    private class MockDataAccessor extends Mocks.MockAccessor {

      @Override
      public <T extends HelixProperty> T getProperty(PropertyKey key) {
        PropertyType type = key.getType();

        if (type == PropertyType.EXTERNALVIEW || type == PropertyType.IDEALSTATES) {
          return (T) new IdealState(_idealState);
        }

        return null;
      }

      @Override
      public <T extends HelixProperty> List<T> getChildValues(PropertyKey key) {
        PropertyType type = key.getType();
        Class<? extends HelixProperty> clazz = key.getTypeClass();
        if (type == PropertyType.EXTERNALVIEW || type == PropertyType.IDEALSTATES) {
          HelixProperty typedInstance = HelixProperty.convertToTypedInstance(clazz, _idealState);
          return Lists.newArrayList((T) typedInstance);
        } else if (type == PropertyType.INSTANCES || type == PropertyType.LIVEINSTANCES) {
          return (List<T>) HelixProperty.convertToTypedList(clazz, _liveInstances);
        }

        return Lists.newArrayList();
      }
    }

    private final HelixDataAccessor _accessor = new MockDataAccessor();
    private final ZNRecord _idealState;
    private final List<String> _instances = Lists.newArrayList();
    private final List<ZNRecord> _liveInstances = Lists.newArrayList();

    public MockHelixManager() {
      for (int i = 0; i < PARTICIPANTS; i++) {
        String instance = INSTANCE_NAME_PREFIX + i;
        _instances.add(instance);
        ZNRecord metaData = new ZNRecord(instance);
        metaData.setSimpleField(LiveInstance.LiveInstanceProperty.SESSION_ID.toString(), UUID.randomUUID().toString());
        _liveInstances.add(metaData);
      }

      _idealState = DefaultTwoStateStrategy.calculateIdealState(_instances, PARTITIONS, REPLICAS, RESOURCE_NAME,
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
      return "localhost_0";
    }

    @Override
    public InstanceType getInstanceType() {
      return InstanceType.CONTROLLER;
    }
  }
}
