package org.apache.helix.alerts;

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

import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.Mocks.MockManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.stages.HealthDataCache;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestAddAlerts {

  protected static final String CLUSTER_NAME = "TestCluster";

  MockManager _helixManager;
  AlertsHolder _alertsHolder;

  public final String EXP = AlertParser.EXPRESSION_NAME;
  public final String CMP = AlertParser.COMPARATOR_NAME;
  public final String CON = AlertParser.CONSTANT_NAME;

  @BeforeMethod()
  public void setup() {
    _helixManager = new MockManager(CLUSTER_NAME);
    _alertsHolder = new AlertsHolder(_helixManager, new HealthDataCache());
  }

  public boolean alertRecordContains(ZNRecord rec, String alertName) {
    Map<String, Map<String, String>> alerts = rec.getMapFields();
    return alerts.containsKey(alertName);
  }

  public int alertsSize(ZNRecord rec) {
    Map<String, Map<String, String>> alerts = rec.getMapFields();
    return alerts.size();
  }

  @Test()
  public void testAddAlert() throws Exception {
    String alert =
        EXP + "(accumulate()(dbFoo.partition10.latency))" + CMP + "(GREATER)" + CON + "(10)";
    _alertsHolder.addAlert(alert);
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.alerts()).getRecord();
    System.out.println("alert: " + alert);
    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(alertRecordContains(rec, alert));
    AssertJUnit.assertEquals(1, alertsSize(rec));
  }

  @Test()
  public void testAddTwoAlerts() throws Exception {
    String alert1 =
        EXP + "(accumulate()(dbFoo.partition10.latency))" + CMP + "(GREATER)" + CON + "(10)";
    String alert2 =
        EXP + "(accumulate()(dbFoo.partition10.latency))" + CMP + "(GREATER)" + CON + "(100)";
    _alertsHolder.addAlert(alert1);
    _alertsHolder.addAlert(alert2);

    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.alerts()).getRecord();
    // System.out.println("alert: "+alert1);
    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(alertRecordContains(rec, alert1));
    AssertJUnit.assertTrue(alertRecordContains(rec, alert2));
    AssertJUnit.assertEquals(2, alertsSize(rec));
  }

  @Test(groups = {
    "unitTest"
  })
  public void testAddTwoWildcardAlert() throws Exception {
    String alert1 =
        EXP + "(accumulate()(dbFoo.partition*.put*))" + CMP + "(GREATER)" + CON + "(10)";
    _alertsHolder.addAlert(alert1);

    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.alerts()).getRecord();
    // System.out.println("alert: "+alert1);
    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(alertRecordContains(rec, alert1));
    AssertJUnit.assertEquals(1, alertsSize(rec));
  }

  // add 2 wildcard alert here
}
