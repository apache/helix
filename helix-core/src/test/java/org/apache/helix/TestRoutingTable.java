package org.apache.helix;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.helix.mock.MockManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRoutingTable {
  NotificationContext changeContext = null;

  @BeforeClass()
  public synchronized void setup() {

    final String[] array = new String[] {
        "localhost_8900", "localhost_8901"
    };
    HelixManager manager = new MockManager() {
      private MockAccessor _mockAccessor;

      @Override
      // public DataAccessor getDataAccessor()
      public HelixDataAccessor getHelixDataAccessor() {
        if (_mockAccessor == null) {
          _mockAccessor = new MockAccessor() {
            @SuppressWarnings("unchecked")
            @Override
            public <T extends HelixProperty> List<T> getChildValues(PropertyKey key)
            {
              PropertyType type = key.getType();
              String[] keys = key.getParams();
              if (type == PropertyType.CONFIGS && keys != null && keys.length > 1
                  && keys[1].equalsIgnoreCase(ConfigScopeProperty.PARTICIPANT.toString())) {
                List<InstanceConfig> configs = new ArrayList<InstanceConfig>();
                for (String instanceName : array) {
                  InstanceConfig config = new InstanceConfig(instanceName);
                  String[] splits = instanceName.split("_");
                  config.setHostName(splits[0]);
                  config.setPort(splits[1]);
                  configs.add(config);
                }
                return (List<T>) configs;
              }
              return Collections.emptyList();
            };
          };
        }
        return _mockAccessor;
      }
    };
    changeContext = new NotificationContext(manager);
  }

  @Test()
  public void testNullAndEmpty() {
    RoutingTableProvider routingTable = new RoutingTableProvider();
    try {
      routingTable.onExternalViewChange(null, changeContext);
      List<ExternalView> list = Collections.emptyList();
      routingTable.onExternalViewChange(list, changeContext);
    } finally {
      routingTable.shutdown();
    }
  }

  @Test()
  public void testSimple() {
    List<InstanceConfig> instances;
    RoutingTableProvider routingTable = new RoutingTableProvider();
    ZNRecord record = new ZNRecord("TESTDB");

    try {
      // one master
      add(record, "TESTDB_0", "localhost_8900", "MASTER");
      List<ExternalView> externalViewList = new ArrayList<>();
      externalViewList.add(new ExternalView(record));
      routingTable.onExternalViewChange(externalViewList, changeContext);

      instances = routingTable.getInstances("TESTDB", "TESTDB_0", "MASTER");
      AssertJUnit.assertNotNull(instances);
      AssertJUnit.assertEquals(instances.size(), 1);

      // additions
      add(record, "TESTDB_0", "localhost_8901", "MASTER");
      add(record, "TESTDB_1", "localhost_8900", "SLAVE");

      externalViewList = new ArrayList<ExternalView>();
      externalViewList.add(new ExternalView(record));
      routingTable.onExternalViewChange(externalViewList, changeContext);

      instances = routingTable.getInstances("TESTDB", "TESTDB_0", "MASTER");
      AssertJUnit.assertNotNull(instances);
      AssertJUnit.assertEquals(instances.size(), 2);

      instances = routingTable.getInstances("TESTDB", "TESTDB_1", "SLAVE");
      AssertJUnit.assertNotNull(instances);
      AssertJUnit.assertEquals(instances.size(), 1);

      // updates
      add(record, "TESTDB_0", "localhost_8901", "SLAVE");
      externalViewList = new ArrayList<>();
      externalViewList.add(new ExternalView(record));
      routingTable.onExternalViewChange(externalViewList, changeContext);
      instances = routingTable.getInstances("TESTDB", "TESTDB_0", "SLAVE");
      AssertJUnit.assertNotNull(instances);
      AssertJUnit.assertEquals(instances.size(), 1);
    } finally {
      routingTable.shutdown();
    }
  }


  @Test()
  public void testGetResources() {
    RoutingTableProvider routingTable = new RoutingTableProvider();
    List<ExternalView> externalViewList = new ArrayList<>();
    Set<String> databases = new HashSet<>();

    try {
      for (int i = 0; i < 5; i++) {
        String db = "TESTDB" + i;
        ZNRecord record = new ZNRecord(db);
        // one master
        add(record, db + "_0", "localhost_8900", "MASTER");
        add(record, db + "_1", "localhost_8901", "SLAVE");
        externalViewList.add(new ExternalView(record));
        databases.add(db);
      }

      routingTable.onExternalViewChange(externalViewList, changeContext);
      Collection<String> resources = routingTable.getResources();
      Assert.assertEquals(databases.size(), externalViewList.size());
      Assert.assertEquals(databases, new HashSet<>(resources));
    } finally {
      routingTable.shutdown();
    }
  }

  @Test()
  public void testStateUnitGroupDeletion() throws InterruptedException {
    List<InstanceConfig> instances;
    RoutingTableProvider routingTable = new RoutingTableProvider();

    try {
      List<ExternalView> externalViewList = new ArrayList<ExternalView>();
      ZNRecord record = new ZNRecord("TESTDB");

      // one master
      add(record, "TESTDB_0", "localhost_8900", "MASTER");
      externalViewList.add(new ExternalView(record));
      externalViewList.add(new ExternalView(new ZNRecord("fake")));
      routingTable.onExternalViewChange(externalViewList, changeContext);
      instances = routingTable.getInstances("TESTDB", "TESTDB_0", "MASTER");
      AssertJUnit.assertNotNull(instances);
      AssertJUnit.assertEquals(instances.size(), 1);

      externalViewList.remove(0);
      routingTable.onExternalViewChange(externalViewList, changeContext);
      Thread.sleep(100);
      instances = routingTable.getInstances("TESTDB", "TESTDB_0", "MASTER");
      AssertJUnit.assertNotNull(instances);
      AssertJUnit.assertEquals(instances.size(), 0);
    } finally {
      routingTable.shutdown();
    }
  }

  @Test()
  public void testGetInstanceForAllStateUnits() {
    List<InstanceConfig> instancesList;
    Set<InstanceConfig> instancesSet;
    InstanceConfig instancesArray[];
    RoutingTableProvider routingTable = new RoutingTableProvider();

    try {
      List<ExternalView> externalViewList = new ArrayList<ExternalView>();
      ZNRecord record = new ZNRecord("TESTDB");

      // one master
      add(record, "TESTDB_0", "localhost_8900", "MASTER");
      add(record, "TESTDB_1", "localhost_8900", "MASTER");
      add(record, "TESTDB_2", "localhost_8900", "MASTER");
      add(record, "TESTDB_3", "localhost_8900", "SLAVE");
      add(record, "TESTDB_4", "localhost_8900", "SLAVE");
      add(record, "TESTDB_5", "localhost_8900", "SLAVE");

      add(record, "TESTDB_0", "localhost_8901", "SLAVE");
      add(record, "TESTDB_1", "localhost_8901", "SLAVE");
      add(record, "TESTDB_2", "localhost_8901", "SLAVE");
      add(record, "TESTDB_3", "localhost_8901", "MASTER");
      add(record, "TESTDB_4", "localhost_8901", "MASTER");
      add(record, "TESTDB_5", "localhost_8901", "MASTER");

      externalViewList.add(new ExternalView(record));
      routingTable.onExternalViewChange(externalViewList, changeContext);
      instancesList = routingTable.getInstances("TESTDB", "TESTDB_0", "MASTER");
      AssertJUnit.assertNotNull(instancesList);
      AssertJUnit.assertEquals(instancesList.size(), 1);
      instancesSet = routingTable.getInstances("TESTDB", "MASTER");
      AssertJUnit.assertNotNull(instancesSet);
      AssertJUnit.assertEquals(instancesSet.size(), 2);
      instancesSet = routingTable.getInstances("TESTDB", "SLAVE");
      AssertJUnit.assertNotNull(instancesSet);
      AssertJUnit.assertEquals(instancesSet.size(), 2);
      instancesArray = new InstanceConfig[instancesSet.size()];
      instancesSet.toArray(instancesArray);
      AssertJUnit.assertEquals(instancesArray[0].getHostName(), "localhost");
      AssertJUnit.assertEquals(instancesArray[0].getPort(), "8900");
      AssertJUnit.assertEquals(instancesArray[1].getHostName(), "localhost");
      AssertJUnit.assertEquals(instancesArray[1].getPort(), "8901");
    } finally {
      routingTable.shutdown();
    }
  }

  @Test()
  public void testMultiThread() throws Exception {
    final RoutingTableProvider routingTable = new RoutingTableProvider();
    List<ExternalView> externalViewList = new ArrayList<>();

    try {
      ZNRecord record = new ZNRecord("TESTDB");
      for (int i = 0; i < 1000; i++) {
        add(record, "TESTDB_" + i, "localhost_8900", "MASTER");
      }
      externalViewList.add(new ExternalView(record));
      routingTable.onExternalViewChange(externalViewList, changeContext);
      Callable<Boolean> runnable = new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {

          try {
            int count = 0;
            while (count < 100) {
              List<InstanceConfig> instancesList =
                  routingTable.getInstances("TESTDB", "TESTDB_0", "MASTER");
              AssertJUnit.assertEquals(instancesList.size(), 1);
              // System.out.println(System.currentTimeMillis() + "-->"
              // + instancesList.size());

              Thread.sleep(5);

              count++;
            }
          } catch (InterruptedException e) {
            // e.printStackTrace();
          }
          return true;
        }
      };
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
      Future<Boolean> submit = executor.submit(runnable);
      int count = 0;
      while (count < 10) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        routingTable.onExternalViewChange(externalViewList, changeContext);
        count++;
      }

      Boolean result = submit.get(60, TimeUnit.SECONDS);
      AssertJUnit.assertEquals(result, Boolean.TRUE);
    } finally {
      routingTable.shutdown();
    }
  }

  private void add(ZNRecord record, String stateUnitKey, String instanceName, String state) {
    Map<String, String> stateUnitKeyMap = record.getMapField(stateUnitKey);
    if (stateUnitKeyMap == null) {
      stateUnitKeyMap = new HashMap<>();
      record.setMapField(stateUnitKey, stateUnitKeyMap);
    }
    stateUnitKeyMap.put(instanceName, state);
    record.setMapField(stateUnitKey, stateUnitKeyMap);
  }
}
