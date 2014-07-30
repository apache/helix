package org.apache.helix.manager.zk;

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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZKLiveInstanceData extends ZkTestBase {
  private final String clusterName = "TestZKLiveInstanceData";

  @Test
  public void testDataChange() throws Exception {
    // Create an admin and add LiveInstanceChange listener to it
    HelixManager adminManager =
        HelixManagerFactory.getZKHelixManager(clusterName, null, InstanceType.ADMINISTRATOR,
            _zkaddr);
    adminManager.connect();
    final BlockingQueue<List<LiveInstance>> changeList =
        new LinkedBlockingQueue<List<LiveInstance>>();

    adminManager.addLiveInstanceChangeListener(new LiveInstanceChangeListener() {
      @Override
      public void onLiveInstanceChange(List<LiveInstance> liveInstances,
          NotificationContext changeContext) {
        // The queue is basically unbounded, so shouldn't throw exception when calling
        // "add".
        changeList.add(deepCopy(liveInstances));
      }
    });

    // Check the initial condition
    List<LiveInstance> instances = changeList.poll(1, TimeUnit.SECONDS);
    Assert.assertNotNull(instances, "Expecting a list of live instance");
    Assert.assertTrue(instances.isEmpty(), "Expecting an empty list of live instance");
    // Join as participant, should trigger a live instance change event
    HelixManager manager =
        HelixManagerFactory.getZKHelixManager(clusterName, "localhost_54321",
            InstanceType.PARTICIPANT, _zkaddr);
    manager.connect();
    instances = changeList.poll(1, TimeUnit.SECONDS);
    Assert.assertNotNull(instances, "Expecting a list of live instance");
    Assert.assertEquals(instances.size(), 1, "Expecting one live instance");
    Assert.assertEquals(instances.get(0).getInstanceName(), manager.getInstanceName());
    // Update data in the live instance node, should trigger another live instance change
    // event
    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    PropertyKey propertyKey =
        helixDataAccessor.keyBuilder().liveInstance(manager.getInstanceName());
    LiveInstance instance = helixDataAccessor.getProperty(propertyKey);

    Map<String, String> map = new TreeMap<String, String>();
    map.put("k1", "v1");
    instance.getRecord().setMapField("test", map);
    Assert.assertTrue(helixDataAccessor.updateProperty(propertyKey, instance),
        "Failed to update live instance node");

    instances = changeList.poll(1, TimeUnit.SECONDS);
    Assert.assertNotNull(instances, "Expecting a list of live instance");
    Assert.assertEquals(instances.get(0).getRecord().getMapField("test"), map, "Wrong map data.");
    manager.disconnect();
    Thread.sleep(1000); // wait for callback finish

    instances = changeList.poll(1, TimeUnit.SECONDS);
    Assert.assertNotNull(instances, "Expecting a list of live instance");
    Assert.assertTrue(instances.isEmpty(), "Expecting an empty list of live instance");

    adminManager.disconnect();

  }

  @BeforeClass()
  public void beforeClass() throws Exception {
    ClusterSetup.processCommandLineArgs(getArgs("-zkSvr", _zkaddr, "-addCluster", clusterName));
    ClusterSetup.processCommandLineArgs(getArgs("-zkSvr", _zkaddr, "-addNode", clusterName,
        "localhost:54321"));
    ClusterSetup.processCommandLineArgs(getArgs("-zkSvr", _zkaddr, "-addNode", clusterName,
        "localhost:54322"));
  }

  private String[] getArgs(String... args) {
    return args;
  }

  private List<LiveInstance> deepCopy(List<LiveInstance> instances) {
    List<LiveInstance> result = new ArrayList<LiveInstance>();
    for (LiveInstance instance : instances) {
      result.add(new LiveInstance(instance.getRecord()));
    }
    return result;
  }
}
