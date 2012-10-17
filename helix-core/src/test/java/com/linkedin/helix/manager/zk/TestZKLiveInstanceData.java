package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.tools.ClusterSetup;

public class TestZKLiveInstanceData extends ZkUnitTestBase
{
  private final String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();

  @Test
  public void testDataChange() throws Exception
  {
    // Create an admin and add LiveInstanceChange listener to it
    HelixManager adminManager =
        HelixManagerFactory.getZKHelixManager(clusterName,
                                              null,
                                              InstanceType.ADMINISTRATOR,
                                              ZK_ADDR);
    adminManager.connect();
    final BlockingQueue<List<LiveInstance>> changeList =
        new LinkedBlockingQueue<List<LiveInstance>>();

    adminManager.addLiveInstanceChangeListener(new LiveInstanceChangeListener()
    {
      @Override
      public void onLiveInstanceChange(List<LiveInstance> liveInstances,
                                       NotificationContext changeContext)
      {
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
        HelixManagerFactory.getZKHelixManager(clusterName,
                                              "localhost_54321",
                                              InstanceType.PARTICIPANT,
                                              ZK_ADDR);
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
    Assert.assertEquals(instances.get(0).getRecord().getMapField("test"),
                        map,
                        "Wrong map data.");
    manager.disconnect();
    Thread.sleep(1000); // wait for callback finish

    instances = changeList.poll(1, TimeUnit.SECONDS);
    Assert.assertNotNull(instances, "Expecting a list of live instance");
    Assert.assertTrue(instances.isEmpty(), "Expecting an empty list of live instance");

    adminManager.disconnect();

  }

  @BeforeClass()
  public void beforeClass() throws Exception
  {
    ZkClient zkClient = null;
    try
    {
      zkClient = new ZkClient(ZK_ADDR);
      zkClient.setZkSerializer(new ZNRecordSerializer());
      if (zkClient.exists("/" + clusterName))
      {
        zkClient.deleteRecursive("/" + clusterName);
      }
    }
    finally
    {
      if (zkClient != null)
      {
        zkClient.close();
      }
    }

    ClusterSetup.processCommandLineArgs(getArgs("-zkSvr",
                                                ZK_ADDR,
                                                "-addCluster",
                                                clusterName));
    ClusterSetup.processCommandLineArgs(getArgs("-zkSvr",
                                                ZK_ADDR,
                                                "-addNode",
                                                clusterName,
                                                "localhost:54321"));
    ClusterSetup.processCommandLineArgs(getArgs("-zkSvr",
                                                ZK_ADDR,
                                                "-addNode",
                                                clusterName,
                                                "localhost:54322"));
  }

  private String[] getArgs(String... args)
  {
    return args;
  }

  private List<LiveInstance> deepCopy(List<LiveInstance> instances)
  {
    List<LiveInstance> result = new ArrayList<LiveInstance>();
    for (LiveInstance instance : instances)
    {
      result.add(new LiveInstance(instance.getRecord()));
    }
    return result;
  }
}
