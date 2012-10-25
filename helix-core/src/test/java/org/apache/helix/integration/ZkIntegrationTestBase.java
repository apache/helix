package org.apache.helix.integration;

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
import java.util.logging.Level;

import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper.StartCMResult;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.ZKClientPool;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;


public class ZkIntegrationTestBase
{
  private static Logger         LOG                       =
                                                              Logger.getLogger(ZkIntegrationTestBase.class);

  protected static ZkServer     _zkServer;
  protected static ZkClient     _gZkClient;
  protected static ClusterSetup _gSetupTool;

  public static final String    ZK_ADDR                   = "localhost:2183";
  protected static final String CLUSTER_PREFIX            = "CLUSTER";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";

  protected final String        CONTROLLER_PREFIX         = "controller";
  protected final String        PARTICIPANT_PREFIX        = "localhost";

  @BeforeSuite
  public void beforeSuite() throws Exception
  {
    // TODO: use logging.properties file to config java.util.logging.Logger levels
    java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
    topJavaLogger.setLevel(Level.WARNING);
    
    _zkServer = TestHelper.startZkSever(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);
    ZKClientPool.reset();

    _gZkClient = new ZkClient(ZK_ADDR);
    _gZkClient.setZkSerializer(new ZNRecordSerializer());
    _gSetupTool = new ClusterSetup(ZK_ADDR);
  }

  @AfterSuite
  public void afterSuite()
  {
    ZKClientPool.reset();
    TestHelper.stopZkServer(_zkServer);
    _gZkClient.close();
  }

  protected String getShortClassName()
  {
    String className = this.getClass().getName();
    return className.substring(className.lastIndexOf('.') + 1);
  }

  protected String getCurrentLeader(ZkClient zkClient, String clusterName)
  {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader == null)
    {
      return null;
    }
    return leader.getInstanceName();
  }

  /**
   * Stop current leader and returns the new leader
   * 
   * @param zkClient
   * @param clusterName
   * @param startCMResultMap
   * @return
   */
  protected String stopCurrentLeader(ZkClient zkClient,
                                     String clusterName,
                                     Map<String, StartCMResult> startCMResultMap)
  {
    String leader = getCurrentLeader(zkClient, clusterName);
    Assert.assertTrue(leader != null);
    System.out.println("stop leader: " + leader + " in " + clusterName);
    Assert.assertTrue(leader != null);

    StartCMResult result = startCMResultMap.remove(leader);
    Assert.assertTrue(result._manager != null);
    result._manager.disconnect();

    Assert.assertTrue(result._thread != null);
    result._thread.interrupt();

    boolean isNewLeaderElected = false;
    String newLeader = null;
    try
    {
      for (int i = 0; i < 5; i++)
      {
        Thread.sleep(1000);
        newLeader = getCurrentLeader(zkClient, clusterName);
        if (!newLeader.equals(leader))
        {
          isNewLeaderElected = true;
          System.out.println("new leader elected: " + newLeader + " in " + clusterName);
          break;
        }
      }
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    if (isNewLeaderElected == false)
    {
      System.out.println("fail to elect a new leader in " + clusterName);
    }
    AssertJUnit.assertTrue(isNewLeaderElected);
    return newLeader;
  }

  protected void enableHealthCheck(String clusterName)
  {
    ConfigScope scope = new ConfigScopeBuilder().forCluster(clusterName).build();
    new ConfigAccessor(_gZkClient).set(scope, "healthChange" + ".enabled", "" + true);
  }

}
