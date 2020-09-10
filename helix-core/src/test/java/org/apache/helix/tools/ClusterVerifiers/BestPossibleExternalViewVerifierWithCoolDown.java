package org.apache.helix.tools.ClusterVerifiers;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;
import java.util.Set;

import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BestPossibleExternalViewVerifierWithCoolDown extends BestPossibleExternalViewVerifier{
  private static Logger LOG = LoggerFactory.getLogger(BestPossibleExternalViewVerifierWithCoolDown.class);
  private static final int  COOL_DOWN = Integer.parseInt(
      System.getProperty("test_cool_down", "2 * 1000"));

  private BestPossibleExternalViewVerifierWithCoolDown(RealmAwareZkClient zkClient, String clusterName,
      Map<String, Map<String, String>> errStates, Set<String> resources,
      Set<String> expectLiveInstances) {
    super (zkClient, clusterName, errStates, resources, expectLiveInstances);
  }

  public static class Builder extends BestPossibleExternalViewVerifier.Builder {
    public Builder(String clusterName) {
      super(clusterName);
    }
  }

  @Override
  public boolean verifyByPolling(long timeout, long period) {
    try {
      Thread.sleep(COOL_DOWN);
    } catch (InterruptedException e) {
      LOG.error("sleeping in verifyByPolling interrupted");
    }

    return super.verifyByPolling(timeout, period);
  }

}
