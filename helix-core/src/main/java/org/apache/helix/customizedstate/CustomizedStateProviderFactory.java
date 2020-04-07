package org.apache.helix.customizedstate;

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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton factory that build customized state provider.
 */
public class CustomizedStateProviderFactory {
  private static Logger LOG = LoggerFactory.getLogger(CustomizedStateProvider.class);

  protected CustomizedStateProviderFactory() {
  }

  private static class SingletonHelper {
    private static final CustomizedStateProviderFactory INSTANCE =
        new CustomizedStateProviderFactory();
  }

  public static CustomizedStateProviderFactory getInstance() {
    return SingletonHelper.INSTANCE;
  }

  /**
   * Build a customized state provider based on user input.
   * @param instanceName The name of the instance
   * @param clusterName The name of the cluster that has the instance
   * @param zkAddress The zookeeper address of the cluster
   * @return CustomizedStateProvider
   */
  public CustomizedStateProvider buildCustomizedStateProvider(String instanceName,
      String clusterName, String zkAddress) {
    HelixManager helixManager = HelixManagerFactory
        .getZKHelixManager(clusterName, instanceName, InstanceType.ADMINISTRATOR, zkAddress);
    return new CustomizedStateProvider(helixManager, instanceName);
  }

  /**
   * Build a customized state provider based on user input.
   * @param instanceName The name of the instance
   * @param helixManager Helix manager provided by user
   * @return CustomizedStateProvider
   */
  public CustomizedStateProvider buildCustomizedStateProvider(HelixManager helixManager,
      String instanceName) {
    return new CustomizedStateProvider(helixManager, instanceName);
  }
}
