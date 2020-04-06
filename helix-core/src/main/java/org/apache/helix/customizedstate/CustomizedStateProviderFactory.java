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

import java.util.HashMap;
import java.util.Map;

import javafx.util.Pair;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Singleton factory that build customized state provider.
 */
public class CustomizedStateProviderFactory {
  private static Logger LOG = LoggerFactory.getLogger(CustomizedStateProvider.class);
  private final Map<Pair<String, HelixManager>, CustomizedStateProvider>
      _customizedStateProviderMap = new HashMap<>();

  protected CustomizedStateProviderFactory() {
  }

  private static class SingletonHelper {
    private static final CustomizedStateProviderFactory INSTANCE =
        new CustomizedStateProviderFactory();
  }

  public static CustomizedStateProviderFactory getInstance() {
    return SingletonHelper.INSTANCE;
  }

  public CustomizedStateProvider buildCustomizedStateProvider(String instanceName,
      HelixManager helixManager) {
    return buildCustomizedStateProvider(helixManager, instanceName);
  }

  /**
   * Build a customized state provider based on the specified input. If the pair of instance
   * name and helix manager already has a provider, return it. Otherwise, build a new one and put
   * it in the map.
   * @param helixManager A helix manager that belongs to the instance
   * @param instanceName The name of the instance
   * @return CustomizedStateProvider
   */
  public CustomizedStateProvider buildCustomizedStateProvider(HelixManager helixManager,
      String instanceName) {
    Pair<String, HelixManager> pairKey = new Pair(instanceName, helixManager);
    synchronized (_customizedStateProviderMap) {
      if (_customizedStateProviderMap.get(pairKey) != null) {
        return _customizedStateProviderMap.get(pairKey);
      }
      CustomizedStateProvider customizedStateProvider =
          new CustomizedStateProvider(helixManager, instanceName);
      _customizedStateProviderMap.put(pairKey, customizedStateProvider);
      return customizedStateProvider;
    }
  }
}
