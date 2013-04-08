package org.apache.helix.model.builder;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.util.StringTemplate;
import org.apache.log4j.Logger;


/**
 * @deprecated replaced by {@link HelixConfigScopeBuilder}
 */
@Deprecated
public class ConfigScopeBuilder
{
  private static Logger LOG = Logger.getLogger(ConfigScopeBuilder.class);

  private static StringTemplate template = new StringTemplate();
  static
  {
    // @formatter:off
    template.addEntry(ConfigScopeProperty.CLUSTER, 1, "CLUSTER={clusterName}");
    template.addEntry(ConfigScopeProperty.RESOURCE, 2, "CLUSTER={clusterName},RESOURCE={resourceName}");
    template.addEntry(ConfigScopeProperty.PARTITION, 3, "CLUSTER={clusterName},RESOURCE={resourceName},PARTITION={partitionName}");
    template.addEntry(ConfigScopeProperty.PARTICIPANT, 2, "CLUSTER={clusterName},PARTICIPANT={participantName}");
    // @formatter:on
  }

  private final Map<ConfigScopeProperty, String> _scopeMap;

  public Map<ConfigScopeProperty, String> getScopeMap()
  {
    return _scopeMap;
  }

  public ConfigScopeBuilder()
  {
    _scopeMap = new HashMap<ConfigScopeProperty, String>();
  }

  public ConfigScopeBuilder forCluster(String clusterName)
  {
    _scopeMap.put(ConfigScopeProperty.CLUSTER, clusterName);
    return this;
  }

  public ConfigScopeBuilder forParticipant(String participantName)
  {
    _scopeMap.put(ConfigScopeProperty.PARTICIPANT, participantName);
    return this;
  }

  public ConfigScopeBuilder forResource(String resourceName)
  {
    _scopeMap.put(ConfigScopeProperty.RESOURCE, resourceName);
    return this;

  }

  public ConfigScopeBuilder forPartition(String partitionName)
  {
    _scopeMap.put(ConfigScopeProperty.PARTITION, partitionName);
    return this;

  }

  public ConfigScope build()
  {
    // TODO: validate the scopes map
    return new ConfigScope(this);
  }

  public ConfigScope build(ConfigScopeProperty scope, String clusterName, String... scopeKeys)
  {
    if (scopeKeys == null)
    {
      scopeKeys = new String[]{};
    }

    String[] args = new String[1 + scopeKeys.length];
    args[0] = clusterName;
    System.arraycopy(scopeKeys, 0, args, 1, scopeKeys.length);
    String scopePairs = template.instantiate(scope, args);

    return build(scopePairs);
  }

  public ConfigScope build(String scopePairs)
  {
    String[] scopes = scopePairs.split("[\\s,]+");
    for (String scope : scopes)
    {
      try
      {
        int idx = scope.indexOf('=');
        if (idx == -1)
        {
          LOG.error("Invalid scope string: " + scope);
          continue;
        }

        String scopeStr = scope.substring(0, idx);
        String value = scope.substring(idx + 1);
        ConfigScopeProperty scopeProperty = ConfigScopeProperty.valueOf(scopeStr);
        _scopeMap.put(scopeProperty, value);
      } catch (Exception e)
      {
        LOG.error("Invalid scope string: " + scope);
        continue;
      }
    }

    return build();
  }

  @Override
  public String toString()
  {
    return _scopeMap.toString();
  }
}
