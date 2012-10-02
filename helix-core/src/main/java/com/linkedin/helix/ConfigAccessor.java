/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.manager.zk.ZKUtil;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.util.StringTemplate;

public class ConfigAccessor
{
  private static Logger               LOG      = Logger.getLogger(ConfigAccessor.class);

  private static final StringTemplate template = new StringTemplate();
  static
  {
    // @formatter:off
    template.addEntry(ConfigScopeProperty.CLUSTER, 1, "/{clusterName}/CONFIGS/CLUSTER");
    template.addEntry(ConfigScopeProperty.CLUSTER,
                      2,
                      "/{clusterName}/CONFIGS/CLUSTER/{clusterName}|SIMPLEKEYS");
    template.addEntry(ConfigScopeProperty.PARTICIPANT,
                      1,
                      "/{clusterName}/CONFIGS/PARTICIPANT");
    template.addEntry(ConfigScopeProperty.PARTICIPANT,
                      2,
                      "/{clusterName}/CONFIGS/PARTICIPANT/{participantName}|SIMPLEKEYS");
    template.addEntry(ConfigScopeProperty.RESOURCE, 1, "/{clusterName}/CONFIGS/RESOURCE");
    template.addEntry(ConfigScopeProperty.RESOURCE,
                      2,
                      "/{clusterName}/CONFIGS/RESOURCE/{resourceName}|SIMPLEKEYS");
    template.addEntry(ConfigScopeProperty.PARTITION,
                      2,
                      "/{clusterName}/CONFIGS/RESOURCE/{resourceName}|MAPKEYS");
    template.addEntry(ConfigScopeProperty.PARTITION,
                      3,
                      "/{clusterName}/CONFIGS/RESOURCE/{resourceName}|MAPMAPKEYS|{partitionName}");
    // @formatter:on
  }

  private final ZkClient              zkClient;

  public ConfigAccessor(ZkClient zkClient)
  {
    this.zkClient = zkClient;
  }

  /**
   * Get config value
   * 
   * @param scope
   * @param key
   * @return value or null if doesn't exist
   */
  public String get(ConfigScope scope, String key)
  {
    if (scope == null || scope.getScope() == null)
    {
      LOG.error("Scope can't be null");
      return null;
    }

    String value = null;
    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, zkClient))
    {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    String scopeStr = scope.getScopeStr();
    String[] splits = scopeStr.split("\\|");

    ZNRecord record = zkClient.readData(splits[0], true);

    if (record != null)
    {
      if (splits.length == 1)
      {
        value = record.getSimpleField(key);
      }
      else if (splits.length == 2)
      {
        if (record.getMapField(splits[1]) != null)
        {
          value = record.getMapField(splits[1]).get(key);
        }
      }
    }
    return value;

  }

  /**
   * Set a config value
   * 
   * @param scope
   * @param key
   * @param value
   */
  public void set(ConfigScope scope, String key, String value)
  {
    if (scope == null || scope.getScope() == null)
    {
      LOG.error("Scope can't be null");
      return;
    }

    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, zkClient))
    {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    String scopeStr = scope.getScopeStr();
    String[] splits = scopeStr.split("\\|");

    String id = splits[0].substring(splits[0].lastIndexOf('/') + 1);
    ZNRecord update = new ZNRecord(id);
    if (splits.length == 1)
    {
      update.setSimpleField(key, value);
    }
    else if (splits.length == 2)
    {
      if (update.getMapField(splits[1]) == null)
      {
        update.setMapField(splits[1], new TreeMap<String, String>());
      }
      update.getMapField(splits[1]).put(key, value);
    }
    ZKUtil.createOrUpdate(zkClient, splits[0], update, true, true);
    return;
  }

  /**
   * Remove config value
   * 
   * @param scope
   * @param key
   */
  public void remove(ConfigScope scope, String key)
  {
    if (scope == null || scope.getScope() == null)
    {
      LOG.error("Scope can't be null");
      return;
    }

    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, zkClient))
    {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    String scopeStr = scope.getScopeStr();
    String[] splits = scopeStr.split("\\|");

    String id = splits[0].substring(splits[0].lastIndexOf('/') + 1);
    ZNRecord update = new ZNRecord(id);
    if (splits.length == 1)
    {
      // subtract doesn't care about value, use empty string
      update.setSimpleField(key, "");
    }
    else if (splits.length == 2)
    {
      if (update.getMapField(splits[1]) == null)
      {
        update.setMapField(splits[1], new TreeMap<String, String>());
      }
      // subtract doesn't care about value, use empty string
      update.getMapField(splits[1]).put(key, "");
    }

    ZKUtil.subtract(zkClient, splits[0], update);
    return;
  }

  /**
   * Get a list of config keys
   * 
   * @param type
   * @param clusterName
   * @param keys
   * @return
   */
  public List<String> getKeys(ConfigScopeProperty type,
                              String clusterName,
                              String... keys)
  {
    if (type == null || clusterName == null)
    {
      LOG.error("clusterName|scope can't be null");
      return Collections.emptyList();
    }

    try
    {
      if (!ZKUtil.isClusterSetup(clusterName, zkClient))
      {
        LOG.error("cluster " + clusterName + " is not setup yet");
        return Collections.emptyList();
      }

      String[] args = new String[1 + keys.length];
      args[0] = clusterName;
      System.arraycopy(keys, 0, args, 1, keys.length);
      String scopeStr = template.instantiate(type, args);
      String[] splits = scopeStr.split("\\|");
      List<String> retKeys = null;
      if (splits.length == 1)
      {
        retKeys = zkClient.getChildren(splits[0]);
      }
      else
      {
        ZNRecord record = zkClient.readData(splits[0]);

        if (splits[1].startsWith("SIMPLEKEYS"))
        {
          retKeys = new ArrayList<String>(record.getSimpleFields().keySet());

        }
        else if (splits[1].startsWith("MAPKEYS"))
        {
          retKeys = new ArrayList<String>(record.getMapFields().keySet());
        }
        else if (splits[1].startsWith("MAPMAPKEYS"))
        {
          retKeys = new ArrayList<String>(record.getMapField(splits[2]).keySet());
        }
      }
      if (retKeys == null)
      {
        LOG.error("Invalid scope: " + type + " or keys: " + Arrays.toString(args));
        return Collections.emptyList();
      }

      Collections.sort(retKeys);
      return retKeys;
    }
    catch (Exception e)
    {
      return Collections.emptyList();
    }

  }
}
