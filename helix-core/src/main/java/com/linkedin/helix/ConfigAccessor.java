package com.linkedin.helix;

import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZKUtil;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;

public class ConfigAccessor
{
  private static Logger LOG = Logger.getLogger(ConfigAccessor.class);

  private final String _zkAddr;

  public ConfigAccessor(String zkAddr)
  {
    _zkAddr = zkAddr;
  }

  public String get(ConfigScope scope, String key)
  {
    if (scope == null || scope.getScope() == null)
    {
      LOG.error("Scope can't be null");
      return null;
    }

    ZkClient zkClient = null;
    try
    {
      String value = null;
      zkClient = new ZkClient(_zkAddr);
      zkClient.setZkSerializer(new ZNRecordSerializer());
      ZKDataAccessor accessor = new ZKDataAccessor(scope.getClusterName(), zkClient);

      ZNRecord record = accessor.getProperty(PropertyType.CONFIGS, scope.getScope().toString(),
          scope.getScopeKey());
      
      List<String> keys = scope.getKeys();
      if (record != null)
      {
        if (keys.size() == 0)
        {
          value = record.getSimpleField(key);
        } else if (keys.size() == 1)
        {
          if (record.getMapField(keys.get(0)) != null)
          {
            value = record.getMapField(keys.get(0)).get(key);
          }
        }
      }
      return value;
    } finally
    {
      if (zkClient != null)
      {
        zkClient.close();
      }
    }

  }

  public void set(ConfigScope scope, String key, String value)
  {
    if (scope == null || scope.getScope() == null)
    {
      LOG.error("Scope can't be null");
      return;
    }

    ZkClient zkClient = null;
    try
    {
      zkClient = new ZkClient(_zkAddr);
      zkClient.setZkSerializer(new ZNRecordSerializer());
      String path = PropertyPathConfig.getPath(PropertyType.CONFIGS, scope.getClusterName(), scope
          .getScope().toString(), scope.getScopeKey());

      ZNRecord update = new ZNRecord(scope.getScopeKey());
      List<String> keys = scope.getKeys();
      if (keys.size() == 0)
      {
        update.setSimpleField(key, value);
      } else if (keys.size() == 1)
      {
        if (update.getMapField(keys.get(0)) == null)
        {
          update.setMapField(keys.get(0), new HashMap<String, String>());
        }
        update.getMapField(keys.get(0)).put(key, value);
      }
      ZKUtil.createOrUpdate(zkClient, path, update, true, true);
      return;
    } finally
    {
      if (zkClient != null)
      {
        zkClient.close();
      }
    }

  }
}
