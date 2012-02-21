package com.linkedin.helix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import com.linkedin.helix.ConfigScopeBuilder;

public class ConfigScope
{
  public enum ConfigScopeProperty
  {
    CLUSTER, PARTICIPANT, RESOURCE, PARTITION;
  }

  private static Logger LOG = Logger.getLogger(ConfigScope.class);

  private final String _clusterName;
  private final ConfigScopeProperty _scope;
  private final String _scopeKey;
  private final List<String> _keys;

  ConfigScope(ConfigScopeBuilder configScopeBuilder)
  {
    Map<ConfigScopeProperty, String> scopeMap = configScopeBuilder
        .getScopeMap();
    _clusterName = scopeMap.get(ConfigScopeProperty.CLUSTER);
    _keys = new ArrayList<String>();
    ConfigScopeProperty temp = null;
    if (scopeMap.containsKey(ConfigScopeProperty.CLUSTER))
    {
      temp = ConfigScopeProperty.CLUSTER;
    }
    if (scopeMap.containsKey(ConfigScopeProperty.RESOURCE))
    {
      temp = ConfigScopeProperty.RESOURCE;
    }
    if (scopeMap.containsKey(ConfigScopeProperty.PARTITION))
    {
      temp = ConfigScopeProperty.RESOURCE;
    }
    if (scopeMap.containsKey(ConfigScopeProperty.PARTICIPANT))
    {
      temp = ConfigScopeProperty.PARTICIPANT;
    }

    if (temp != null && scopeMap.containsKey(temp))
    {
      _scope = temp;
      _scopeKey = scopeMap.get(temp);
    } else
    {
      throw new HelixException("Unrecognized scope for config");
    }
    switch (temp)
    {
    case CLUSTER:
      break;
    case RESOURCE:
      ;
      break;
    case PARTITION:
      _keys.add(scopeMap.get(ConfigScopeProperty.PARTITION));
      break;
    case PARTICIPANT:
      if (scopeMap.containsKey(ConfigScopeProperty.RESOURCE))
      {
        _keys.add(scopeMap.get(ConfigScopeProperty.RESOURCE));
      }
      break;
    default:
      throw new HelixException("Unrecognized scope for config");

    }

  }

  public ConfigScopeProperty getScope()
  {
    return _scope;
  }

  public String getClusterName()
  {
    return _clusterName;
  }

  public String getScopeKey()
  {
    return _scopeKey;
  }

  public List<String> getKeys()
  {
    return _keys;
  }
}
