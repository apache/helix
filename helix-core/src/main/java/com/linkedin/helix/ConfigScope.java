package com.linkedin.helix;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class ConfigScope
{
  public enum ConfigScopeProperty
  {
    CLUSTER, PARTICIPANT, RESOURCE, PARTITION, CONSTRAINT;
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
    _keys = new ArrayList<String>();
    ConfigScopeProperty tempScope = null;
    if (scopeMap.containsKey(ConfigScopeProperty.CLUSTER))
    {
      _clusterName = scopeMap.get(ConfigScopeProperty.CLUSTER);
      tempScope = ConfigScopeProperty.CLUSTER;
    } else
    {
      throw new HelixException("Invalid scope for config: cluster name is null");
    }
    if (scopeMap.containsKey(ConfigScopeProperty.RESOURCE))
    {
      tempScope = ConfigScopeProperty.RESOURCE;
    }
    if (scopeMap.containsKey(ConfigScopeProperty.PARTITION))
    {
      tempScope = ConfigScopeProperty.RESOURCE;
      _keys.add(scopeMap.get(ConfigScopeProperty.PARTITION));
    }
    if (scopeMap.containsKey(ConfigScopeProperty.PARTICIPANT))
    {
      tempScope = ConfigScopeProperty.PARTICIPANT;
      if (scopeMap.containsKey(ConfigScopeProperty.RESOURCE))
      {
        _keys.add(scopeMap.get(ConfigScopeProperty.RESOURCE));
      }
    }

    if (tempScope != null && scopeMap.containsKey(tempScope))
    {
      _scope = tempScope;
      _scopeKey = scopeMap.get(tempScope);
    } else
    {
      throw new HelixException("Invalid scope for config. scope:");
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
  
  @Override
  public String toString()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("cluster:" + _clusterName + ", scope:" + _scope + ", scopeKey:" + _scopeKey
        + ", keys:" + _keys);
    return sb.toString();
  }
}
