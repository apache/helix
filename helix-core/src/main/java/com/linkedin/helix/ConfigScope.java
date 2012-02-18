package com.linkedin.helix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class ConfigScope
{
  public enum ConfigScopeProperty {
    CLUSTER, PARTICIPANT, RESOURCE, PARTITION;
  }

  private static Logger LOG = Logger.getLogger(ConfigScope.class);
  private static Map<ConfigScopeProperty, Map<ConfigScopeProperty, ConfigScopeProperty>> _transitionMap = new HashMap<ConfigScopeProperty, Map<ConfigScopeProperty, ConfigScopeProperty>>();

  private String _clusterName;
  private ConfigScopeProperty _scope = null;
  private String _scopeKey;
  private List<String> _keys;

  static
  {
    // @formatter:off
    addEntry(ConfigScopeProperty.CLUSTER, ConfigScopeProperty.PARTICIPANT,
        ConfigScopeProperty.PARTICIPANT);
    addEntry(ConfigScopeProperty.CLUSTER, ConfigScopeProperty.RESOURCE,
        ConfigScopeProperty.RESOURCE);
    addEntry(ConfigScopeProperty.PARTICIPANT, ConfigScopeProperty.RESOURCE,
        ConfigScopeProperty.PARTICIPANT);
    addEntry(ConfigScopeProperty.RESOURCE, ConfigScopeProperty.PARTITION,
        ConfigScopeProperty.RESOURCE);
    // @formatter:on
  }

  private static void addEntry(ConfigScopeProperty curScope, ConfigScopeProperty inputScope,
      ConfigScopeProperty nextScope)
  {
    if (!_transitionMap.containsKey(curScope))
    {
      _transitionMap.put(curScope, new HashMap<ConfigScopeProperty, ConfigScopeProperty>());
    }
    LOG.info("Add transition for scope:" + curScope + " input:" + inputScope + " next:" + nextScope);
    _transitionMap.get(curScope).put(inputScope, nextScope);
  }

  private static ConfigScopeProperty getNextScope(ConfigScopeProperty curScope,
      ConfigScopeProperty inputScope)
  {
    if (_transitionMap.containsKey(curScope))
    {
      return _transitionMap.get(curScope).get(inputScope);
    }
    return null;
  }

  public ConfigScope forCluster(String clusterName)
  {
    _clusterName = clusterName;
    _scope = ConfigScopeProperty.CLUSTER;
    _scopeKey = clusterName;
    _keys = new ArrayList<String>();
    return this;
  }

  private ConfigScope transitScope(ConfigScopeProperty inputScope, String inputKey)
  {
    ConfigScopeProperty nextScope = getNextScope(_scope, inputScope);
    if (nextScope != null)
    {
      if (nextScope != _scope)
      {
        _scope = nextScope;
        _scopeKey = inputKey;
      } else
      {
        _keys.add(inputKey);
      }
    }
 else
    {
      LOG.error("Invalid scope transition. curScope" + _scope + ", inputScope:" + inputScope
          + ", inputKey:" + inputKey);
    }
    return this;
  }

  public ConfigScope forParticipant(String participantName)
  {
    return transitScope(ConfigScopeProperty.PARTICIPANT, participantName);
  }

  public ConfigScope forResource(String resourceName)
  {
    return transitScope(ConfigScopeProperty.RESOURCE, resourceName);
  }

  public ConfigScope forPartition(String partitionName)
  {
    return transitScope(ConfigScopeProperty.PARTITION, partitionName);
  }

  public ConfigScope build()
  {
    if (_scope == null)
    {
      throw new IllegalArgumentException("Invalid scope. curScope:" + _scope + ", clusterName:"
          + _clusterName + ", scopeKey:" + _scopeKey + ", keys:" + _keys);
    }

    return this;
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
