package com.linkedin.clustermanager.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.annotate.JsonProperty;

import com.linkedin.clustermanager.core.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.core.ClusterDataAccessor.InstancePropertyType;

public class ClusterView
{
  private Map<ClusterPropertyType, List<ZNRecord>> clusterPropertyLists;

  // getter/setter's are needed for private fields for
  // serialization/de-serialization
  // ref:
  // http://jackson.codehaus.org/DataBindingDeepDive
  // @JsonProperty
  public void setClusterPropertyLists(Map<ClusterPropertyType, List<ZNRecord>> clusterPropertyLists)
  {
    this.clusterPropertyLists = clusterPropertyLists;
  }

  // @JsonProperty
  public Map<ClusterPropertyType, List<ZNRecord>> getClusterPropertyLists()
  {
    return clusterPropertyLists;
  }

  public void setClusterPropertyList(ClusterPropertyType type, List<ZNRecord> propertyList)
  {
    clusterPropertyLists.put(type, propertyList);
  }

  public List<ZNRecord> getClusterPropertyList(ClusterPropertyType type)
  {
    return clusterPropertyLists.get(type);
  }

  public void setMemberInstanceMap(Map<String, MemberInstance> memberInstanceMap)
  {
    this._memberInstanceMap = memberInstanceMap;
  }

  @JsonProperty
  public Map<String, MemberInstance> getMemberInstanceMap()
  {
    return _memberInstanceMap;
  }

  public void set_memberInstanceMap(Map<String, MemberInstance> _memberInstanceMap)
  {
    this._memberInstanceMap = _memberInstanceMap;
  }

  private Map<String, MemberInstance> _memberInstanceMap;
  private List<MemberInstance> _instances;

  public void setInstances(List<MemberInstance> instances)
  {
    this._instances = instances;
  }

  public List<MemberInstance> getInstances()
  {
    return _instances;
  }

  public static class MemberInstance
  {
    private Map<InstancePropertyType, List<ZNRecord>> _instanceProperties = new TreeMap<InstancePropertyType, List<ZNRecord>>();

    public void setClusterProperties(Map<InstancePropertyType, List<ZNRecord>> instanceProperties)
    {
      this._instanceProperties = instanceProperties;
    }

    // @JsonProperty
    public Map<InstancePropertyType, List<ZNRecord>> getInstanceProperties()
    {
      return _instanceProperties;
    }

    public void setInstanceProperty(InstancePropertyType type, List<ZNRecord> values)
    {
      _instanceProperties.put(type, values);
    }

    public List<ZNRecord> getInstanceProperty(InstancePropertyType type)
    {
      return _instanceProperties.get(type);
    }

    private String _instanceName;

    // for JSON de-serialization
    public MemberInstance()
    {

    }

    public MemberInstance(String instanceName)
    {
      this._instanceName = instanceName;
    }

    public String getInstanceName()
    {
      return _instanceName;
    }

    public void setInstanceName(String instanceName)
    {
      this._instanceName = instanceName;
    }

  }

  public MemberInstance getMemberInstance(String instanceName, boolean createNewIfAbsent)
  {
    if (!_memberInstanceMap.containsKey(instanceName))
    {
      _memberInstanceMap.put(instanceName, new MemberInstance(instanceName));
    }
    return _memberInstanceMap.get(instanceName);
  }

  private List<ZNRecord> _externalView;

  public ClusterView()
  {
    clusterPropertyLists = new TreeMap<ClusterPropertyType, List<ZNRecord>>();
    setClusterPropertyList(ClusterPropertyType.IDEALSTATES, new ArrayList<ZNRecord>());
    setClusterPropertyList(ClusterPropertyType.CONFIGS, new ArrayList<ZNRecord>());
    setClusterPropertyList(ClusterPropertyType.LIVEINSTANCES, new ArrayList<ZNRecord>());
    setClusterPropertyList(ClusterPropertyType.INSTANCES, new ArrayList<ZNRecord>());

    _memberInstanceMap = new HashMap<String, ClusterView.MemberInstance>();
  }

  public List<ZNRecord> getExternalView()
  {
    return _externalView;
  }

  public void setExternalView(List<ZNRecord> externalView)
  {
    this._externalView = externalView;
  }

}
