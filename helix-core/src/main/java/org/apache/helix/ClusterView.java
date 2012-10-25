package org.apache.helix;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.annotate.JsonProperty;

@Deprecated
public class ClusterView
{
  private Map<PropertyType, List<ZNRecord>> clusterPropertyLists;

  // getter/setter's are needed for private fields for
  // serialization/de-serialization
  // ref:
  // http://jackson.codehaus.org/DataBindingDeepDive
  // @JsonProperty
  public void setClusterPropertyLists(Map<PropertyType, List<ZNRecord>> clusterPropertyLists)
  {
    this.clusterPropertyLists = clusterPropertyLists;
  }

  // @JsonProperty
  public Map<PropertyType, List<ZNRecord>> getPropertyLists()
  {
    return clusterPropertyLists;
  }

  public void setClusterPropertyList(PropertyType type, List<ZNRecord> propertyList)
  {
    clusterPropertyLists.put(type, propertyList);
  }

  public List<ZNRecord> getPropertyList(PropertyType type)
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
    private Map<PropertyType, List<ZNRecord>> _instanceProperties = new TreeMap<PropertyType, List<ZNRecord>>();

    public void setClusterProperties(Map<PropertyType, List<ZNRecord>> instanceProperties)
    {
      this._instanceProperties = instanceProperties;
    }

    // @JsonProperty
    public Map<PropertyType, List<ZNRecord>> getInstanceProperties()
    {
      return _instanceProperties;
    }

    public void setInstanceProperty(PropertyType type, List<ZNRecord> values)
    {
      _instanceProperties.put(type, values);
    }

    public List<ZNRecord> getInstanceProperty(PropertyType type)
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
    clusterPropertyLists = new TreeMap<PropertyType, List<ZNRecord>>();
    setClusterPropertyList(PropertyType.IDEALSTATES, new ArrayList<ZNRecord>());
    setClusterPropertyList(PropertyType.CONFIGS, new ArrayList<ZNRecord>());
    setClusterPropertyList(PropertyType.LIVEINSTANCES, new ArrayList<ZNRecord>());
    setClusterPropertyList(PropertyType.INSTANCES, new ArrayList<ZNRecord>());

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
