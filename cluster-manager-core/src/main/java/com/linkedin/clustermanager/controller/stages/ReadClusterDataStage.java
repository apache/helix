package com.linkedin.clustermanager.controller.stages;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;
import com.linkedin.clustermanager.util.ZNRecordUtil;

public class ReadClusterDataStage extends AbstractBaseStage
{
  private static final Logger logger = Logger
      .getLogger(ReadClusterDataStage.class.getName());
  ClusterDataCache _cache;

  public ReadClusterDataStage()
  {
    _cache = new ClusterDataCache();
  }

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    if (manager == null)
    {
      throw new StageException("ClusterManager attribute value is null");
    }
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();
    _cache.setIdealStateMap(retrieve(dataAccessor,
        ClusterPropertyType.IDEALSTATES, IdealState.class));
    Map<String, LiveInstance> liveInstanceMap = retrieve(dataAccessor,
        ClusterPropertyType.LIVEINSTANCES, LiveInstance.class);
    _cache.setLiveInstanceMap(liveInstanceMap);
    _cache.setStateModelDefMap(retrieve(dataAccessor,
        ClusterPropertyType.STATEMODELDEFS, StateModelDefinition.class));
    _cache.setInstanceConfigMap(retrieve(dataAccessor,
        ClusterPropertyType.CONFIGS, InstanceConfig.class));
    for (String instanceName : liveInstanceMap.keySet())
    {
      retrieve(instanceName, dataAccessor, InstancePropertyType.CURRENTSTATES,
          CurrentState.class);

    }

    event.addAttribute("clusterDataCache", _cache);
  }

  private <T extends Object> Map<String, T> retrieve(String instanceName,
      ClusterDataAccessor dataAccessor, InstancePropertyType type,
      Class<T> clazz)
  {
    List<ZNRecord> instancePropertyList = dataAccessor.getInstancePropertyList(
        instanceName, type);
    Map<String, T> map = ZNRecordUtil.convertListToTypedMap(
        instancePropertyList, clazz);
    return map;
  }

  private <T extends Object> Map<String, T> retrieve(
      ClusterDataAccessor dataAccessor, ClusterPropertyType type, Class<T> clazz)
  {

    List<ZNRecord> clusterPropertyList;
    clusterPropertyList = dataAccessor.getClusterPropertyList(type);
    Map<String, T> map = ZNRecordUtil.convertListToTypedMap(
        clusterPropertyList, clazz);
    return map;
  }

}
