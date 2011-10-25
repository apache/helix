package com.linkedin.clustermanager.controller.stages;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

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
    _cache.refresh(dataAccessor);
 
    event.addAttribute("ClusterDataCache", _cache);
  }

  /*
  private <T extends Object> Map<String, T> retrieve(String instanceName,
      ClusterDataAccessor dataAccessor, PropertyType type,
      Class<T> clazz)
  {
    List<ZNRecord> instancePropertyList = dataAccessor.getChildValues(type,
        instanceName);
    Map<String, T> map = ZNRecordUtil.convertListToTypedMap(
        instancePropertyList, clazz);
    return map;
  }
  */
  

}
