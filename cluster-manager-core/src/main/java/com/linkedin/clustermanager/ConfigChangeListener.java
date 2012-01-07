package com.linkedin.clustermanager;

import java.util.List;

import com.linkedin.clustermanager.model.InstanceConfig;
/**
 * @author kgopalak
 *
 */
public interface ConfigChangeListener
{

  public void onConfigChange(List<InstanceConfig> configs,
      NotificationContext changeContext);

}
