package com.linkedin.helix;

import java.util.List;

import com.linkedin.helix.model.InstanceConfig;
/**
 * @author kgopalak
 *
 */
public interface ConfigChangeListener
{

  public void onConfigChange(List<InstanceConfig> configs,
      NotificationContext changeContext);

}
