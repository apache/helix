package com.linkedin.clustermanager;

import java.util.List;


public interface ConfigChangeListener
{

    public void onConfigChange(List<ZNRecord> configs,
            NotificationContext changeContext);

}
