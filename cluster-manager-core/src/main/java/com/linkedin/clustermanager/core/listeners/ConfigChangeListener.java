package com.linkedin.clustermanager.core.listeners;

import java.util.List;

import com.linkedin.clustermanager.core.NotificationContext;
import com.linkedin.clustermanager.model.ZNRecord;

public interface ConfigChangeListener
{

    public void onConfigChange(List<ZNRecord> configs,
            NotificationContext changeContext);

}
