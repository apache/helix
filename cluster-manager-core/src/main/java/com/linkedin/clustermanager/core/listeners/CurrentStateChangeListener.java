package com.linkedin.clustermanager.core.listeners;

import java.util.List;

import com.linkedin.clustermanager.core.NotificationContext;
import com.linkedin.clustermanager.model.ZNRecord;

public interface CurrentStateChangeListener
{

    public void onStateChange(String instanceName, List<ZNRecord> statesInfo,
            NotificationContext changeContext);

}
