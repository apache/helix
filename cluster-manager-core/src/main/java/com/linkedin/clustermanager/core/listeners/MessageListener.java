package com.linkedin.clustermanager.core.listeners;

import java.util.List;

import com.linkedin.clustermanager.core.NotificationContext;
import com.linkedin.clustermanager.model.ZNRecord;

public interface MessageListener
{

    public void onMessage(String instanceName, List<ZNRecord> messages,
            NotificationContext changeContext);

}
