package com.linkedin.clustermanager.core.listeners;

import java.util.List;

import com.linkedin.clustermanager.core.NotificationContext;
import com.linkedin.clustermanager.model.ZNRecord;

public interface LiveInstanceChangeListener
{

    public void onLiveInstanceChange(List<ZNRecord> liveInstances,
            NotificationContext changeContext);

}
