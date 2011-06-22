package com.linkedin.clustermanager.core.listeners;

import java.util.List;

import com.linkedin.clustermanager.core.NotificationContext;
import com.linkedin.clustermanager.model.ZNRecord;

public interface IdealStateChangeListener
{

    void onIdealStateChange(List<ZNRecord> idealState,
            NotificationContext changeContext);

}
