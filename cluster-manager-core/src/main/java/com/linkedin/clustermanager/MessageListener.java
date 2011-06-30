package com.linkedin.clustermanager;

import java.util.List;


public interface MessageListener
{

    public void onMessage(String instanceName, List<ZNRecord> messages,
            NotificationContext changeContext);

}
