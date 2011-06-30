package com.linkedin.clustermanager;

import java.util.List;


public interface ExternalViewChangeListener
{

    public void onExternalViewChange(List<ZNRecord> externalViewList,
            NotificationContext changeContext);

}
