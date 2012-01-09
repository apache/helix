package com.linkedin.clustermanager;

import java.util.List;

import com.linkedin.clustermanager.model.ExternalView;

public interface ExternalViewChangeListener
{

  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext);

}
