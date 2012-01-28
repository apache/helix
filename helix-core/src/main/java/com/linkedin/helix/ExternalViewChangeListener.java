package com.linkedin.helix;

import java.util.List;

import com.linkedin.helix.model.ExternalView;

public interface ExternalViewChangeListener
{

  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext);

}
