package com.linkedin.helix;

import java.util.List;

import com.linkedin.helix.model.Message;

public interface MessageListener
{

  public void onMessage(String instanceName, List<Message> messages,
      NotificationContext changeContext);

}
