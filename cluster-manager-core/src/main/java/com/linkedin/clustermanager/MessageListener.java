package com.linkedin.clustermanager;

import java.util.List;

import com.linkedin.clustermanager.model.Message;

public interface MessageListener
{

  public void onMessage(String instanceName, List<Message> messages,
      NotificationContext changeContext);

}
