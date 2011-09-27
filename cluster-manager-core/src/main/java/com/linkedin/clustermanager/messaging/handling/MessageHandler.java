package com.linkedin.clustermanager.messaging.handling;

import java.util.Map;

import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;

public interface MessageHandler
{
  public void handleMessage(Message message, NotificationContext context, Map<String, String> resultMap)  throws InterruptedException;
}
