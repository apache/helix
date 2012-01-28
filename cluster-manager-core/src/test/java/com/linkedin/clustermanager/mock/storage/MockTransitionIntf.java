package com.linkedin.clustermanager.mock.storage;

import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;

public interface MockTransitionIntf
{
  public void doTrasition(Message message, NotificationContext context);
}
