package com.linkedin.helix.mock.storage;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

public interface MockTransitionIntf
{
  public void doTrasition(Message message, NotificationContext context);
}
