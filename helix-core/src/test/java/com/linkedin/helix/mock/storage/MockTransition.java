package com.linkedin.helix.mock.storage;

import org.apache.log4j.Logger;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

public class MockTransition
{
  private static Logger LOG = Logger.getLogger(MockTransition.class);

  // called by state model transition functions
  public void doTransition(Message message, NotificationContext context)
  {
    LOG.info("default doTransition() invoked");
  }

  // called by state model reset function
  public void doReset()
  {
    LOG.info("default doReset() invoked");
  }

}
