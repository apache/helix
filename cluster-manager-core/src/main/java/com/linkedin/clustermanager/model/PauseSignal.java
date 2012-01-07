package com.linkedin.clustermanager.model;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordDecorator;

public class PauseSignal extends ZNRecordDecorator
{
  public PauseSignal(String id)
  {
    super(id);
  }

  public PauseSignal(ZNRecord record)
  {
    super(record);
  }
}
