package com.linkedin.helix.model;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;

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

  @Override
  public boolean isValid()
  { 
    return true;
  }
}
