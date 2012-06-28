package com.linkedin.helix.model;

import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.ZNRecord;

public class Error extends HelixProperty
{
  public Error(ZNRecord record)
  {
    super(record);
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean isValid()
  {
    // TODO Auto-generated method stub
    return true;
  }
}
