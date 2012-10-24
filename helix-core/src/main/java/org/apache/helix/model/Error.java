package org.apache.helix.model;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

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
