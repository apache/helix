package org.apache.helix.model;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

public class AlertHistory extends HelixProperty
{

  public AlertHistory(ZNRecord record)
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
