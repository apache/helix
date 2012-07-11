package com.linkedin.helix;

import org.I0Itec.zkclient.DataUpdater;

public class ZNRecordUpdater implements DataUpdater<ZNRecord>
{
  final ZNRecord _record;

  public ZNRecordUpdater(ZNRecord record)
  {
    _record = record;
  }

  @Override
  public ZNRecord update(ZNRecord current)
  {
    if (current != null)
    {
      current.merge(_record);
      return current;
    }
    return _record;
  }
}
