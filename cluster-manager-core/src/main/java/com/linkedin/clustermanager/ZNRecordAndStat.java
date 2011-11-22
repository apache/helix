package com.linkedin.clustermanager;

import org.apache.zookeeper.data.Stat;

public class ZNRecordAndStat
{
  protected final ZNRecord _record;
  protected final Stat _stat;

  public ZNRecordAndStat(ZNRecord record)
  {
    this(record, null);
  }

  public ZNRecordAndStat(ZNRecord record, Stat stat)
  {
    _record = record;
    _stat = stat;
  }

  public ZNRecord getRecord()
  {
    return _record;
  }

  public Stat getStat()
  {
    return _stat;
  }
}
