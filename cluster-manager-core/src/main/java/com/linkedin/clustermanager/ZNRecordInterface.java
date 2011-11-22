package com.linkedin.clustermanager;

import org.apache.zookeeper.data.Stat;

public interface ZNRecordInterface
{
  public ZNRecord getRecord();
  public Stat getStat();
}
