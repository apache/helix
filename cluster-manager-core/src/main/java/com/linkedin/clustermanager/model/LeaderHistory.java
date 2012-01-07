package com.linkedin.clustermanager.model;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordDecorator;

public class LeaderHistory extends ZNRecordDecorator
{
  private final static int HISTORY_SIZE = 8;

  public LeaderHistory(String id)
  {
    super(id);
  }

  public LeaderHistory(ZNRecord record)
  {
    super(record);
  }

  /**
   * Save up to HISTORY_SIZE number of leaders in FIFO order
   * @param clusterName
   * @param instanceName
   */
  public void updateHistory(String clusterName, String instanceName)
  {
    List<String> list = _record.getListField(clusterName);
    if (list == null)
    {
      list = new ArrayList<String>();
      _record.setListField(clusterName, list);
    }

    if (list.size() == HISTORY_SIZE)
    {
      list.remove(0);
    }
    list.add(instanceName);
  }
}
