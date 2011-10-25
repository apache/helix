package com.linkedin.clustermanager;

import com.linkedin.clustermanager.ZNRecordDelta.MERGEOPERATION;

public class ZNRecordDelta
{
  public enum MERGEOPERATION {ADD, SUBSTRACT};
  public ZNRecord _record;
  public MERGEOPERATION _mergeOperation;

  public ZNRecordDelta(ZNRecord record, MERGEOPERATION _mergeOperation)
  {
    _record = new ZNRecord(record);
    this._mergeOperation = _mergeOperation;
  }
  
  public ZNRecordDelta(ZNRecord record)
  {
    this(record, MERGEOPERATION.ADD);
  }
  
  public ZNRecordDelta()
  {
    this(new ZNRecord(""), MERGEOPERATION.ADD);
  }
  
  public ZNRecord getRecord()
  {
    return _record;
  }

  public MERGEOPERATION getMergeOperation()
  {
    return _mergeOperation;
  }
}