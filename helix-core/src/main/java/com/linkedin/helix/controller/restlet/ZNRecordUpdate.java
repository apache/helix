package com.linkedin.helix.controller.restlet;

import org.I0Itec.zkclient.DataUpdater;

import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordUpdater;

public class ZNRecordUpdate
{
  final String _path;
  final ZNRecord _record;
  final PropertyType _type;
  
  public ZNRecordUpdate(String path, PropertyType type, ZNRecord record)
  {
    _path = path;
    _record = record;
    _type = type;
  }
  
  public String getPath()
  {
    return _path;
  }
  
  public ZNRecord getRecord()
  {
    return _record;
  }
  
  public PropertyType getPropertyType()
  {
    return _type;
  }
  
  public DataUpdater<ZNRecord> getZNRecordUpdater()
  {
    if(_type == PropertyType.HEALTHREPORT)
    {
      return new ZNRecordUpdater(_record)
      {
        @Override
        public ZNRecord update(ZNRecord current)
        {
          return _record;
        }
      };
    }
    else if ((_type == PropertyType.STATUSUPDATES))
    {
      return new ZNRecordUpdater(_record);
    }
    else
    {
      throw new UnsupportedOperationException("Not supported : " + _type);
    }
  }
}