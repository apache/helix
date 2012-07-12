package com.linkedin.helix.controller.restlet;

import org.I0Itec.zkclient.DataUpdater;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordUpdater;

public class ZNRecordUpdate
{
  final String _path;
  final ZNRecord _record;
  final PropertyType _type;

  @JsonCreator
  public ZNRecordUpdate(@JsonProperty("path")String path, @JsonProperty("propertyType")PropertyType type, @JsonProperty("record")ZNRecord record)
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

  @JsonIgnore(true)
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