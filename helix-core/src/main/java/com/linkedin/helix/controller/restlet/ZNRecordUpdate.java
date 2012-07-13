package com.linkedin.helix.controller.restlet;

import org.I0Itec.zkclient.DataUpdater;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordUpdater;
/**
 * Unit of transfered ZNRecord updates. Contains the ZNRecord Value, zkPath
 * to store the update value, and the property type (used to merge the ZNRecord)
 * For ZNRecord subtraction, it is currently not supported yet. 
 * */
public class ZNRecordUpdate
{
  public enum OpCode
  {
    // TODO: create is not supported; but update will create if not exist
    CREATE,
    UPDATE,
    SET
  }
  final String _path;
  ZNRecord _record;
  final OpCode _code;

  @JsonCreator
  public ZNRecordUpdate(@JsonProperty("path")String path, 
                        @JsonProperty("opcode")OpCode code, 
                        @JsonProperty("record")ZNRecord record)
  {
    _path = path;
    _record = record;
    _code = code;
  }
  
  public String getPath()
  {
    return _path;
  }
  
  public ZNRecord getRecord()
  {
    return _record;
  }
  
  public OpCode getOpcode()
  {
    return _code;
  }

  @JsonIgnore(true)
  public DataUpdater<ZNRecord> getZNRecordUpdater()
  {
    if(_code == OpCode.SET)

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
    else if ((_code == OpCode.UPDATE))
    {
      return new ZNRecordUpdater(_record);
    }
    else
    {
      throw new UnsupportedOperationException("Not supported : " + _code);
    }
  }
}