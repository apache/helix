package org.apache.helix.controller.restlet;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZNRecordUpdater;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

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