/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.store;

import org.apache.log4j.Logger;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;

public class ZNRecordJsonSerializer implements PropertySerializer<ZNRecord>
{
  static private Logger LOG = Logger.getLogger(ZNRecordJsonSerializer.class);
  private final ZNRecordSerializer _serializer = new ZNRecordSerializer();
  
  @Override
  public byte[] serialize(ZNRecord data) throws PropertyStoreException
  {
    return _serializer.serialize(data);
  }

  @Override
  public ZNRecord deserialize(byte[] bytes) throws PropertyStoreException
  {
    return (ZNRecord) _serializer.deserialize(bytes);
  }

}
