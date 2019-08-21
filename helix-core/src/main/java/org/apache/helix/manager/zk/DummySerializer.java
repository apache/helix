package org.apache.helix.manager.zk;

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

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.HelixException;


/**
 * An implementation of ZKSerializer that does nothing but pass the argument through. To be used
 * with {@link ZkBucketDataAccessor}.
 */
public class DummySerializer implements ZkSerializer {
  @Override
  public byte[] serialize(Object data) throws ZkMarshallingError {
    if (data instanceof byte[]) {
      return (byte[]) data;
    }
    throw new HelixException("DummySerializer only supports a byte array as an argument!");
  }

  @Override
  public Object deserialize(byte[] data) throws ZkMarshallingError {
    return data;
  }
}
