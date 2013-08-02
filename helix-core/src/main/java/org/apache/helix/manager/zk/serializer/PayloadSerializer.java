package org.apache.helix.manager.zk.serializer;

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

/**
 * Interface for converting back and forth between raw bytes and generic objects
 */
public interface PayloadSerializer {

  /**
   * Convert a generic object instance to raw bytes
   * @param data instance of the generic type
   * @return byte array representing the object
   */
  public <T> byte[] serialize(final T data);

  /**
   * Convert raw bytes to a generic object instance
   * @param clazz The class represented by the deserialized bytes
   * @param bytes byte array representing the object
   * @return instance of the generic type or null if the conversion failed
   */
  public <T> T deserialize(final Class<T> clazz, final byte[] bytes);
}
