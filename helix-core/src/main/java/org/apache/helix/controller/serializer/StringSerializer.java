package org.apache.helix.controller.serializer;

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

public interface StringSerializer {
  /**
   * Convert an object instance to a String
   * @param data instance of an arbitrary type
   * @return String representing the object
   */
  public <T> String serialize(final T data);

  /**
   * Convert raw bytes to a generic object instance
   * @param clazz The class represented by the deserialized string
   * @param string String representing the object
   * @return instance of the generic type or null if the conversion failed
   */
  public <T> T deserialize(final Class<T> clazz, final String string);
}
