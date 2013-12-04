package org.apache.helix.controller.context;

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

import org.apache.helix.api.id.ContextId;
import org.apache.helix.controller.serializer.DefaultStringSerializer;
import org.apache.helix.controller.serializer.StringSerializer;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A simple context that can be serialized by a {@link DefaultStringSerializer}
 */
public class BasicControllerContext implements ControllerContext {
  private final ContextId _id;
  private Class<? extends StringSerializer> _serializer;

  /**
   * Instantiate with an id
   * @param id ContextId, unique among all contexts in this cluster
   */
  public BasicControllerContext(@JsonProperty("id") ContextId id) {
    _id = id;
    _serializer = DefaultStringSerializer.class;
  }

  /**
   * Set the class that can serialize this object into a String, and back
   * @param serializer StringSerializer implementation class
   */
  public void setSerializerClass(Class<? extends StringSerializer> serializer) {
    _serializer = serializer;
  }

  @Override
  public Class<? extends StringSerializer> getSerializerClass() {
    return _serializer;
  }

  @Override
  public ContextId getId() {
    return _id;
  }
}
