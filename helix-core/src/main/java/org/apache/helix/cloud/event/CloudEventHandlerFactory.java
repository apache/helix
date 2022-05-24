package org.apache.helix.cloud.event;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * This class is the factory for singleton class {@link CloudEventHandler}
 */
public class CloudEventHandlerFactory implements AbstractEventHandlerFactory{
  private static CloudEventHandler INSTANCE = null;

  private CloudEventHandlerFactory() {
  }

  /**
   * Get a CloudEventHandler instance.
   * This is a hacky way of doing this. Because user may implement their own handler and we need
   * to dymanic load. So we need a both class method and static method.
   * @return
   */
  @Override
  public CloudEventHandler getInstanceObjectFunction() {
    if (INSTANCE == null) {
      synchronized (CloudEventHandlerFactory.class) {
        if (INSTANCE == null) {
          INSTANCE = new CloudEventHandler();
        }
      }
    }
    return INSTANCE;
  }

  public static CloudEventHandler getInstance() {
    if (INSTANCE == null) {
      synchronized (CloudEventHandlerFactory.class) {
        if (INSTANCE == null) {
          INSTANCE = new CloudEventHandler();
        }
      }
    }
    return INSTANCE;
  }
}
