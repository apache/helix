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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is the factory for singleton class {@link AbstractEventHandler}
 */
public class CloudEventHandlerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CloudEventHandlerFactory.class);
  private static Map<String, AbstractEventHandler> INSTANCE_MAP = new HashMap();

  private CloudEventHandlerFactory() {
  }

  /**
   * Get an instance of AbstractEventHandler implementation.
   * @return
   */
  public static AbstractEventHandler getInstance(String eventHandlerClassName)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    synchronized (CloudEventHandlerFactory.class) {
      AbstractEventHandler instance = INSTANCE_MAP.get(eventHandlerClassName);
      if (instance == null) {
        LOG.info("Initiating an object of {}", eventHandlerClassName);
        instance = (AbstractEventHandler) (HelixUtil
            .loadClass(AbstractEventHandler.class, eventHandlerClassName)).newInstance();
        INSTANCE_MAP.put(eventHandlerClassName, instance);
      }
      return instance;
    }
  }
}
