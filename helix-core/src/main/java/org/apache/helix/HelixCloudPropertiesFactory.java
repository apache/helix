package org.apache.helix;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Singleton factory that build Helix cloud properties.
 */
public final class HelixCloudPropertiesFactory {
  private static final Logger logger = LoggerFactory.getLogger(HelixCloudPropertiesFactory.class);
  private static HelixCloudProperties _helixCloudProperties;

  private static class SingletonHelper {
    private static final HelixCloudPropertiesFactory INSTANCE = new HelixCloudPropertiesFactory();
  }

  public static HelixCloudPropertiesFactory getInstance() {
    return SingletonHelper.INSTANCE;
  }

  public final void buildHelixCloudProperties(HelixCloudProperties helixCloudProperties) {
    _helixCloudProperties = helixCloudProperties;
  }

  public HelixCloudProperties getHelixCloudProperties() {
    return _helixCloudProperties;
  }
}
