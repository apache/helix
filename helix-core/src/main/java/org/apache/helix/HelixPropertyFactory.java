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

import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Singleton factory that build Helix properties.
 */
public final class HelixPropertiesFactory {
  private static final Logger logger = LoggerFactory.getLogger(HelixPropertiesFactory.class);
  private final String HELIX_PARTICIPANT_PROPERTY_FILE = "HelixParticipant.properties";

  private static class SingletonHelper {
    private static final HelixPropertiesFactory INSTANCE = new HelixPropertiesFactory();
  }

  public static HelixPropertiesFactory getInstance() {
    return SingletonHelper.INSTANCE;
  }

  public HelixParticipantProperties getHelixParticipantProperties(HelixCloudProperties helixCloudProperties) {
    Properties properties = new Properties();
    try {
      InputStream stream =
          Thread.currentThread().getContextClassLoader().getResourceAsStream(HELIX_PARTICIPANT_PROPERTY_FILE);

      properties.load(stream);
    } catch (Exception e) {
      String errMsg = "fail to open properties file: " + HELIX_PARTICIPANT_PROPERTY_FILE;
      throw new IllegalArgumentException(errMsg, e);
    }

    logger.info("load helix-manager properties: " + properties);
    return new HelixParticipantProperties(properties);
  }
}
