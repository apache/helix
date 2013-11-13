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
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.log4j.Logger;

/**
 * hold helix-manager properties read from
 * helix-core/src/main/resources/cluster-manager.properties
 */
public class HelixManagerProperties {
  private static final Logger LOG = Logger.getLogger(HelixManagerProperties.class.getName());

  private final Properties _properties = new Properties();

  /**
   * Initialize properties from a file
   * @param propertyFileName
   */
  public HelixManagerProperties(String propertyFileName) {
    try {
      InputStream stream =
          Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyFileName);
      _properties.load(stream);

    } catch (Exception e) {
      String errMsg = "fail to open properties file: " + propertyFileName;
      throw new IllegalArgumentException(errMsg, e);
    }

    LOG.info("load helix-manager properties: " + _properties);
  }

  // for test purpose
  public HelixManagerProperties() {
    // load empty properties
  }

  /**
   * Get properties wrapped as {@link Properties}
   * @return Properties
   */
  public Properties getProperties() {
    return _properties;
  }

  /**
   * get helix version
   * @return version read from properties
   */
  public String getVersion() {
    return this.getProperty("clustermanager.version");
  }

  /**
   * get property for key
   * @param key
   * @return property associated by key
   */
  public String getProperty(String key) {
    String value = _properties.getProperty(key);
    if (value == null) {
      LOG.warn("no property for key: " + key);
    }

    return value;
  }

  /**
   * return true if version1 >= version2, false otherwise, ignore non-numerical strings
   * assume version in format of n.n.n-x-x, where n is number and x is any string
   * e.g. 0.6.0-incubating-SNAPSHOT
   * @param version1
   * @param version2
   * @return true if version1 >= version2, false otherwise
   */
  static boolean versionNoLessThan(String version1, String version2) {
    if (version1 == null || version2 == null) {
      LOG.warn("Skip null version check. version1: " + version1 + ", version2: " + version2);
      return true;
    }

    String[] version1Splits = version1.split("(\\.|-)");
    String[] version2Splits = version2.split("(\\.|-)");

    if (version1Splits == null || version1Splits.length == 0 || version2Splits == null
        || version2Splits.length == 0) {
      LOG.warn("Skip empty version check. version1: " + version1 + ", version2: " + version2);
      return true;
    }

    for (int i = 0; i < version1Splits.length && i < version2Splits.length; i++) {
      try {
        int versionNum1 = Integer.parseInt(version1Splits[i]);
        int versionNum2 = Integer.parseInt(version2Splits[i]);

        if (versionNum1 < versionNum2) {
          return false;
        } else if (versionNum1 > versionNum2) {
          return true;
        }
      } catch (Exception e) {
        // ignore non-numerical strings and strings after non-numerical strings
        LOG.warn("Skip non-numerical version check. version1: " + version1 + ", version2: "
            + version2);
        break;
      }
    }

    return true;
  }

  /**
   * return true if participantVersion is no less than minimum supported version for participant
   * false otherwise
   * @param participantVersion
   * @return true if compatible, false otherwise
   */
  public boolean isParticipantCompatible(String participantVersion) {
    return isFeatureSupported("participant", participantVersion);
  }

  /**
   * return true if participantVersion is no less than minimum supported version for the feature
   * false otherwise
   * @param featureName
   * @param participantVersion
   * @return true if supported, false otherwise
   */
  public boolean isFeatureSupported(String featureName, String participantVersion) {
    String minSupportedVersion = getProperty("minimum_supported_version." + featureName);

    return versionNoLessThan(participantVersion, minSupportedVersion);
  }

}
