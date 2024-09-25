package org.apache.helix.model;

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

import java.util.Optional;

import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * The configuration entry for persisting the client side rest endpoint
 * The rest endpoint is used for helix to fetch the health status or other important status of the participant at runtime
 */
public class RESTConfig extends HelixProperty {
  /**
   * Corresponds to "simpleFields" concept in ZnNode
   */
  public enum SimpleFields {
    /**
     * Customized URL for getting participant(instance)'s health status or partition's health status.
     */
    CUSTOMIZED_HEALTH_URL
  }

  /**
   * Instantiate REST config with a pre-populated record
   *
   * @param record a ZNRecord corresponding to a cluster configuration
   */
  public RESTConfig(ZNRecord record) {
    super(record);
  }

  public RESTConfig(String id) {
    super(id);
  }

  public void set(SimpleFields property, String value) {
    _record.setSimpleField(property.name(), value);
  }

  public String get(SimpleFields property) {
    return _record.getSimpleField(property.name());
  }

  /**
   * Resolves the customized health URL by replacing the wildcard with the instance's name
   *
   * @param instance The instance
   * @return The base restful endpoint
   */
  public String getBaseUrl(String instance) {
    String baseUrl = get(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL);

    // pre-assumption of the url, must be format of "http://*/path", the wildcard is replaceable by
    // the instance vip
    if (baseUrl == null || !baseUrl.contains("*")) {
      throw new HelixException("Invalid CUSTOMIZED_HEALTH_URL in REST config: " + baseUrl);
    }

    String instanceVip = instance;
    // instance name format could be <instanceVip>_<port> so we need to extract the instance vip
    if (instance.contains("_")) {
      instanceVip = instance.substring(0, instance.indexOf('_'));
    }

    return baseUrl.replace("*", instanceVip);
  }

  /**
   * Retrieves the complete configured health URL if no wildcard is present.
   * <p>
   * For complete URL, only aggregated customized health check is supported. For
   * partition/instance health check, the URL should have the wildcard. The example of the complete
   * URL is "http://localhost:8080/healthcheck". The example of the URL with wildcard is
   * "http://*\/path".
   * <p>
   * This method is useful when aggregated health checks are required and individual partition or
   * instance checks need to be excluded.
   * <p>
   * Returns an empty Optional if the URL contains a wildcard.
   *
   * @return Optional containing the exact configured URL, or empty if a wildcard is present.
   */
  public Optional<String> getCompleteConfiguredHealthUrl() {
    String baseUrl = get(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL);
    if (baseUrl == null || baseUrl.contains("*")) {
      return Optional.empty();
    }
    return Optional.of(baseUrl);
  }
}
