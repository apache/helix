package org.apache.helix.provisioning;

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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class TaskConfig {
  private static final Logger LOG = Logger.getLogger(TaskConfig.class);

  public Map<String, String> config = new HashMap<String, String>();
  public String yamlFile;
  public String name;

  public URI getYamlURI() {
    try {
      return yamlFile != null ? new URI(yamlFile) : null;
    } catch (URISyntaxException e) {
      LOG.error("Error parsing URI for task config", e);
    }
    return null;
  }

  public String getValue(String key) {
    return (config != null ? config.get(key) : null);
  }
}
