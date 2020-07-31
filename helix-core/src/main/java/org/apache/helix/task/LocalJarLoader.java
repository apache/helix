package org.apache.helix.task;

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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalJarLoader implements JarLoader {
  private static final Logger LOG = LoggerFactory.getLogger(LocalJarLoader.class);

  @Override
  public URL openJar(String jar) {
    File taskJar;
    try {
      taskJar = new File(jar);
      if (taskJar.exists() && !taskJar.isDirectory()) {
        return taskJar.toURI().toURL();
      } else {
        LOG.error("Failed to find JAR " + jar + " for new task.");
        throw new IllegalStateException("No JAR for task");
      }
    } catch (MalformedURLException e) {
      LOG.error("Failed to open JAR " + jar + " for new task.");
      throw new IllegalStateException("Malformed JAR URL for task");
    }
  }
}
