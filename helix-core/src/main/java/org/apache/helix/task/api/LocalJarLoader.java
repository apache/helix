package org.apache.helix.task.api;

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

  /**
   * Loads a local JAR file and returns its URL.
   * @param jarPath path of the JAR file
   * @return URL of the JAR file at path jarPath
   */
  @Override
  public URL loadJar(String jarPath) {
    // If taskJarFile doesn't exist or it's a directory, throw exception
    File taskJarFile = new File(jarPath);
    if (!taskJarFile.exists() || taskJarFile.isDirectory()) {
      LOG.error("Failed to find JAR " + jarPath + " for new task.");
      throw new IllegalStateException("No JAR for task");
    }

    try {
      return taskJarFile.toURI().toURL();
    } catch (MalformedURLException e) {
      LOG.error("Failed to open JAR " + jarPath + " for new task.");
      throw new IllegalStateException("Malformed JAR URL for task");
    }
  }
}
