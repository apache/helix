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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGetProperty {
  @Test
  public void testGetProperty() {
    String version;
    Properties props = new Properties();

    try {
      InputStream stream =
          Thread.currentThread().getContextClassLoader()
              .getResourceAsStream("cluster-manager-version.properties");
      props.load(stream);
      version = props.getProperty("clustermanager.version");
      Assert.assertNotNull(version);
      System.out.println("cluster-manager-version:" + version);
    } catch (IOException e) {
      // e.printStackTrace();
      Assert.fail("could not open cluster-manager-version.properties. ", e);
    }
  }
}
