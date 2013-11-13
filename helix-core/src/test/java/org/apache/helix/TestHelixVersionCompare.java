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

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixVersionCompare {

  @Test
  public void testNullVersionCompare() {
    boolean isNoLessThan = HelixManagerProperties.versionNoLessThan(null, null);
    Assert.assertTrue(isNoLessThan, "Skip version compare if no version is specified");

    isNoLessThan = HelixManagerProperties.versionNoLessThan("1.0", null);
    Assert.assertTrue(isNoLessThan, "Skip version compare if no version is specified");

    isNoLessThan = HelixManagerProperties.versionNoLessThan(null, "1.0");
    Assert.assertTrue(isNoLessThan, "Skip version compare if no version is specified");
  }

  @Test
  public void testEmptyVersionCompare() {
    boolean isNoLessThan = HelixManagerProperties.versionNoLessThan("", "");
    Assert.assertTrue(isNoLessThan, "Skip version compare if empty version is specified");

  }

  @Test
  public void testNonNumericalVersionCompare() {
    boolean isNoLessThan =
        HelixManagerProperties.versionNoLessThan("project.version1", "project.version2");
    Assert.assertTrue(isNoLessThan, "Skip version compare if non-numerical version is specified");

  }

  @Test
  public void testNumericalVersionCompare() {
    boolean isNoLessThan = HelixManagerProperties.versionNoLessThan("0.7.0", "0.6.1");
    Assert.assertTrue(isNoLessThan);

    isNoLessThan = HelixManagerProperties.versionNoLessThan("0.5.31", "0.6.1");
    Assert.assertFalse(isNoLessThan);

    isNoLessThan = HelixManagerProperties.versionNoLessThan("0.5.31-SNAPSHOT", "0.6.1");
    Assert.assertFalse(isNoLessThan);

    isNoLessThan = HelixManagerProperties.versionNoLessThan("0.5.31-incubating", "0.6.1");
    Assert.assertFalse(isNoLessThan);

    isNoLessThan = HelixManagerProperties.versionNoLessThan("0.7.0", "0.6.1-SNAPSHOT");
    Assert.assertTrue(isNoLessThan);

    isNoLessThan = HelixManagerProperties.versionNoLessThan("0.7.0", "0.6.1-incubating");
    Assert.assertTrue(isNoLessThan);
  }
}
