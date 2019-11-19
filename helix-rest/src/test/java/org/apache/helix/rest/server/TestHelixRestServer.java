package org.apache.helix.rest.server;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.helix.TestHelper;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.server.auditlog.AuditLogger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixRestServer extends AbstractTestClass {
  @Test
  public void testInvalidHelixRestServerInitialization() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Namespace manifests has invalid metadata store type should generate failure
    try {
      List<HelixRestNamespace> invalidManifest1 = new ArrayList<>();
      invalidManifest1.add(
          new HelixRestNamespace("test1", HelixRestNamespace.HelixMetadataStoreType.valueOf("InvalidMetadataStore"),
              ZK_ADDR, false));
      HelixRestServer svr = new HelixRestServer(invalidManifest1, 10250, "/", Collections.<AuditLogger>emptyList());
      Assert.assertFalse(true, "InvalidManifest1 test failed");
    } catch (IllegalArgumentException e) {
      // OK
    }

    // Namespace manifests has invalid namespace name shall generate failure
    try {
      List<HelixRestNamespace> invalidManifest2 = new ArrayList<>();
      invalidManifest2.add(
          new HelixRestNamespace("", HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, ZK_ADDR, true));
      HelixRestServer svr = new HelixRestServer(invalidManifest2, 10250, "/", Collections.<AuditLogger>emptyList());
      Assert.assertFalse(true, "InvalidManifest2 test failed");
    } catch (IllegalArgumentException e) {
      // OK
    }

    // Duplicated namespace shall cause exception
    try {
      List<HelixRestNamespace> invalidManifest3 = new ArrayList<>();
      invalidManifest3.add(
          new HelixRestNamespace("DuplicatedName", HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, ZK_ADDR, false));
      invalidManifest3.add(
          new HelixRestNamespace("DuplicatedName", HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, ZK_ADDR,
              false));
      HelixRestServer svr = new HelixRestServer(invalidManifest3, 10250, "/", Collections.<AuditLogger>emptyList());
      Assert.assertFalse(true, "InvalidManifest3 test failed");
    } catch (IllegalArgumentException e) {
      // OK
    }

    // More than 1 default namespace shall cause failure
    try {
      List<HelixRestNamespace> invalidManifest4 = new ArrayList<>();
      invalidManifest4.add(
          new HelixRestNamespace("test4-1", HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, ZK_ADDR, true));
      invalidManifest4.add(
          new HelixRestNamespace("test4-2", HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, ZK_ADDR, true));
      HelixRestServer svr = new HelixRestServer(invalidManifest4, 10250, "/", Collections.<AuditLogger>emptyList());
      Assert.assertFalse(true, "InvalidManifest4 test failed");
    } catch (IllegalStateException e) {
      // OK
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

}
