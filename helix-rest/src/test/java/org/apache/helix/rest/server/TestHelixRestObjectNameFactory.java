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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import javax.management.ObjectName;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixRestObjectNameFactory {
  @Test
  public void createsObjectNameWithDomainInInput() {
    HelixRestObjectNameFactory f = new HelixRestObjectNameFactory("namespace");
    ObjectName objectName = f.createName("type", "org.apache.helix.rest", "something.with.dots");
    Assert.assertEquals(objectName.getDomain(), "org.apache.helix.rest");
  }

  @Test
  public void createsObjectNameWithNameAsKeyPropertyName() {
    HelixRestObjectNameFactory f = new HelixRestObjectNameFactory("helix.rest");
    ObjectName objectName =
        f.createName("counter", "org.apache.helix.rest", "something.with.dots");
    Assert.assertEquals(objectName.getKeyProperty("name"), "something.with.dots");
    Assert.assertEquals(objectName.getKeyProperty("namespace"), "helix.rest");
    Assert.assertEquals(objectName.getKeyProperty("type"), "counter");
  }
}
