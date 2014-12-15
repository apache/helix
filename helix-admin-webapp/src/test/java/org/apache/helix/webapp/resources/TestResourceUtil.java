package org.apache.helix.webapp.resources;


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


import org.apache.helix.model.IdealState;
import org.apache.helix.webapp.resources.ClusterRepresentationUtil;
import org.apache.helix.webapp.resources.ResourceUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestResourceUtil {

  @Test
  public void testReadSimpleFieldWithoutDer() throws Exception {
    IdealState idealState = new IdealState("MyDB");
    idealState.setInstanceGroupTag("MyTag");
    String recordStr = ClusterRepresentationUtil.ZNRecordToJson(idealState.getRecord());
    String value =
        ResourceUtil.extractSimpleFieldFromZNRecord(recordStr,
            IdealState.IdealStateProperty.INSTANCE_GROUP_TAG.toString());
    Assert.assertEquals(value, "MyTag");
  }
}
