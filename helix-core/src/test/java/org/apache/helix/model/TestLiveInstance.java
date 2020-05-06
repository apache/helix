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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.task.TaskConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestLiveInstance {

  @Test
  public void testGetCurrentTaskThreadPoolSize() {
    LiveInstance testLiveInstance = new LiveInstance("testId");
    testLiveInstance.getRecord()
        .setIntField(LiveInstance.LiveInstanceProperty.CURRENT_TASK_THREAD_POOL_SIZE.name(), 100);

    Assert.assertEquals(testLiveInstance.getCurrentTaskThreadPoolSize(), 100);
  }

  @Test(dependsOnMethods = "testGetCurrentTaskThreadPoolSize")
  public void testGetCurrentTaskThreadPoolSizeDefault() {
    LiveInstance testLiveInstance = new LiveInstance("testId");

    Assert.assertEquals(testLiveInstance.getCurrentTaskThreadPoolSize(), TaskConstants.DEFAULT_TASK_THREAD_POOL_SIZE);
  }

  @Test(dependsOnMethods = "testGetCurrentTaskThreadPoolSizeDefault")
  public void testSetCurrentTaskThreadPoolSize() {
    LiveInstance testLiveInstance = new LiveInstance("testId");
    testLiveInstance.setCurrentTaskThreadPoolSize(100);

    Assert.assertEquals(testLiveInstance.getCurrentTaskThreadPoolSize(), 100);
  }
}
