/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.participant;

import org.testng.annotations.Test;
import org.testng.annotations.Test;

import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.participant.DistClusterControllerStateModel;
import com.linkedin.helix.participant.DistClusterControllerStateModelFactory;

public class TestDistControllerStateModelFactory
{
  final String zkAddr = ZkUnitTestBase.ZK_ADDR;
      
  @Test(groups = { "unitTest" })
  public void testDistControllerStateModelFactory()
  {
    DistClusterControllerStateModelFactory factory = new DistClusterControllerStateModelFactory(zkAddr);
    DistClusterControllerStateModel stateModel = factory.createNewStateModel("key");
    stateModel.onBecomeStandbyFromOffline(null, null);
  }
}
