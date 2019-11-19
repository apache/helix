package org.apache.helix.tools;

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

import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.model.ScheduledTaskSMD;
import org.apache.helix.model.StorageSchemataSMD;
import org.apache.helix.model.TaskSMD;

// TODO refactor to use StateModelDefinition.Builder
@Deprecated
public class StateModelConfigGenerator {

  public static void main(String[] args) {
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    System.out.println(new String(serializer.serialize(generateConfigForMasterSlave())));
  }

  /**
   * count -1 don't care any numeric value > 0 will be tried to be satisfied based on
   * priority N all nodes in the cluster will be assigned to this state if possible R all
   * remaining nodes in the preference list will be assigned to this state, applies only
   * to last state
   */

  @Deprecated
  public static ZNRecord generateConfigForStorageSchemata() {
    return StorageSchemataSMD.generateConfigForStorageSchemata();
  }

  @Deprecated
  public static ZNRecord generateConfigForMasterSlave() {
    return MasterSlaveSMD.generateConfigForMasterSlave();
  }

  @Deprecated
  public static ZNRecord generateConfigForLeaderStandby() {
    return LeaderStandbySMD.generateConfigForLeaderStandby();
  }

  @Deprecated
  public static ZNRecord generateConfigForOnlineOffline() {
    return OnlineOfflineSMD.generateConfigForOnlineOffline();
  }

  @Deprecated
  public static ZNRecord generateConfigForScheduledTaskQueue() {
    return ScheduledTaskSMD.generateConfigForScheduledTaskQueue();
  }

  @Deprecated
  public static ZNRecord generateConfigForTaskStateModel() {
    return TaskSMD.generateConfigForTaskStateModel();
  }
}
