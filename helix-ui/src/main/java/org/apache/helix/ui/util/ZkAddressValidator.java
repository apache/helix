package org.apache.helix.ui.util;

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

import java.util.HashSet;
import java.util.Set;

public class ZkAddressValidator {

  private final Set<String> zkMachines;

  public ZkAddressValidator(Set<String> zkAddresses) {
    if (zkAddresses == null) {
      this.zkMachines = null;
    } else {
      this.zkMachines = new HashSet<String>();
      for (String zkAddress : zkAddresses) {
        for (String machine : zkAddress.split(",")) {
          this.zkMachines.add(machine);
        }
      }
    }
  }

  public boolean validate(String zkAddress) {
    if (zkMachines == null) {
      return true;
    }

    String[] machines = zkAddress.split(",");
    for (String machine : machines) {
      if (!zkMachines.contains(machine)) {
        return false;
      }
    }

    return true;
  }
}
