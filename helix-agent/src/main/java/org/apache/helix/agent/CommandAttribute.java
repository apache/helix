package org.apache.helix.agent;

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

import java.util.HashMap;
import java.util.Map;

public enum CommandAttribute {
  COMMAND("command"),
  WORKING_DIR("command.workingDir"),
  TIMEOUT("command.timeout"),
  PID_FILE("command.pidFile"),
  NOP("nop");

  // map from name to value
  private static final Map<String, CommandAttribute> map = new HashMap<String, CommandAttribute>();
  static {
    for (CommandAttribute attr : CommandAttribute.values()) {
      map.put(attr.getName(), attr);
    }
  }

  private final String _name;

  private CommandAttribute(String name) {
    _name = name;
  }

  public String getName() {
    return _name;
  }

  public static CommandAttribute getCommandAttributeByName(String name) {
    return map.get(name);
  }
}
