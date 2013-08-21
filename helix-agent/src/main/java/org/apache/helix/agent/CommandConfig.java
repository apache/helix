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

import java.util.Map;
import java.util.TreeMap;

public class CommandConfig {
  private final String _fromState;
  private final String _toState;
  private final String _command;
  private final String _workingDir;
  private final String _timeout;
  private final String _pidFile;

  public CommandConfig(String fromState, String toState, String command, String workingDir,
      String timeout, String pidFile) {
    if (command == null) {
      throw new IllegalArgumentException("command is null");

    }

    _fromState = fromState;
    _toState = toState;
    _command = command;
    _workingDir = workingDir;
    _timeout = timeout;
    _pidFile = pidFile;

  }

  private String buildKey(String fromState, String toState, CommandAttribute attribute) {
    return fromState + "-" + toState + "." + attribute.getName();
  }

  public Map<String, String> toKeyValueMap() {
    Map<String, String> map = new TreeMap<String, String>();
    map.put(buildKey(_fromState, _toState, CommandAttribute.COMMAND), _command);
    if (!_command.equals(CommandAttribute.NOP.getName())) {
      if (_workingDir != null) {
        map.put(buildKey(_fromState, _toState, CommandAttribute.WORKING_DIR), _workingDir);
      }

      if (_timeout != null) {
        map.put(buildKey(_fromState, _toState, CommandAttribute.TIMEOUT), _timeout);
      }

      if (_pidFile != null) {
        map.put(buildKey(_fromState, _toState, CommandAttribute.PID_FILE), _pidFile);
      }
    }
    return map;
  }

  /**
   * builder for command-config
   */
  public static class Builder {
    private String _fromState;
    private String _toState;
    private String _command;
    private String _workingDir;
    private String _timeout;
    private String _pidFile;

    public Builder setTransition(String fromState, String toState) {
      _fromState = fromState;
      _toState = toState;
      return this;
    }

    public Builder setCommand(String command) {
      _command = command;
      return this;
    }

    public Builder setCommandWorkingDir(String workingDir) {
      _workingDir = workingDir;
      return this;
    }

    public Builder setCommandTimeout(String timeout) {
      _timeout = timeout;
      return this;
    }

    public Builder setPidFile(String pidFile) {
      _pidFile = pidFile;
      return this;
    }

    public CommandConfig build() {
      return new CommandConfig(_fromState, _toState, _command, _workingDir, _timeout, _pidFile);
    }
  }

}
