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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.helix.ExternalCommand;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.State;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

@StateModelInfo(initialState = "OFFLINE", states = {})
public class AgentStateModel extends TransitionHandler {
  private static final Logger _logger = Logger.getLogger(AgentStateModel.class);
  private static Pattern pattern = Pattern.compile("(\\{.+?\\})");

  private static String buildKey(String fromState, String toState, CommandAttribute attribute) {
    return fromState + "-" + toState + "." + attribute.getName();
  }

  private static String instantiateByMessage(String string, Message message) {
    Matcher matcher = pattern.matcher(string);
    String result = string;
    while (matcher.find()) {
      String var = matcher.group();
      result =
          result.replace(var,
              message.getAttribute(Message.Attributes.valueOf(var.substring(1, var.length() - 1))));
    }

    return result;
  }

  @Transition(to = "*", from = "*")
  public void genericStateTransitionHandler(Message message, NotificationContext context)
      throws Exception {
    // first try get command from message
    String cmd = message.getRecord().getSimpleField(CommandAttribute.COMMAND.getName());
    String workingDir = message.getRecord().getSimpleField(CommandAttribute.WORKING_DIR.getName());
    String timeout = message.getRecord().getSimpleField(CommandAttribute.TIMEOUT.getName());
    String pidFile = message.getRecord().getSimpleField(CommandAttribute.PID_FILE.getName());

    HelixManager manager = context.getManager();
    String clusterName = manager.getClusterName();
    State fromState = message.getTypedFromState();
    State toState = message.getTypedToState();

    // construct keys for command-config
    String cmdKey = buildKey(fromState.toString(), toState.toString(), CommandAttribute.COMMAND);
    String workingDirKey =
        buildKey(fromState.toString(), toState.toString(), CommandAttribute.WORKING_DIR);
    String timeoutKey =
        buildKey(fromState.toString(), toState.toString(), CommandAttribute.TIMEOUT);
    String pidFileKey =
        buildKey(fromState.toString(), toState.toString(), CommandAttribute.PID_FILE);
    List<String> cmdConfigKeys = Arrays.asList(cmdKey, workingDirKey, timeoutKey, pidFileKey);

    // read command from resource-scope configures
    if (cmd == null) {
      HelixConfigScope resourceScope =
          new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(clusterName)
              .forResource(message.getResourceId().stringify()).build();
      Map<String, String> cmdKeyValueMap =
          manager.getConfigAccessor().get(resourceScope, cmdConfigKeys);
      if (cmdKeyValueMap != null) {
        cmd = cmdKeyValueMap.get(cmdKey);
        workingDir = cmdKeyValueMap.get(workingDirKey);
        timeout = cmdKeyValueMap.get(timeoutKey);
        pidFile = cmdKeyValueMap.get(pidFileKey);
      }
    }

    // if resource-scope doesn't contain command, fall back to cluster-scope configures
    if (cmd == null) {
      HelixConfigScope clusterScope =
          new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
      Map<String, String> cmdKeyValueMap =
          manager.getConfigAccessor().get(clusterScope, cmdConfigKeys);

      if (cmdKeyValueMap != null) {
        cmd = cmdKeyValueMap.get(cmdKey);
        workingDir = cmdKeyValueMap.get(workingDirKey);
        timeout = cmdKeyValueMap.get(timeoutKey);
        pidFile = cmdKeyValueMap.get(pidFileKey);
      }
    }

    if (cmd == null) {
      throw new Exception("Unable to find command for transition from:" + message.getTypedFromState()
          + " to:" + message.getTypedToState());
    }
    _logger.info("Executing command: " + cmd + ", using workingDir: " + workingDir + ", timeout: "
        + timeout + ", on " + manager.getInstanceName());

    // skip nop command
    if (cmd.equals(CommandAttribute.NOP.getName())) {
      return;
    }

    // split the cmd to actual cmd and args[]
    String cmdSplits[] = cmd.trim().split("\\s+");
    String cmdValue = cmdSplits[0];
    String args[] = Arrays.copyOfRange(cmdSplits, 1, cmdSplits.length);

    // get the command-execution timeout
    long timeoutValue = 0; // 0 means wait for ever
    if (timeout != null) {
      try {
        timeoutValue = Long.parseLong(timeout);
      } catch (NumberFormatException e) {
        // OK to use 0
      }
    }
    ExternalCommand externalCmd =
        ExternalCommand.executeWithTimeout(new File(workingDir), cmdValue, timeoutValue, args);

    int exitValue = externalCmd.exitValue();

    // debug
    // System.out.println("command: " + cmd + ", exitValue: " + exitValue
    // + " output:\n" + externalCmd.getStringOutput());

    if (_logger.isDebugEnabled()) {
      _logger.debug("command: " + cmd + ", exitValue: " + exitValue + " output:\n"
          + externalCmd.getStringOutput());
    }

    // monitor pid if pidFile exists
    if (pidFile == null) {
      // no pid to monitor
      return;
    }

    String pidFileValue = instantiateByMessage(pidFile, message);
    String pid = SystemUtil.getPidFromFile(new File(pidFileValue));

    if (pid != null) {
      new ProcessMonitorThread(pid).start();
    }
  }
}
