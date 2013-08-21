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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.helix.ExternalCommand;
import org.apache.log4j.Logger;

public class SystemUtil {
  public static final String OS_NAME = System.getProperty("os.name");
  private static Logger LOG = Logger.getLogger(SystemUtil.class);

  /**
   * PROCESS STATE CODES
   */
  public static enum ProcessStateCode {
    // Here are the different values that the s, stat and state output specifiers (header "STAT" or
    // "S")
    // will display to describe the state of a process.
    D("Uninterruptible sleep (usually IO)"),
    R("Running or runnable (on run queue)"),
    S("Interruptible sleep (waiting for an event to complete)"),
    T("Stopped, either by a job control signal or because it is being traced."),
    W("paging (not valid since the 2.6.xx kernel)"),
    X("dead (should never be seen)"),
    Z("Defunct (\"zombie\") process, terminated but not reaped by its parent.");

    private final String _description;

    private ProcessStateCode(String description) {
      _description = description;
    }

    public String getDescription() {
      return _description;
    }
  }

  public static ProcessStateCode getProcessState(String processId) throws Exception {
    if (OS_NAME.equals("Mac OS X") || OS_NAME.equals("Linux")) {
      ExternalCommand cmd = ExternalCommand.start("ps", processId);
      cmd.waitFor();

      // split by new lines
      // should return 2 lines for an existing process, or 1 line for a non-existing process
      String lines[] = cmd.getStringOutput().split("[\\r\\n]+");
      if (lines.length != 2) {
        LOG.info("process: " + processId + " not exist");
        return null;
      }

      // split by whitespace, 1st line is attributes, 2nd line is actual values
      // should be parallel arrays
      String attributes[] = lines[0].trim().split("\\s+");
      String values[] = lines[1].trim().split("\\s+");

      Character processStateCodeChar = null;
      for (int i = 0; i < attributes.length; i++) {
        String attribute = attributes[i];
        // header "STAT" or "S"
        if ("STAT".equals(attribute) || "S".equals(attribute)) {
          // first character should be major process state code
          processStateCodeChar = values[i].charAt(0);
          break;
        }
      }

      return ProcessStateCode.valueOf(Character.toString(processStateCodeChar));
    } else {
      throw new UnsupportedOperationException("Not supported OS: " + OS_NAME);
    }
  }

  public static String getPidFromFile(File file) {
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(file));
      String line = br.readLine();
      return line;
    } catch (IOException e) {
      LOG.warn("fail to read pid from pidFile: " + file + ". will not monitor");
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          LOG.error("fail to close file: " + file, e);
        }
      }
    }
    return null;
  }
}
