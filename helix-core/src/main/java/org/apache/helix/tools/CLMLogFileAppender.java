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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.ErrorCode;

public class CLMLogFileAppender extends FileAppender {
  public CLMLogFileAppender() {
  }

  public CLMLogFileAppender(Layout layout, String filename, boolean append, boolean bufferedIO,
      int bufferSize) throws IOException {
    super(layout, filename, append, bufferedIO, bufferSize);
  }

  public CLMLogFileAppender(Layout layout, String filename, boolean append) throws IOException {
    super(layout, filename, append);
  }

  public CLMLogFileAppender(Layout layout, String filename) throws IOException {
    super(layout, filename);
  }

  public void activateOptions() {
    if (fileName != null) {
      try {
        fileName = getNewLogFileName();
        setFile(fileName, fileAppend, bufferedIO, bufferSize);
      } catch (Exception e) {
        errorHandler.error("Error while activating log options", e, ErrorCode.FILE_OPEN_FAILURE);
      }
    }
  }

  private String getNewLogFileName() {
    Calendar cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
    String time = sdf.format(cal.getTime());

    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    StackTraceElement main = stack[stack.length - 1];
    String mainClass = main.getClassName();

    return System.getProperty("user.home") + "/EspressoLogs/" + mainClass + "_" + time + ".txt";
  }
}
