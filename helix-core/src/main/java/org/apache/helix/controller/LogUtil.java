package org.apache.helix.controller;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.slf4j.Logger;

public class LogUtil {
  public static void logInfo(Logger logger, String eventInfo, String message) {
    logger.info("Event {} : {}", eventInfo, message);
  }

  public static void logWarn(Logger logger, String eventInfo, String message) {
    logger.warn("Event {} : {}", eventInfo, message);
  }

  public static void logError(Logger logger, String eventInfo, String message) {
    logger.error("Event {} : {}", eventInfo, message);
  }

  public static void logDebug(Logger logger, String eventInfo, String message) {
    logger.debug("Event {} : {}", eventInfo, message);
  }

  public static void logInfo(Logger logger, String eventInfo, String message, Exception e) {
    logger.info("Event {} : {}", eventInfo, message, e);
  }

  public static void logWarn(Logger logger, String eventInfo, String message, Exception e) {
    logger.warn("Event {} : {}", eventInfo, message, e);
  }

  public static void logError(Logger logger, String eventInfo, String message, Exception e) {
    logger.error("Event {} : {}", eventInfo, message, e);
  }

  public static void logDebug(Logger logger, String eventInfo, String message, Exception e) {
    logger.debug("Event {} : {}", eventInfo, message, e);
  }
}
