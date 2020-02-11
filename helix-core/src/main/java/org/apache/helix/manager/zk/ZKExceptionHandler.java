package org.apache.helix.manager.zk;

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

import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKExceptionHandler {
  private static ZKExceptionHandler instance = new ZKExceptionHandler();
  private static Logger logger = LoggerFactory.getLogger(ZKExceptionHandler.class);

  private ZKExceptionHandler() {

  }

  public void handle(Exception e) {
    handle("", e);
  }

  public void handle(String msg, Exception e) {
    if (!(e instanceof ZkInterruptedException)) {
      logger.error(msg, e);
    }
  }

  public static ZKExceptionHandler getInstance() {
    return instance;
  }
}
