package org.apache.helix.mock.participant;

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

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

public class MockTransition {
  private static Logger LOG = Logger.getLogger(MockTransition.class);

  // called by state model transition functions
  public void doTransition(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info("default doTransition() invoked");
  }

  // called by state model reset function
  public void doReset() {
    LOG.info("default doReset() invoked");
  }

}
