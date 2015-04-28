package org.apache.helix.ui.task;

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

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import org.apache.helix.ui.util.ClientCache;

import java.io.PrintWriter;

public class ClearClientCache extends Task {
  private final ClientCache clientCache;

  public ClearClientCache(ClientCache clientCache) {
    super("clearClientCache");
    this.clientCache = clientCache;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception {
    printWriter.println("Clearing ZK connections ...");
    printWriter.flush();
    clientCache.invalidateAll();
    printWriter.println("Done!");
    printWriter.flush();
  }
}
