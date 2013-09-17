package org.apache.helix.filestore;

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

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processes the change log and invokes rsync for every change on the remote
 * machine
 */
public class ChangeLogProcessor implements Runnable {
  private final ChangeLogReader reader;
  RsyncInvoker rsyncInvoker;
  private AtomicBoolean shutdownRequested;
  private CheckpointFile checkpointFile;
  private Thread thread;

  public ChangeLogProcessor(ChangeLogReader reader, String remoteHost, String remoteBaseDir,
      String localBaseDir, String checkpointDirPath) throws Exception {
    this.reader = reader;
    checkpointFile = new CheckpointFile(checkpointDirPath);

    shutdownRequested = new AtomicBoolean(false);
    rsyncInvoker = new RsyncInvoker(remoteHost, remoteBaseDir, localBaseDir);
  }

  public void start() {
    thread = new Thread(this);
    thread.start();
  }

  public void run() {
    try {
      ChangeRecord lastRecordProcessed = checkpointFile.findLastRecordProcessed();
      do {
        try {
          List<ChangeRecord> changes = reader.getChangeSince(lastRecordProcessed);
          Set<String> paths = getRemotePathsToSync(changes);
          for (String path : paths) {
            rsyncInvoker.rsync(path);
          }
          lastRecordProcessed = changes.get(changes.size() - 1);
          checkpointFile.checkpoint(lastRecordProcessed);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } while (!shutdownRequested.get());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private Set<String> getRemotePathsToSync(List<ChangeRecord> changes) {
    Set<String> paths = new TreeSet<String>();
    for (ChangeRecord change : changes) {
      paths.add(change.file);
    }
    return paths;
  }

  public void stop() {
    shutdownRequested.set(true);
    thread.interrupt();
  }

}
