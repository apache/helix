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

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

@StateModelInfo(initialState = "OFFLINE", states = {
    "OFFLINE", "MASTER", "SLAVE"
})
public class FileStoreStateModel extends TransitionHandler {
  private final class HighWaterMarkUpdater implements DataUpdater<ZNRecord> {
    private final Message message;
    private final ChangeRecord lastRecordProcessed;

    private HighWaterMarkUpdater(Message message, ChangeRecord lastRecordProcessed) {
      this.message = message;
      this.lastRecordProcessed = lastRecordProcessed;
    }

    @Override
    public ZNRecord update(ZNRecord currentData) {
      ZNRecord newRec = new ZNRecord(message.getResourceId().stringify());

      if (currentData != null) {
        int currentGen = convertToInt(newRec.getSimpleField("currentGen"), 0);
        int currentGenStartSeq = convertToInt(newRec.getSimpleField("currentGenStartSeq"), 0);
        int prevGen = convertToInt(newRec.getSimpleField("prevGen"), 0);
        int prevGenEndSeq = convertToInt(newRec.getSimpleField("prevGenEndSeq"), 0);
        newRec.setSimpleField("currentGen", Integer.toString(currentGen + 1));
        newRec.setSimpleField("currentGenStartSeq", Integer.toString(1));
        if (currentGen > 0) {
          newRec.setSimpleField("prevGen", Integer.toString(currentGen));
          int localEndSeq = 1;
          if (lastRecordProcessed != null) {
            localEndSeq = (int) lastRecordProcessed.txid;
          }
          newRec.setSimpleField("prevGenEndSeq", "" + localEndSeq);
        }
        newRec.merge(currentData);
      } else {
        newRec.setSimpleField("currentGen", Integer.toString(1));
        newRec.setSimpleField("currentGenStartSeq", Integer.toString(1));
      }
      return newRec;

    }

    private int convertToInt(String number, int defaultValue) {
      try {
        if (number != null) {
          return Integer.parseInt(number);
        }
      } catch (Exception e) {

      }
      return defaultValue;
    }
  }

  private static Logger LOG = Logger.getLogger(FileStoreStateModel.class);

  private final String _serverId;
  private final String _partition;

  private Replicator replicator;

  private ChangeLogGenerator generator;

  private FileSystemWatchService service;

  private InstanceConfig instanceConfig;

  public FileStoreStateModel(HelixManager manager, String resource, String partition) {
    String clusterName = manager.getClusterName();
    String instanceName = manager.getInstanceName();
    instanceConfig = manager.getClusterManagmentTool().getInstanceConfig(clusterName, instanceName);
    replicator = new Replicator(instanceConfig, resource, partition);
    try {
      manager.addExternalViewChangeListener(replicator);
    } catch (Exception e) {
      e.printStackTrace();
    }
    _partition = partition;
    _serverId = instanceName;
  }

  /**
   * If the node is slave, start the rsync thread if it is not started
   * @param message
   * @param context
   * @throws Exception
   */

  @Transition(from = "OFFLINE", to = "SLAVE")
  public void onBecomeSlaveFromOffline(Message message, NotificationContext context)
      throws Exception {
    System.out.println(_serverId + " transitioning from " + message.getTypedFromState() + " to "
        + message.getTypedToState() + " for " + _partition);

    replicator.start();
    System.out.println(_serverId + " transitioned from " + message.getTypedFromState() + " to "
        + message.getTypedToState() + " for " + _partition);
  }

  /**
   * When the node becomes master, it will start accepting writes and increments
   * the epoch and starts logging the changes in a file
   * @param message
   * @param context
   * @throws Exception
   */
  @Transition(from = "SLAVE", to = "MASTER")
  public void onBecomeMasterFromSlave(final Message message, NotificationContext context)
      throws Exception {
    replicator.stop();
    System.out.println(_serverId + " transitioning from " + message.getTypedFromState() + " to "
        + message.getTypedToState() + " for " + _partition);
    ZkHelixPropertyStore<ZNRecord> helixPropertyStore =
        context.getManager().getHelixPropertyStore();
    String checkpointDirPath = instanceConfig.getRecord().getSimpleField("check_point_dir");
    CheckpointFile checkpointFile = new CheckpointFile(checkpointDirPath);
    final ChangeRecord lastRecordProcessed = checkpointFile.findLastRecordProcessed();
    DataUpdater<ZNRecord> updater = new HighWaterMarkUpdater(message, lastRecordProcessed);
    helixPropertyStore.update(
        "/TRANSACTION_ID_METADATA" + "/" + message.getResourceId().stringify(), updater,
        AccessOption.PERSISTENT);
    Stat stat = new Stat();
    ;
    ZNRecord znRecord =
        helixPropertyStore.get("/TRANSACTION_ID_METADATA" + "/"
            + message.getResourceId().stringify(), stat, AccessOption.PERSISTENT);
    int startGen = Integer.parseInt(znRecord.getSimpleField("currentGen"));
    int startSeq = Integer.parseInt(znRecord.getSimpleField("currentGenStartSeq"));
    String fileStoreDir = instanceConfig.getRecord().getSimpleField("file_store_dir");
    String changeLogDir = instanceConfig.getRecord().getSimpleField("change_log_dir");

    generator = new ChangeLogGenerator(changeLogDir, startGen, startSeq);
    // To indicate that we need callbacks for changes that happen starting now
    long now = System.currentTimeMillis();
    service = new FileSystemWatchService(fileStoreDir, now, generator);
    service.start();
    System.out.println(_serverId + " transitioned from " + message.getTypedFromState() + " to "
        + message.getTypedToState() + " for " + _partition);
  }

  /**
   * Stop writing
   * @param message
   * @param context
   * @throws Exception
   */

  @Transition(from = "MASTER", to = "SLAVE")
  public void onBecomeSlaveFromMaster(Message message, NotificationContext context)
      throws Exception {
    service.stop();
    LOG.info(_serverId + " transitioning from " + message.getTypedFromState() + " to "
        + message.getTypedToState() + " for " + _partition);
    replicator.start();
  }

  @Transition(from = "SLAVE", to = "OFFLINE")
  public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
    replicator.stop();
    LOG.info(_serverId + " transitioning from " + message.getTypedFromState() + " to "
        + message.getTypedToState() + " for " + _partition);
  }

  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    LOG.info(_serverId + " Dropping partition " + _partition);
  }

  @Override
  public void reset() {
    LOG.warn("Default reset() invoked");
  }
}
