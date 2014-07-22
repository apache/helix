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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ChangeLogReader implements FileChangeWatcher {
  int MAX_ENTRIES_TO_READ = 100;
  private final String changeLogDir;
  Lock lock;
  private Condition condition;

  public ChangeLogReader(String changeLogDir) {
    this.changeLogDir = changeLogDir;
    lock = new ReentrantLock();
    condition = lock.newCondition();

  }

  /**
   * Blocking call
   * @param record
   * @return
   */
  public List<ChangeRecord> getChangeSince(ChangeRecord record) {
    List<ChangeRecord> changes = new ArrayList<ChangeRecord>();
    String fileName;
    long endOffset;
    if (record == null) {
      fileName = "log.1";
      endOffset = 0;
    } else {
      fileName = record.changeLogFileName;
      endOffset = record.endOffset;
    }
    try {
      lock.lock();

      File file;
      file = new File(changeLogDir + "/" + fileName);
      while (!file.exists() || file.length() <= endOffset) {
        // wait
        try {
          System.out.println("Waiting for new changes");
          condition.await();
          System.out.println("Detected changes");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      RandomAccessFile raf = new RandomAccessFile(changeLogDir + "/" + fileName, "r");
      raf.seek(endOffset);
      // out.writeLong(record.txid);
      // out.writeShort(record.type);
      // out.writeLong(record.timestamp);
      // out.writeUTF(record.file);

      int count = 0;
      do {
        ChangeRecord newRecord = new ChangeRecord();
        newRecord.changeLogFileName = fileName;
        newRecord.startOffset = raf.getFilePointer();
        newRecord.txid = raf.readLong();
        newRecord.type = raf.readShort();
        newRecord.timestamp = raf.readLong();
        newRecord.file = raf.readUTF();
        newRecord.endOffset = raf.getFilePointer();
        changes.add(newRecord);
        count++;
      } while (count < MAX_ENTRIES_TO_READ && raf.getFilePointer() < raf.length());
      raf.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.unlock();
    }
    return changes;
  }

  @Override
  public void onEntryModified(String path) {
    try {
      lock.lock();
      condition.signalAll();
    } catch (Exception e) {
      // TODO: handle exception
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onEntryAdded(String path) {
    try {
      lock.lock();
      condition.signalAll();
    } catch (Exception e) {
      // TODO: handle exception
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onEntryDeleted(String path) {
    try {
      lock.lock();
      condition.signalAll();
    } catch (Exception e) {
      // TODO: handle exception
    } finally {
      lock.unlock();
    }
  }
}
