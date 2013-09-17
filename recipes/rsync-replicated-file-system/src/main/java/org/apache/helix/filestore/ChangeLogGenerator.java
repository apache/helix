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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.helix.filestore.FileSystemWatchService.ChangeType;

public class ChangeLogGenerator implements FileChangeWatcher {
  Lock lock;
  private long currentSeq;
  private long currentGen;
  private int entriesLogged;
  private DataOutputStream out;
  private final String directory;

  public ChangeLogGenerator(String directory, long startGen, long startSeq) throws Exception {
    this.directory = directory;
    lock = new ReentrantLock();
    currentSeq = startSeq;
    currentGen = startGen;
    setLogFile();
  }

  private void setLogFile() throws Exception {
    File file = new File(directory);
    String[] list = file.list();
    if (list == null) {
      list = new String[] {};
    }
    int max = 1;
    for (String name : list) {
      String[] split = name.split("\\.");
      if (split.length == 2) {
        try {
          int index = Integer.parseInt(split[1]);
          if (index > max) {
            max = index;
          }
        } catch (NumberFormatException e) {
          System.err.println("Invalid transaction log file found:" + name);
        }
      }
    }

    String transLogFile = directory + "/" + "log." + (max);
    System.out.println("Current file name:" + transLogFile);
    out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(transLogFile, true)));
  }

  @Override
  public void onEntryAdded(String path) {
    appendChange(path, FileSystemWatchService.ChangeType.CREATE);

  }

  @Override
  public void onEntryDeleted(String path) {
    appendChange(path, FileSystemWatchService.ChangeType.DELETE);

  }

  @Override
  public void onEntryModified(String path) {

    appendChange(path, FileSystemWatchService.ChangeType.MODIFY);

  }

  public boolean appendChange(String path, ChangeType type) {
    lock.lock();
    if (new File(path).isDirectory()) {
      return true;
    }
    try {
      ChangeRecord record = new ChangeRecord();
      record.file = path;
      record.timestamp = System.currentTimeMillis();
      currentSeq++;
      long txnId = (((long) currentGen) << 32) + ((long) currentSeq);
      record.txid = txnId;
      record.type = (short) type.ordinal();
      write(record);
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    } finally {
      lock.unlock();
    }
    return true;
  }

  private void write(ChangeRecord record) throws Exception {
    out.writeLong(record.txid);
    out.writeShort(record.type);
    out.writeLong(record.timestamp);
    out.writeUTF(record.file);
    out.flush();
    entriesLogged++;
    if (entriesLogged == 10000) {
      entriesLogged = 0;
      out.close();
      setLogFile();
    }
  }

}
