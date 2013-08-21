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
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.jci.listeners.AbstractFilesystemAlterationListener;
import org.apache.commons.jci.monitor.FilesystemAlterationMonitor;

public class FileSystemWatchService {
  enum ChangeType {
    CREATE,
    DELETE,
    MODIFY
  };

  private FilesystemAlterationMonitor fam;
  private MyFilesystemAlterationListener listener;
  private Thread thread;

  public FileSystemWatchService(String root, FileChangeWatcher... watchers) {
    this(root, -1, watchers);
  }

  public FileSystemWatchService(String root, long startTime, FileChangeWatcher... watchers) {
    File file = new File(root);
    System.out.println("Setting up watch service for path:" + file.getAbsolutePath());
    fam = new FilesystemAlterationMonitor();
    listener = new MyFilesystemAlterationListener(root, startTime, watchers);
    fam.addListener(file, listener);
  }

  public void start() {
    fam.start();
    thread = new Thread(listener);
    thread.start();
  }

  static class MyFilesystemAlterationListener extends AbstractFilesystemAlterationListener
      implements Runnable {
    private final FileChangeWatcher[] watchers;
    private int length;
    private final long startTime;
    private AtomicBoolean stopRequest;

    public MyFilesystemAlterationListener(String root, long startTime, FileChangeWatcher[] watchers) {
      this.startTime = startTime;
      File file = new File(root);
      length = root.length() + 1;
      this.watchers = watchers;
      stopRequest = new AtomicBoolean(false);

    }

    @SuppressWarnings("unchecked")
    public void run() {
      while (!stopRequest.get()) {
        try {
          waitForCheck();
          process(getCreatedDirectories(), watchers, ChangeType.CREATE);
          process(getDeletedDirectories(), watchers, ChangeType.DELETE);
          process(getChangedDirectories(), watchers, ChangeType.MODIFY);
          process(getCreatedFiles(), watchers, ChangeType.CREATE);
          process(getDeletedFiles(), watchers, ChangeType.DELETE);
          process(getChangedFiles(), watchers, ChangeType.MODIFY);

        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    private void process(Collection<File> files, FileChangeWatcher[] watchers, ChangeType type) {
      if (files.size() > 0) {
        for (File file : files) {
          if (file.lastModified() < startTime) {
            continue;
          }
          String path = file.getPath();
          String relativePath = ".";
          if (path.length() > length) {
            relativePath = path.substring(length);
          }
          for (FileChangeWatcher watcher : watchers) {
            switch (type) {
            case CREATE:
              watcher.onEntryAdded(relativePath);
              break;
            case DELETE:
              watcher.onEntryDeleted(relativePath);
              break;
            case MODIFY:
              watcher.onEntryModified(relativePath);
              break;
            }
          }
        }
      }
    }

    public void stop() {
      stopRequest.set(true);
    }
  }

  public void stop() {
    listener.stop();
    fam.stop();
    thread.interrupt();
  }

  public static void main(String[] args) throws Exception {
    FileChangeWatcher defaultWatcher = new FileChangeWatcher() {

      @Override
      public void onEntryModified(String path) {
        System.out
            .println("FileSystemWatchService.main(...).new FileChangeWatcher() {...}.onEntryModified():"
                + path);
      }

      @Override
      public void onEntryDeleted(String path) {
        System.out
            .println("FileSystemWatchService.main(...).new FileChangeWatcher() {...}.onEntryDeleted():"
                + path);
      }

      @Override
      public void onEntryAdded(String path) {
        System.out
            .println("FileSystemWatchService.main(...).new FileChangeWatcher() {...}.onEntryAdded() : "
                + path);
      }
    };
    ChangeLogGenerator ChangeLogGenerator =
        new ChangeLogGenerator("data/localhost_12000/translog", 1, 1);
    FileSystemWatchService watchService =
        new FileSystemWatchService("data/localhost_12000/filestore", defaultWatcher);
    watchService.start();
    Thread.sleep(10000);
    watchService.stop();
  }
}
