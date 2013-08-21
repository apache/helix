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
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class CheckpointFile {
  private static final String TEMP = ".bak";
  private static String CHECK_POINT_FILE = "lastprocessedchange.checkpoint";

  private String checkpointDirPath;

  public CheckpointFile(String checkpointDirPath) throws IOException {
    this.checkpointDirPath = checkpointDirPath;
    File checkpointdir = new File(checkpointDirPath);
    if (!checkpointdir.exists() && !checkpointdir.mkdirs()) {
      throw new IOException("unable to create SCN file parent:" + checkpointdir.getAbsolutePath());
    }
  }

  public void checkpoint(ChangeRecord lastRecordProcessed) throws Exception {

    // delete the temp file if one exists
    File tempCheckpointFile = new File(checkpointDirPath + "/" + CHECK_POINT_FILE + TEMP);
    if (tempCheckpointFile.exists() && !tempCheckpointFile.delete()) {
      System.err.println("unable to erase temp SCN file: " + tempCheckpointFile.getAbsolutePath());
    }

    String checkpointFileName = checkpointDirPath + "/" + CHECK_POINT_FILE;
    File checkpointfile = new File(checkpointFileName);
    if (checkpointfile.exists() && !checkpointfile.renameTo(tempCheckpointFile)) {
      System.err.println("unable to backup scn file");
    }
    if (!checkpointfile.createNewFile()) {
      System.err.println("unable to create new SCN file:" + checkpointfile.getAbsolutePath());
    }
    FileWriter writer = new FileWriter(checkpointfile);
    writer.write(lastRecordProcessed.toString());
    writer.flush();
    writer.close();
    System.out.println("scn persisted: " + lastRecordProcessed.txid);

  }

  public ChangeRecord findLastRecordProcessed() {
    String checkpointFileName = checkpointDirPath + "/" + CHECK_POINT_FILE;
    File file = new File(checkpointFileName);
    ChangeRecord record = null;
    if (file.exists()) {
      try {
        String line = FileUtils.readFileToString(file);
        record = ChangeRecord.fromString(line);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return record;
  }
}
