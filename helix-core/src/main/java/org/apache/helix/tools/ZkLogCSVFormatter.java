package org.apache.helix.tools;

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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.util.HelixUtil;

public class ZkLogCSVFormatter {
  private static final ZNRecordSerializer _deserializer = new ZNRecordSerializer();
  private static String _fieldDelim = ",";

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("USAGE: ZkLogCSVFormatter log_file output_dir");
      System.exit(2);
    }
    File outputDir = new File(args[1]);
    if (!outputDir.exists() || !outputDir.isDirectory()) {
      System.err.println(outputDir.getAbsolutePath() + " does NOT exist or is NOT a directory");
      System.exit(2);
    }
    format(args[0], args[1]);
  }

  private static void formatter(BufferedWriter bw, String... args) {
    StringBuffer sb = new StringBuffer();

    if (args.length == 0) {
      return;
    } else {
      sb.append(args[0]);
      for (int i = 1; i < args.length; i++) {
        sb.append(_fieldDelim).append(args[i]);
      }
    }

    try {
      bw.write(sb.toString());
      bw.newLine();
      // System.out.println(sb.toString());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private static String getAttributeValue(String line, String attribute) {
    String[] parts = line.split("\\s");
    if (parts != null && parts.length > 0) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].startsWith(attribute)) {
          String val = parts[i].substring(attribute.length());
          return val;
        }
      }
    }
    return null;
  }

  private static void format(String logfilepath, String outputDir) throws FileNotFoundException {
    try {
      // input file
      FileInputStream fis = new FileInputStream(logfilepath);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));

      // output files
      FileOutputStream isFos = new FileOutputStream(outputDir + "/" + "idealState.csv");
      BufferedWriter isBw = new BufferedWriter(new OutputStreamWriter(isFos));

      FileOutputStream cfgFos = new FileOutputStream(outputDir + "/" + "config.csv");
      BufferedWriter cfgBw = new BufferedWriter(new OutputStreamWriter(cfgFos));

      FileOutputStream evFos = new FileOutputStream(outputDir + "/" + "externalView.csv");
      BufferedWriter evBw = new BufferedWriter(new OutputStreamWriter(evFos));

      FileOutputStream smdCntFos =
          new FileOutputStream(outputDir + "/" + "stateModelDefStateCount.csv");
      BufferedWriter smdCntBw = new BufferedWriter(new OutputStreamWriter(smdCntFos));

      FileOutputStream smdNextFos =
          new FileOutputStream(outputDir + "/" + "stateModelDefStateNext.csv");
      BufferedWriter smdNextBw = new BufferedWriter(new OutputStreamWriter(smdNextFos));

      FileOutputStream csFos = new FileOutputStream(outputDir + "/" + "currentState.csv");
      BufferedWriter csBw = new BufferedWriter(new OutputStreamWriter(csFos));

      FileOutputStream msgFos = new FileOutputStream(outputDir + "/" + "messages.csv");
      BufferedWriter msgBw = new BufferedWriter(new OutputStreamWriter(msgFos));

      FileOutputStream hrPerfFos =
          new FileOutputStream(outputDir + "/" + "healthReportDefaultPerfCounters.csv");
      BufferedWriter hrPerfBw = new BufferedWriter(new OutputStreamWriter(hrPerfFos));

      FileOutputStream liFos = new FileOutputStream(outputDir + "/" + "liveInstances.csv");
      BufferedWriter liBw = new BufferedWriter(new OutputStreamWriter(liFos));

      formatter(cfgBw, "timestamp", "instanceName", "host", "port", "enabled");
      formatter(isBw, "timestamp", "resourceName", "partitionNumber", "mode", "partition",
          "instanceName", "priority");
      formatter(evBw, "timestamp", "resourceName", "partition", "instanceName", "state");
      formatter(smdCntBw, "timestamp", "stateModel", "state", "count");
      formatter(smdNextBw, "timestamp", "stateModel", "from", "to", "next");
      formatter(liBw, "timestamp", "instanceName", "sessionId", "Operation");
      formatter(csBw, "timestamp", "resourceName", "partition", "instanceName", "sessionId",
          "state");
      formatter(msgBw, "timestamp", "resourceName", "partition", "instanceName", "sessionId",
          "from", "to", "messageType", "messageState");
      formatter(hrPerfBw, "timestamp", "instanceName", "availableCPUs", "averageSystemLoad",
          "freeJvmMemory", "freePhysicalMemory", "totalJvmMemory");

      Map<String, ZNRecord> liveInstanceSessionMap = new HashMap<String, ZNRecord>();

      int pos;
      String inputLine;
      while ((inputLine = br.readLine()) != null) {
        if (inputLine.indexOf("CONFIGS") != -1) {
          pos = inputLine.indexOf("CONFIGS");
          pos = inputLine.indexOf("data:{", pos);
          if (pos != -1) {
            String timestamp = getAttributeValue(inputLine, "time:");
            ZNRecord record =
                (ZNRecord) _deserializer.deserialize(inputLine.substring(pos + 5).getBytes());

            formatter(cfgBw, timestamp, record.getId(), record.getSimpleField("HOST"),
                record.getSimpleField("PORT"), record.getSimpleField("ENABLED"));

          }
        } else if (inputLine.indexOf("IDEALSTATES") != -1) {
          pos = inputLine.indexOf("IDEALSTATES");
          pos = inputLine.indexOf("data:{", pos);
          if (pos != -1) {
            String timestamp = getAttributeValue(inputLine, "time:");
            ZNRecord record =
                (ZNRecord) _deserializer.deserialize(inputLine.substring(pos + 5).getBytes());
            // System.out.println("record=" + record);
            for (String partition : record.getListFields().keySet()) {
              List<String> preferenceList = record.getListFields().get(partition);
              for (int i = 0; i < preferenceList.size(); i++) {
                String instance = preferenceList.get(i);
                formatter(isBw, timestamp, record.getId(),
                    record.getSimpleField(IdealStateProperty.NUM_PARTITIONS.toString()),
                    record.getSimpleField(IdealStateProperty.REBALANCE_MODE.toString()), partition,
                    instance, Integer.toString(i));
              }
            }
          }
        } else if (inputLine.indexOf("LIVEINSTANCES") != -1) {
          pos = inputLine.indexOf("LIVEINSTANCES");
          pos = inputLine.indexOf("data:{", pos);
          if (pos != -1) {
            String timestamp = getAttributeValue(inputLine, "time:");
            ZNRecord record =
                (ZNRecord) _deserializer.deserialize(inputLine.substring(pos + 5).getBytes());
            formatter(liBw, timestamp, record.getId(), record.getSimpleField("SESSION_ID"), "ADD");
            String zkSessionId = getAttributeValue(inputLine, "session:");
            if (zkSessionId == null) {
              System.err.println("no zk session id associated with the adding of live instance: "
                  + inputLine);
            } else {
              liveInstanceSessionMap.put(zkSessionId, record);
            }
          }

        } else if (inputLine.indexOf("EXTERNALVIEW") != -1) {
          pos = inputLine.indexOf("EXTERNALVIEW");
          pos = inputLine.indexOf("data:{", pos);
          if (pos != -1) {
            String timestamp = getAttributeValue(inputLine, "time:");
            ZNRecord record =
                (ZNRecord) _deserializer.deserialize(inputLine.substring(pos + 5).getBytes());
            // System.out.println("record=" + record);
            for (String partition : record.getMapFields().keySet()) {
              Map<String, String> stateMap = record.getMapFields().get(partition);
              for (String instance : stateMap.keySet()) {
                String state = stateMap.get(instance);
                formatter(evBw, timestamp, record.getId(), partition, instance, state);
              }
            }
          }
        } else if (inputLine.indexOf("STATEMODELDEFS") != -1) {
          pos = inputLine.indexOf("STATEMODELDEFS");
          pos = inputLine.indexOf("data:{", pos);
          if (pos != -1) {
            String timestamp = getAttributeValue(inputLine, "time:");
            ZNRecord record =
                (ZNRecord) _deserializer.deserialize(inputLine.substring(pos + 5).getBytes());

            for (String stateInfo : record.getMapFields().keySet()) {
              if (stateInfo.endsWith(".meta")) {
                Map<String, String> metaMap = record.getMapFields().get(stateInfo);
                formatter(smdCntBw, timestamp, record.getId(),
                    stateInfo.substring(0, stateInfo.indexOf('.')), metaMap.get("count"));
              } else if (stateInfo.endsWith(".next")) {
                Map<String, String> nextMap = record.getMapFields().get(stateInfo);
                for (String destState : nextMap.keySet()) {
                  formatter(smdNextBw, timestamp, record.getId(),
                      stateInfo.substring(0, stateInfo.indexOf('.')), destState,
                      nextMap.get(destState));
                }
              }
            }
          }
        } else if (inputLine.indexOf("CURRENTSTATES") != -1) {
          pos = inputLine.indexOf("CURRENTSTATES");
          pos = inputLine.indexOf("data:{", pos);
          if (pos != -1) {
            String timestamp = getAttributeValue(inputLine, "time:");
            ZNRecord record =
                (ZNRecord) _deserializer.deserialize(inputLine.substring(pos + 5).getBytes());
            // System.out.println("record=" + record);
            for (String partition : record.getMapFields().keySet()) {
              Map<String, String> stateMap = record.getMapFields().get(partition);
              String path = getAttributeValue(inputLine, "path:");
              if (path != null) {
                String instance = HelixUtil.getInstanceNameFromPath(path);
                formatter(csBw, timestamp, record.getId(), partition, instance,
                    record.getSimpleField("SESSION_ID"), stateMap.get("CURRENT_STATE"));
              }
            }
          }
        } else if (inputLine.indexOf("MESSAGES") != -1) {
          pos = inputLine.indexOf("MESSAGES");
          pos = inputLine.indexOf("data:{", pos);
          if (pos != -1) {
            String timestamp = getAttributeValue(inputLine, "time:");
            ZNRecord record =
                (ZNRecord) _deserializer.deserialize(inputLine.substring(pos + 5).getBytes());

            formatter(msgBw, timestamp, record.getSimpleField("RESOURCE_NAME"),
                record.getSimpleField("PARTITION_NAME"), record.getSimpleField("TGT_NAME"),
                record.getSimpleField("TGT_SESSION_ID"), record.getSimpleField("FROM_STATE"),
                record.getSimpleField("TO_STATE"), record.getSimpleField("MSG_TYPE"),
                record.getSimpleField("MSG_STATE"));
          }

        } else if (inputLine.indexOf("closeSession") != -1) {
          String zkSessionId = getAttributeValue(inputLine, "session:");
          if (zkSessionId == null) {
            System.err.println("no zk session id associated with the closing of zk session: "
                + inputLine);
          } else {
            ZNRecord record = liveInstanceSessionMap.remove(zkSessionId);
            // System.err.println("zkSessionId:" + zkSessionId + ", record:" + record);
            if (record != null) {
              String timestamp = getAttributeValue(inputLine, "time:");
              formatter(liBw, timestamp, record.getId(), record.getSimpleField("SESSION_ID"),
                  "DELETE");
            }
          }
        } else if (inputLine.indexOf("HEALTHREPORT/defaultPerfCounters") != -1) {
          pos = inputLine.indexOf("HEALTHREPORT/defaultPerfCounters");
          pos = inputLine.indexOf("data:{", pos);
          if (pos != -1) {
            String timestamp = getAttributeValue(inputLine, "time:");
            ZNRecord record =
                (ZNRecord) _deserializer.deserialize(inputLine.substring(pos + 5).getBytes());

            String path = getAttributeValue(inputLine, "path:");
            if (path != null) {
              String instance = HelixUtil.getInstanceNameFromPath(path);
              formatter(hrPerfBw, timestamp, instance, record.getSimpleField("availableCPUs"),
                  record.getSimpleField("averageSystemLoad"),
                  record.getSimpleField("freeJvmMemory"),
                  record.getSimpleField("freePhysicalMemory"),
                  record.getSimpleField("totalJvmMemory"));
            }
          }
        }
      }

      br.close();
      isBw.close();
      cfgBw.close();
      evBw.close();
      smdCntBw.close();
      smdNextBw.close();
      csBw.close();
      msgBw.close();
      liBw.close();
      hrPerfBw.close();
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
    }
  }
}
