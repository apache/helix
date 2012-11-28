package org.apache.helix.monitoring;

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

import java.io.StringWriter;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;


public class ZKPathDataDumpTask extends TimerTask
{
  static Logger logger = Logger.getLogger(ZKPathDataDumpTask.class);

  private final int _thresholdNoChangeInMs;
  private final HelixManager _manager;
  private final ZkClient _zkClient;

  public ZKPathDataDumpTask(HelixManager manager, ZkClient zkClient, int thresholdNoChangeInMs)
  {
    _manager = manager;
    _zkClient = zkClient;
    logger.info("Scannning cluster statusUpdate " + manager.getClusterName()
        + " thresholdNoChangeInMs: " + thresholdNoChangeInMs);
    _thresholdNoChangeInMs = thresholdNoChangeInMs;
  }

  @Override
  public void run()
  {
    // For each record in status update and error node
    // TODO: for now the status updates are dumped to cluster manager log4j log.
    // We need to think if we should create per-instance log files that contains
    // per-instance statusUpdates
    // and errors
    logger.info("Scannning status updates ...");
    try
    {
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();

      List<String> instances = accessor.getChildNames(keyBuilder.instanceConfigs());
      for (String instanceName : instances)
      {
        scanPath(HelixUtil.getInstancePropertyPath(_manager.getClusterName(), instanceName,
            PropertyType.STATUSUPDATES), _thresholdNoChangeInMs);
        scanPath(HelixUtil.getInstancePropertyPath(_manager.getClusterName(), instanceName,
            PropertyType.ERRORS), _thresholdNoChangeInMs * 3);
      }
      scanPath(HelixUtil.getControllerPropertyPath(_manager.getClusterName(),
          PropertyType.STATUSUPDATES_CONTROLLER), _thresholdNoChangeInMs);

      scanPath(HelixUtil.getControllerPropertyPath(_manager.getClusterName(),
          PropertyType.ERRORS_CONTROLLER), _thresholdNoChangeInMs * 3);
    } catch (Exception e)
    {
      logger.error(e);
    }
  }

  void scanPath(String path, int thresholdNoChangeInMs)
  {
    logger.info("Scannning path " + path);
    List<String> subPaths = _zkClient.getChildren(path);
    for (String subPath : subPaths)
    {
      try
      {
        String nextPath = path + "/" + subPath;
        List<String> subSubPaths = _zkClient.getChildren(nextPath);
        for (String subsubPath : subSubPaths)
        {
          try
          {
            checkAndDump(nextPath + "/" + subsubPath, thresholdNoChangeInMs);
          } catch (Exception e)
          {
            logger.error(e);
          }
        }
      } catch (Exception e)
      {
        logger.error(e);
      }
    }
  }

  void checkAndDump(String path, int thresholdNoChangeInMs)
  {
    List<String> subPaths = _zkClient.getChildren(path);
    if(subPaths.size() == 0)
    {
      subPaths.add("");
    }
    for (String subPath : subPaths)
    {
      String fullPath = subPath.length() > 0 ? path + "/" + subPath : path;
      Stat pathStat = _zkClient.getStat(fullPath);

      long lastModifiedTimeInMs = pathStat.getMtime();
      long nowInMs = new Date().getTime();
      // logger.info(nowInMs + " " + lastModifiedTimeInMs + " " + fullPath);

      // Check the last modified time
      if (nowInMs > lastModifiedTimeInMs)
      {
        long timeDiff = nowInMs - lastModifiedTimeInMs;
        if (timeDiff > thresholdNoChangeInMs)
        {
          logger.info("Dumping status update path " + fullPath + " " + timeDiff + "MS has passed");
          _zkClient.setZkSerializer(new ZNRecordSerializer());
          ZNRecord record = _zkClient.readData(fullPath);

          // dump the node content into log file
          ObjectMapper mapper = new ObjectMapper();
          SerializationConfig serializationConfig = mapper.getSerializationConfig();
          serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

          StringWriter sw = new StringWriter();
          try
          {
            mapper.writeValue(sw, record);
            logger.info(sw.toString());
          } catch (Exception e)
          {
            logger.warn(
                    "Exception during serialization in ZKPathDataDumpTask.checkAndDump. This can mostly be ignored",
                    e);
          }
          // Delete the leaf data
          _zkClient.deleteRecursive(fullPath);
        }
      }
    }
  }
}
