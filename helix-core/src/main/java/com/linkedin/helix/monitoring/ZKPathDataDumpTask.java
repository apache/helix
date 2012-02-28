package com.linkedin.helix.monitoring;

import java.io.StringWriter;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.util.HelixUtil;

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
    logger.trace("Scannning cluster statusUpdate " + manager.getClusterName()
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
    logger.trace("Scannning status updates ...");
    try
    {
      List<String> instances = _manager.getDataAccessor().getChildNames(PropertyType.CONFIGS,
          ConfigScopeProperty.PARTICIPANT.toString());
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
      e.printStackTrace();
    }
  }

  void scanPath(String path, int thresholdNoChangeInMs)
  {
    logger.trace("Scannning path " + path);
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
    for (String subPath : subPaths)
    {
      String fullPath = path + "/" + subPath;
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
            logger
                .warn(
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
