package com.linkedin.clustermanager.monitoring;

import java.io.StringWriter;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.util.CMUtil;

public class ZKPathDataDumpTask extends TimerTask
{
  static Logger logger = Logger.getLogger(ZKPathDataDumpTask.class);

  private final int _thresholdNoChangeInMs;
  private final ClusterManager _manager;
  private final ZkClient _zkClient;

  public ZKPathDataDumpTask(ClusterManager manager, ZkClient zkClient,
      int thresholdNoChangeInMs)
  {
    _manager = manager;
    _zkClient = zkClient;
    logger.trace("Scannning cluster statusUpdate " + manager.getClusterName() + " thresholdNoChangeInMs: "
        + thresholdNoChangeInMs);
    _thresholdNoChangeInMs = thresholdNoChangeInMs;
  }

  @Override
  public void run()
  {
    // For each record in status update and error node
    // TODO: for now the status updates are dumped to cluster manager log4j log.
    // We need to think if we should create per-instance log files that contains per-instance statusUpdates
    // and errors 
    logger.trace("Scannning status updates ...");
    try
    {
      List<ZNRecord> instances = _manager.getDataAccessor()
          .getChildValues(PropertyType.CONFIGS);
      for (ZNRecord instance : instances)
      {
        String instanceName = instance.getId();
        scanPath(CMUtil.getInstancePropertyPath(_manager.getClusterName(),
            instanceName, PropertyType.STATUSUPDATES),
            _thresholdNoChangeInMs);
        scanPath(CMUtil.getInstancePropertyPath(_manager.getClusterName(),
            instanceName, PropertyType.ERRORS),
            _thresholdNoChangeInMs * 3);
      }
      scanPath(CMUtil.getControllerPropertyPath(_manager.getClusterName(),
          PropertyType.STATUSUPDATES), _thresholdNoChangeInMs);
      
      scanPath(CMUtil.getControllerPropertyPath(_manager.getClusterName(),
          PropertyType.ERRORS), _thresholdNoChangeInMs * 3);
    } 
    catch (Exception e)
    {
      logger.error(e);
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
        checkAndDump(nextPath, thresholdNoChangeInMs);
      } 
      catch (Exception e)
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
      logger.info(nowInMs + " " + lastModifiedTimeInMs + " " + fullPath);

      // Check the last modified time
      if (nowInMs > lastModifiedTimeInMs)
      {
        long timeDiff = nowInMs - lastModifiedTimeInMs;
        if (timeDiff > thresholdNoChangeInMs)
        {
          logger.info("Dumping status update path " + fullPath + " " + timeDiff
              + "MS has passed");
          _zkClient.setZkSerializer(new ZNRecordSerializer());
          ZNRecord record = _zkClient.readData(fullPath);

          // dump the node content into log file
          ObjectMapper mapper = new ObjectMapper();
          SerializationConfig serializationConfig = mapper
              .getSerializationConfig();
          serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT,
              true);

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
    if(_zkClient.getChildren(path).size() == 0)
    {
      _zkClient.delete(path);
    }
  }
}
