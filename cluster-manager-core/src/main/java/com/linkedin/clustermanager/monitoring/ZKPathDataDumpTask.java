package com.linkedin.clustermanager.monitoring;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.util.CMUtil;

public class ZKPathDataDumpTask extends TimerTask
{
  static Logger logger = Logger.getLogger(ZKPathDataDumpTask.class);
  
  private int _thresholdNoChangeInMs;
  private ZkClient _zkClient;
  private List<String> _paths;

  public ZKPathDataDumpTask(ZkClient zkClient, List<String> paths, int thresholdNoChangeInMs)
  {
    _zkClient = zkClient;
    logger.info("Scannning paths " + paths + " thresholdNoChangeInMs: "+ thresholdNoChangeInMs);
    _thresholdNoChangeInMs = thresholdNoChangeInMs;
    _paths = paths;
  }

  @Override
  public void run()
  {
    // For each record in status update and error node
    logger.info("Scannning paths ...");
    for(String path : _paths)
    {
      ScanPath(path);
    }
  }
  
  void ScanPath(String path)
  {
    logger.info("Scannning path " + path);
    List<String> subPaths  = _zkClient.getChildren(path);
    for(String subPath : subPaths)
    {
      String nextPath = path + "/" + subPath;
      CheckAndDump(nextPath);
    }
  }
  
  void CheckAndDump(String path)
  {
    List<String> subPaths  = _zkClient.getChildren(path);
    for(String subPath : subPaths)
    {
      String fullPath = path + "/" + subPath;
      
      Stat pathStat = _zkClient.getStat(fullPath);
      
      long lastModifiedTimeInMs = pathStat.getMtime();
      long nowInMs = new Date().getTime();
      logger.info(nowInMs + " " + lastModifiedTimeInMs + " " + fullPath);
      
      // Check the last modified time
      if(nowInMs > lastModifiedTimeInMs)
      {
        long timeDiff = nowInMs - lastModifiedTimeInMs;
        if(timeDiff > _thresholdNoChangeInMs)
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
            System.out.println(sw.toString());
          } 
          catch (JsonGenerationException e)
          {
            e.printStackTrace();
            logger.error(e.toString());
          } 
          catch (JsonMappingException e)
          {
            logger.error(e.toString());
            e.printStackTrace();
          } 
          catch (IOException e)
          {
            logger.error(e.toString());
            e.printStackTrace();
          }
          // Delete the path data
          _zkClient.delete(fullPath);
        }
      }
    }
  }
  
  public static void main(String args[])
  {
    ZkClient zkClient = new ZkClient("localhost:2181", 30000);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    String path = CMUtil.getInstancePropertyPath("ESPRESSO_STORAGE", "localhost_8901", InstancePropertyType.STATUSUPDATES);
    List<String> paths = new ArrayList<String>();
    paths.add(path);
    new ZKPathDataDumpTask(zkClient, paths, 2000).run();
  }
}
