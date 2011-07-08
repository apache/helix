package com.linkedin.clustermanager.agent.zk;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.util.CMUtil;

public final class ZKUtil
{
  private static Logger logger = Logger.getLogger(ZKUtil.class);
  private static int RETRYLIMIT = 3;

  private ZKUtil()
  {

  }
  
  public static boolean isClusterSetup(String clusterName, ZkClient zkClient)
  {
    String idealStatePath = CMUtil.getIdealStatePath(clusterName);
    boolean isValid = zkClient.exists(idealStatePath)
        && zkClient.exists(CMUtil.getConfigPath(clusterName))
        && zkClient.exists(CMUtil.getLiveInstancesPath(clusterName))
        && zkClient.exists(CMUtil.getMemberInstancesPath(clusterName))
        && zkClient.exists(CMUtil.getExternalViewPath(clusterName));
    return isValid;
  }

  public static void createChildren(ZkClient client, String parentPath,
      List<ZNRecord> list)
  {
    client.createPersistent(parentPath, true);
    if (list != null)
    {
      for (ZNRecord record : list)
      {
        createChildren(client, parentPath, record);
      }
    }
  }

  public static void createChildren(ZkClient client, String parentPath,
      ZNRecord nodeRecord)
  {
    client.createPersistent(parentPath, true);

    String id = nodeRecord.getId();
    if (id != null)
    {
      String temp = parentPath + "/" + id;
      client.createPersistent(temp, nodeRecord);
    } else
    {
      logger.warn("Not creating child under " + parentPath
          + " record data does not have id: ZNRecord:" + nodeRecord);
    }
  }

  public static List<ZNRecord> getChildren(ZkClient client, String path)
  {
    // parent watch will be set by zkClient
    List<String> children = client.getChildren(path);
    List<ZNRecord> childRecords = new ArrayList<ZNRecord>();
    for (String child : children)
    {
      String childPath = path + "/" + child;
      ZNRecord record = client.readData(childPath, true);
      if (record != null)
      {
        childRecords.add(record);
      }
    }
    return childRecords;
  }

  public static void updateIfExists(ZkClient client, String path,
      final ZNRecord record)
  {

    if (client.exists(path))
    {
      DataUpdater<Object> updater = new DataUpdater<Object>()
      {
        @Override
        public Object update(Object currentData)
        {
          return record;
        }
      };
      client.updateDataSerialized(path, updater);
    }
  }

  public static void createOrUpdate(ZkClient client, String path,
      final ZNRecord record, CreateMode mode)
  {

    int retryCount = 0;
    while (retryCount < RETRYLIMIT)
    {
      try
      {
        if (client.exists(path))
        {
          DataUpdater<Object> updater = new DataUpdater<Object>()
          {
            @Override
            public Object update(Object currentData)
            {
              return record;
            }
          };
          client.updateDataSerialized(path, updater);
        } else
        {
          client.create(path, record, mode);
        }
        break;
      } catch (Exception e)
      {
        retryCount = retryCount + 1;
        logger.warn("Exception trying to update " + path + " Exception:"
            + e.getMessage() + ". Will retry.");
      }
    }
  }
}
