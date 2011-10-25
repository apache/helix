package com.linkedin.clustermanager.agent.zk;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
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
    if (clusterName == null || zkClient == null)
    { 
      return false;
    }
    
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
    String temp = parentPath + "/" + id;
    client.createPersistent(temp, nodeRecord);
  }

  public static void dropChildren(ZkClient client, String parentPath,
	  List<ZNRecord> list)
  {
	 //TODO: check if parentPath exists  
	if (list != null) 
	{
	  for (ZNRecord record : list)
	  {
	    dropChildren(client, parentPath, record);
	  }
	}
  }
  
  public static void dropChildren(ZkClient client, String parentPath,
	  ZNRecord nodeRecord) 
  {
	//TODO: check if parentPath exists  
	String id = nodeRecord.getId();
	String temp = parentPath + "/" + id;
    client.deleteRecursive(temp);
	 
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
      final ZNRecord record, boolean mergeOnUpdate)
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
      final ZNRecord record, final boolean persistent,
      final boolean mergeOnUpdate)
  {
    int retryCount = 0;
    while (retryCount < RETRYLIMIT)
    {
      try
      {
        if (client.exists(path))
        {
          DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>()
          {
            @Override
            public ZNRecord update(ZNRecord currentData)
            {
              if(mergeOnUpdate)
              {
                currentData.merge(record);
                return currentData;
              }
              return record;
            }
          };
          client.updateDataSerialized(path, updater);
        } else
        {
          CreateMode mode = (persistent) ? CreateMode.PERSISTENT
              : CreateMode.EPHEMERAL;
          if(record.getDeltaList().size() > 0)
          {
            ZNRecord value = new ZNRecord(record.getId());
            value.merge(record);
            client.create(path, value, mode);
          }
          else
          {
            client.create(path, record, mode);
          }
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

  public static void createOrReplace(ZkClient client, String path,
      final ZNRecord record, final boolean persistent)
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
          CreateMode mode = (persistent) ? CreateMode.PERSISTENT
              : CreateMode.EPHEMERAL;
          client.create(path, record, mode);
        }
        break;
      } catch (Exception e)
      {
        retryCount = retryCount + 1;
        logger.warn("Exception trying to createOrReplace " + path
            + " Exception:" + e.getMessage() + ". Will retry.");
      }
    }
  }

  public static void substract(
      ZkClient client,
      String path, final ZNRecord recordTosubtract)
  {
    int retryCount = 0;
    while (retryCount < RETRYLIMIT)
    {
      try
      {
        if (client.exists(path))
        {
          DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>()
          {
            @Override
            public ZNRecord update(ZNRecord currentData)
            {
              currentData.substract(recordTosubtract);
              return currentData;
            }
          };
          client.updateDataSerialized(path, updater);
          break;
        }
      } catch (Exception e)
      {
        retryCount = retryCount + 1;
        logger.warn("Exception trying to createOrReplace " + path
            + " Exception:" + e.getMessage() + ". Will retry.");
        e.printStackTrace();
      }
    }
    
  }
}
