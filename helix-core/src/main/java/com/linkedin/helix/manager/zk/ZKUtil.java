package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;

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

    boolean isValid = zkClient.exists(PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName))
        && zkClient.exists(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.CLUSTER.toString(), clusterName))
        && zkClient.exists(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString()))
        && zkClient.exists(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.RESOURCE.toString()))
            && zkClient.exists(PropertyPathConfig.getPath(PropertyType.LIVEINSTANCES, clusterName))
            && zkClient.exists(PropertyPathConfig.getPath(PropertyType.INSTANCES, clusterName))
            && zkClient.exists(PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName))
            && zkClient.exists(PropertyPathConfig.getPath(PropertyType.CONTROLLER, clusterName))
            && zkClient.exists(PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS, clusterName))
            && zkClient.exists(PropertyPathConfig.getPath(PropertyType.MESSAGES_CONTROLLER, clusterName))
            && zkClient.exists(PropertyPathConfig.getPath(PropertyType.ERRORS_CONTROLLER, clusterName))
            && zkClient.exists(PropertyPathConfig.getPath(PropertyType.STATUSUPDATES_CONTROLLER, clusterName))
            && zkClient.exists(PropertyPathConfig.getPath(PropertyType.HISTORY, clusterName));

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
    if (children == null || children.size() == 0)
    {
      return Collections.emptyList();
    }

    List<ZNRecord> childRecords = new ArrayList<ZNRecord>();
    for (String child : children)
    {
      String childPath = path + "/" + child;
      Stat newStat = new Stat();
      ZNRecord record = client.readDataAndStat(childPath, newStat, true);
      if (record != null)
      {
    	  record.setVersion(newStat.getVersion());
    	  record.setCreationTime(newStat.getCtime());
    	  record.setModifiedTime(newStat.getMtime());
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
              if (currentData != null && mergeOnUpdate)
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

  public static void subtract(
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
              currentData.subtract(recordTosubtract);
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
