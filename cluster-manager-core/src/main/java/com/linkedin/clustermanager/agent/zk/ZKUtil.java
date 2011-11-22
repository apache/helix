package com.linkedin.clustermanager.agent.zk;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordAndStat;

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
            && zkClient.exists(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName))
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

  /**
   * read a znode only if it's data has been changed since last read
   * this method is not thread-safe
   * @param zkClient
   * @param parentPath
   * @param childRecords
   * @param clazz
   * @return
   */
  public static <T extends ZNRecordAndStat> boolean refreshChildren(ZkClient zkClient,
    String parentPath, Map<String, T> childRecords, Class<T> clazz)
  {
    if (childRecords == null)
    {
      throw new IllegalArgumentException("should provide non-null map that holds old child records "
                                      + " (empty map if no old values)");
    }

    Stat newStat = new Stat();
    List<String> childs = zkClient.getChildren(parentPath);
    if (childs == null || childs.size() == 0)
    {
      if (childRecords.size() == 0)
      {
        return false;
      }
      else
      {
        childRecords.clear();
        return true;
      }
    }

    boolean dataChanged = false;
    // first remove records that have been removed from zk
    Iterator<String> keyIter = childRecords.keySet().iterator();
    while (keyIter.hasNext())
    {
      String key = keyIter.next();
      if (!childs.contains(key))
      {
        keyIter.remove();
        dataChanged = true;
      }
    }

    // then update
    for (String child : childs)
    {
      String childPath = parentPath + "/" + child;

      try
      {
        // assume record.id should be the last part of zk path
        if (!childRecords.containsKey(child))
        {
          ZNRecord record = zkClient.readDataAndStat(childPath, newStat, true);
          if (record != null)
          {
            Constructor<T> constructor = clazz.getConstructor(new Class[] { ZNRecord.class, Stat.class });
            childRecords.put(child, constructor.newInstance(record, newStat));
          }
          else
          {
            childRecords.remove(child);
          }
          dataChanged = true;
        }
        else
        {
          T oldChild = childRecords.get(child);
          Stat oldStat = oldChild.getStat();
          newStat = zkClient.getStat(childPath);
          if (newStat == null)
          {
            childRecords.remove(child);
          }
          else
          {
//          System.out.print(child + " oldStat:" + oldStat);
//          System.out.print(child + " newStat:" + newStat);

            if (oldStat == null || !oldStat.equals(newStat))
            {
              ZNRecord record = zkClient.readDataAndStat(childPath, newStat, true);
              if (record != null)
              {
                Constructor<T> constructor = clazz.getConstructor(new Class[] { ZNRecord.class, Stat.class });
                childRecords.put(child, constructor.newInstance(record, newStat));
              }
              else
              {
                childRecords.remove(child);
              }
              dataChanged = true;
            }
            else  // if (newStat.equals(oldStat))
            {
              // no need to update record
            }
          }
        }
      }
      catch (Exception e)
      {
        logger.error("Error creating an Object of type:" + clazz.getCanonicalName(), e);
      }
    }

    return dataChanged;
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
