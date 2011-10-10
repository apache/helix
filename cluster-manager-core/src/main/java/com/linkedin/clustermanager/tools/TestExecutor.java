package com.linkedin.clustermanager.tools;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.tools.TestCommand.CommandType;

/**
 * a test is structured logically as a list of commands
 * a command has three parts: COMMAND | TRIGGER | ARG'S
 * COMMAND could be: modify, verify, start, stop
 * 
 * TRIGGER is optional and consists of start-time, timeout, and expect-value
 *   which means the COMMAND is triggered between [start-time, start-time + timeout]
 *   and is triggered when the value in concern equals to expect-value
 * 
 * ARG's format depends on COMMAND
 *   if COMMAND is modify/verify, arg is in form of:
 *     <znode-path, property-type (SIMPLE, LIST, or MAP), operation(+, -, ==, !=), key, update-value>
 *       in which key is k1 for SIMPLE, k1|index for LIST, and k1|k2 for MAP field
 *   if COMMAND is start/stop, arg is a thread handler
 *   
 * @author zzhang
 *
 */

public class TestExecutor
{
  /**
   * SIMPLE: simple field change
   * LIST: list field change
   * MAP: map field change
   * ZNODE: entire znode change
   */
  public enum ZnodePropertyType
  {
    SIMPLE,
    LIST,
    MAP,
    ZNODE
  }
  
  private enum ZnodeModValueType
  {
    INVALID,
    SINGLE_VALUE,
    LIST_VALUE,
    MAP_VALUE,
    ZNODE_VALUE
  }
  
  private static Logger logger = Logger.getLogger(TestExecutor.class);
  private static final int MAX_PARALLEL_TASKS = 1;
  
  private final static PropertyJsonComparator<String> STRING_COMPARATOR 
            = new PropertyJsonComparator<String>(String.class);
  private final static PropertyJsonSerializer<ZNRecord> ZNRECORD_SERIALIZER 
            = new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);

  private static ZnodeModValueType getValueType(ZnodePropertyType type, String key)
  {
    ZnodeModValueType valueType = ZnodeModValueType.INVALID;
    switch(type)
    {
    case SIMPLE:
      if (key == null)
      {
        logger.warn("invalid key for simple field: key is null");
      }
      else
      {
        String keyParts[] = key.split("/");
        if (keyParts.length != 1)
        {
          logger.warn("invalid key for simple field: " + key + ", expect 1 part: key1 (no slash)");
        }
        else 
        {
          valueType = ZnodeModValueType.SINGLE_VALUE;
        }
      }
      break;
    case LIST:
      if (key == null)
      {
        logger.warn("invalid key for simple field: key is null");
      }
      else
      {
        String keyParts[] = key.split("/");
        if (keyParts.length < 1 || keyParts.length > 2)
        {
          logger.warn("invalid key for list field: " + key + ", expect 1 or 2 parts: key1 or key1/index)");
        }
        else if (keyParts.length == 1)
        {
          valueType = ZnodeModValueType.LIST_VALUE;
        }
        else
        {
          try
          {
            int index = Integer.parseInt(keyParts[1]);
            if (index < 0)
            {
              logger.warn("invalid key for list field: " + key + ", index < 0");
            }
            else
            {
              valueType = ZnodeModValueType.SINGLE_VALUE;
            }
          }
          catch (NumberFormatException e)
          {
            logger.warn("invalid key for list field: " + key + ", part-2 is NOT an integer");
          }
        }
      }
      break;
    case MAP:
      if (key == null)
      {
        logger.warn("invalid key for simple field: key is null");
      }
      else
      {
        String keyParts[] = key.split("/");
        if (keyParts.length < 1 || keyParts.length > 2)
        {
          logger.warn("invalid key for map field: " + key + ", expect 1 or 2 parts: key1 or key1/key2)");
        }
        else if (keyParts.length == 1)
        {
          valueType = ZnodeModValueType.MAP_VALUE;
        }
        else
        {
          valueType = ZnodeModValueType.SINGLE_VALUE;
        }
      }
      break;
    case ZNODE:
      valueType = ZnodeModValueType.ZNODE_VALUE;
    default:
      break;
    }
    return valueType;
  }
  
  private static String getSingleValue(ZNRecord record, ZnodePropertyType type, String key)
  {
    if (record == null || key == null)
    {
      return null;
    }
    
    String value = null;
    String keyParts[] = key.split("/");
    
    switch(type)
    {
    case SIMPLE:
      value = record.getSimpleField(key);
      break;
    case LIST:
      List<String> list = record.getListField(keyParts[0]);
      if (list == null)
      {
        logger.warn("invalid key for list field: " + key + ", map for key part-1 doesn't exist");
        return null;
      }
      int idx = Integer.parseInt(keyParts[1]);
      value = list.get(idx);
      break;
    case MAP:
      Map<String, String> map = record.getMapField(keyParts[0]);
      if (map == null)
      {
        logger.warn("invalid key for map field: " + key + ", map for key part-1 doesn't exist");
        return null;
      }
      value = map.get(keyParts[1]);
      break;
    default:
      break;
    }
    
    return value;
  }
  
  private static List<String> getListValue(ZNRecord record, String key)
  {
    return record.getListField(key);
  }
  
  private static Map<String, String> getMapValue(ZNRecord record, String key)
  {
    return record.getMapField(key);
  }
  
  // comparator's for single/list/map-value
  private static boolean compareSingleValue(String value, String expect)
  {
    return STRING_COMPARATOR.compare(value, expect) == 0;
  }
  
  private static boolean compareListValue(List<String> valueList, List<String> expectList)
  {
    if (valueList == null && expectList == null)
    {
      return true;
    }
    else if (valueList == null && expectList != null)
    {
      return false;
    }
    else if (valueList != null && expectList == null)
    {
      return false;
    }
    else
    {
      if (valueList.size() != expectList.size())
      {
        return false;
      }
      
      Iterator<String> itr1 = valueList.iterator();
      Iterator<String> itr2 = expectList.iterator();
      while (itr1.hasNext() && itr2.hasNext()) {
          String value = itr1.next();
          String expect = itr2.next();
          
          if (STRING_COMPARATOR.compare(value, expect) != 0)
          {
            return false;
          }
      }
      return true;
    }
  }
  
  private static boolean compareMapValue(Map<String, String> valueMap, Map<String, String> expectMap)
  {
    if (valueMap == null && expectMap == null)
    {
      return true;
    }
    else if (valueMap == null && expectMap != null)
    {
      return false;
    }
    else if (valueMap != null && expectMap == null)
    {
      return false;
    }
    else
    {
      if (valueMap.size() != expectMap.size())
      {
        return false;
      }
      for (Map.Entry<String, String> entry : valueMap.entrySet())
      {
        String key = entry.getKey();
        String value = entry.getValue();
        if (!expectMap.containsKey(key))
        {
          return false;
        }
        String expect = expectMap.get(key);
        if (STRING_COMPARATOR.compare(value, expect) != 0)
        {
          return false;
        }
      }
      return true;
    }
  }
  
  private static boolean compareZnodeValue(ZNRecord value, ZNRecord expect)
  {
    if (value == null && expect == null)
    {
      return true;
    }
    else if (value == null && expect != null)
    {
      return false;
    }
    else if (value != null && expect == null)
    {
      return false;
    }
    else
    {
      if (!compareMapValue(value.simpleFields, expect.getSimpleFields()))
      {
        return false;
      }
      
      if (value.getMapFields().size() != expect.getMapFields().size())
      {
        return false;
      }
      for (Map.Entry<String, Map<String, String>> entry : value.getMapFields().entrySet())
      {
        String key = entry.getKey();
        Map<String, String> mapValue = entry.getValue();
        if (!expect.getMapFields().containsKey(key))
        {
          return false;
        }
        Map<String, String> mapExpect = expect.getMapFields().get(key);
        if (!compareMapValue(mapValue, mapExpect))
        {
          return false;
        }
      }
      return true;
    }    
  }
  
  private static boolean isValueExpected(ZNRecord current, ZnodePropertyType type, 
                               String key, ZnodeValue expect)
  {
    // expect value = null means not expect any value
    if (expect == null)
    {
      return true;
    }
    
    boolean result = false;
    ZnodeModValueType valueType = getValueType(type, key);
    switch(valueType)
    {
    case SINGLE_VALUE:
      String singleValue = getSingleValue(current, type, key);
      result = compareSingleValue(singleValue, expect._singleValue);
      break;
    case LIST_VALUE:
      List<String> listValue = getListValue(current, key);
      result = compareListValue(listValue, expect._listValue);
      break;
    case MAP_VALUE:
      Map<String, String> mapValue = getMapValue(current, key);
      result = compareMapValue(mapValue, expect._mapValue);
      break;
    case ZNODE_VALUE:
      result = compareZnodeValue(current, expect._znodeValue); 
      break;
    case INVALID:
      break;
    default:
      break;
    }
    return result;
  }
  
  private static void setSingleValue(ZNRecord record, ZnodePropertyType type, String key, String value)
  {
    String keyParts[] = key.split("/");
    
    switch(type)
    {
    case SIMPLE:
      record.setSimpleField(key, value);
      break;
    case LIST:
      List<String> list = record.getListField(keyParts[0]);
      if (list == null)
      {
        logger.warn("invalid key for list field: " + key + ", value for key part-1 doesn't exist");
        return;
      }
      int idx = Integer.parseInt(keyParts[1]);
      list.remove(idx);
      list.add(idx, value);
      break;
    case MAP:
      Map<String, String> map = record.getMapField(keyParts[0]);
      if (map == null)
      {
        logger.warn("invalid key for map field: " + key + ", value for key part-1 doesn't exist");
        return;
      }
      map.put(keyParts[1], value);
      break;
    default:
      break;
    }
  }
  
  private static void setListValue(ZNRecord record, String key, List<String> value)
  {
    record.setListField(key, value);
  }
  
  private static void setMapValue(ZNRecord record, String key, Map<String, String> value)
  {
    record.setMapField(key, value);
  }
  
  private static void removeSingleValue(ZNRecord record, ZnodePropertyType type, String key)
  {
    if (record == null)
    {
      return;
    }
    
    String keyParts[] = key.split("/");
    switch(type)
    {
    case SIMPLE:
      record.getSimpleFields().remove(key);
      break;
    case LIST:
      List<String> list = record.getListField(keyParts[0]);
      if (list == null)
      {
        logger.warn("invalid key for list field: " + key + ", value for key part-1 doesn't exist");
        return;
      }
      int idx = Integer.parseInt(keyParts[1]);
      list.remove(idx);
      break;
    case MAP:
      Map<String, String> map = record.getMapField(keyParts[0]);
      if (map == null)
      {
        logger.warn("invalid key for map field: " + key + ", value for key part-1 doesn't exist");
        return;
      }
      map.remove(keyParts[1]);
      break;
    default:
      break;
    }
  }
  
  private static void removeListValue(ZNRecord record, String key)
  {
    if (record == null || record.getListFields() == null)
    {
      record.getListFields().remove(key);
    }
  }
  
  private static void removeMapValue(ZNRecord record, String key)
  {
    record.getMapFields().remove(key);
  }
  
  private static boolean executeVerifier(ZNRecord record, TestCommand command)
  {
    final ZnodeOpArg arg = command._znodeOpArg;
    final ZnodeValue expectValue = command._trigger._expectValue;

    boolean result = isValueExpected(record, arg._propertyType, arg._key, expectValue);
    String operation = arg._operation; 
    if (operation.equals("!="))
    {
      result = !result;
    }
    else if (!operation.equals("=="))
    {
      logger.warn("fail to execute (unsupport operation=" + operation + "):" + operation);
      result = false;
    }
    
    return result;
  }
  
  private static boolean compareAndSetZnode(ZnodeValue expect, ZnodeOpArg arg, ZkClient zkClient) 
  {
    String path = arg._znodePath;
    ZnodePropertyType type = arg._propertyType;
    String key = arg._key;
    boolean success = true;
    
    if (!zkClient.exists(path))
    {
      if (expect != null)
      {
        return false;
      }
    
      try
      {
        zkClient.createPersistent(path, true);
      }
      catch (ZkNodeExistsException e)
      {
        // OK
      }
    }
      
    try
    {
      Stat stat = new Stat();
      ZNRecord record = zkClient.<ZNRecord>readData(path, stat);

      if (isValueExpected(record, type, key, expect))
      {
        if (arg._operation.compareTo("+") == 0)
        {
          if (record == null)
          {
            record = new ZNRecord();
          }
          ZnodeModValueType valueType = getValueType(arg._propertyType, arg._key);
          switch(valueType)
          {
          case SINGLE_VALUE:
            setSingleValue(record, arg._propertyType, arg._key, arg._updateValue._singleValue);   
            break;
          case LIST_VALUE:
            setListValue(record, arg._key, arg._updateValue._listValue);
            break;
          case MAP_VALUE:
            setMapValue(record, arg._key, arg._updateValue._mapValue);
            break;
          case ZNODE_VALUE:
            // deep copy
            record = ZNRECORD_SERIALIZER.deserialize(ZNRECORD_SERIALIZER.serialize(arg._updateValue._znodeValue));
            break;
          case INVALID:
            break;
          default:
            break;
          }
        }
        else if (arg._operation.compareTo("-") == 0)
        {
          ZnodeModValueType valueType = getValueType(arg._propertyType, arg._key);
          switch(valueType)
          {
          case SINGLE_VALUE:
            removeSingleValue(record, arg._propertyType, arg._key);
            break;
          case LIST_VALUE:
            removeListValue(record, arg._key);
            break;
          case MAP_VALUE:
            removeMapValue(record, arg._key);
            break;
          case ZNODE_VALUE:
            record = null;
            // zkClient.delete(path);
            break;
          case INVALID:
            break;
          default:
            break;
          }
        }
        else
        {
          logger.warn("fail to execute (unsupport operation): " + arg._operation);
          success = false;
        }
        
        if (success == true)
        {
          if (record == null)
          {
            zkClient.delete(path); 
          }
          else
          {
            zkClient.writeData(path, record, stat.getVersion());
          }
          return true;
        }
        else
        {
          return false;
        }
      }
    } catch (ZkBadVersionException e)
    {
      // return false;
    }
    catch (PropertyStoreException e)
    {
      // return false;
    }

    return false;
  }

  private static class DataChangeListener implements IZkDataListener
  {
    private final TestCommand _command;
    private Future _task = null;
    private Future _timeoutTask = null;
    private final ZkClient _zkClient;
    private final CountDownLatch _countDown;
    private final Map<String, Boolean> _testResults;

    public DataChangeListener(TestCommand command, CountDownLatch countDown, ZkClient zkClient, 
                              Map<String, Boolean> testResults)
    {
      _command = command;
      _zkClient = zkClient;
      _countDown = countDown;
      _testResults = testResults;
    }
    
    public void setTask(Future task)
    {
      _task = task;
    }
    
    public void setTimeoutTask(Future timeoutTask)
    {
      _timeoutTask = timeoutTask;
    }
    
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception
    {
      if (_command._commandType == CommandType.MODIFY)
      {
        ZnodeOpArg arg = _command._znodeOpArg;
        // final String znodePath = arg.getZnodePath();
        final ZnodeValue expectValue = _command._trigger._expectValue;
        // final long dataTriggerTimeout = _command._trigger.getTimeout();
        
        if (compareAndSetZnode(expectValue, arg, _zkClient) == true)
        {
          logger.info("result:" + true + ", " + _command.toString());
          _testResults.put(_command.toString(), true);
          
          _zkClient.unsubscribeDataChanges(dataPath, this);
          if (_task != null)
          {
            _task.cancel(true);
          }
          
          if (_timeoutTask != null)
          {
            _timeoutTask.cancel(true);
          }
          _countDown.countDown();
        }
      }
      else if (_command._commandType == CommandType.VERIFY)
      {
        ZNRecord record = (ZNRecord)data;
        boolean result = executeVerifier(record, _command);
        if (result == true)
        {
          logger.info("result:" + true + ", " + _command.toString());
          _testResults.put(_command.toString(), true);
          
          _zkClient.unsubscribeDataChanges(dataPath, this);
          if (_task != null)
          {
            _task.cancel(true);
          }
          
          if (_timeoutTask != null)
          {
            _timeoutTask.cancel(true);
          }
          
          _countDown.countDown();
        }
      }
      else  // if (_command._commandType == CommandType.START)
      {
        throw new UnsupportedOperationException("command type:" + _command._commandType 
                                                + " for data trigger not supported");
      }
        
    }
    
    @Override
    public void handleDataDeleted(String dataPath) throws Exception
    {
      // TODO Auto-generated method stub
      
    }
        
  };
  
  private static class ExecuteCommand implements Runnable
  {
    private final TestCommand _command;
    private final DataChangeListener _listener;
    private Future _timeoutTask = null;
    private final ZkClient _zkClient;
    private final CountDownLatch _countDown;
    private final Map<String, Boolean> _testResults;
    
    public ExecuteCommand(TestCommand command, CountDownLatch countDown, ZkClient zkClient,
                          Map<String, Boolean> testResults)
    {
      this(command, countDown, zkClient, testResults, null);
    }
    
    public ExecuteCommand(TestCommand command, CountDownLatch countDown, ZkClient zkClient, 
                          Map<String, Boolean> testResults, DataChangeListener listener)
    {
      _command = command;
      _countDown = countDown;
      _listener = listener;
      _zkClient = zkClient;
      _testResults = testResults;
    }
    
    public void setTimeoutTask(Future timeoutTask)
    {
      _timeoutTask = timeoutTask;
    }
    
    @Override
    public void run()
    {
      boolean result = false;
      
      if (_command._commandType == CommandType.MODIFY)
      {
        ZnodeOpArg arg = _command._znodeOpArg;
        final String znodePath = arg._znodePath;
        final ZnodeValue expectValue = _command._trigger._expectValue;
        final long dataTriggerTimeout = _command._trigger._timeout;
        
        result = compareAndSetZnode(expectValue, arg, _zkClient);
        if (result == true)
        {
          logger.info("result:" + result + ", " + _command.toString());
          _testResults.put(_command.toString(), true);
          
          if (expectValue != null && _listener != null)
          {
            _zkClient.unsubscribeDataChanges(znodePath, _listener);
          }
          if (dataTriggerTimeout > 0 && _timeoutTask != null)
          {
            _timeoutTask.cancel(true);
          }
          _countDown.countDown();
        }
        else
        {
          if (dataTriggerTimeout == 0)
          {
            logger.warn("fail to execute command (timeout=0):" + _command.toString());
            _countDown.countDown();
          }
        }
      }
      else if (_command._commandType == CommandType.VERIFY)
      {
        ZnodeOpArg arg = _command._znodeOpArg;
        final String znodePath = arg._znodePath;
        // final ZnodeModValue expectValue = _command._trigger.getExpectValue();
        final long dataTriggerTimeout = _command._trigger._timeout;
        
        ZNRecord record = _zkClient.<ZNRecord>readData(znodePath, true);

        result = executeVerifier(record, _command);
        if (result == true)
        {
          logger.info("result:" + result + ", " + _command.toString());
          _testResults.put(_command.toString(), true);
          if (_listener != null)
          {
            _zkClient.unsubscribeDataChanges(znodePath, _listener);
          }
          
          if (dataTriggerTimeout > 0 && _timeoutTask != null)
          {
            _timeoutTask.cancel(true);
          }

          _countDown.countDown();
        }
        else
        {
          if (dataTriggerTimeout == 0)
          {
            logger.warn("fail to verify (timeout=0):" + _command.toString());
            _countDown.countDown();
          }
        }
      }
      else if (_command._commandType == CommandType.START)
      {
        // TODO add data trigger for START command
        Thread arg = _command._threadArg;
        arg.start();
        
        logger.info("result:" + result + ", " + _command.toString());
        _testResults.put(_command.toString(), true);
        _countDown.countDown();
      }
      else if (_command._commandType == CommandType.STOP)
      {
        // TODO add data trigger for STOP command
        Thread arg = _command._threadArg;
        arg.interrupt();
        
        logger.info("result:" + result + ", " + _command.toString());
        _testResults.put(_command.toString(), true);
        _countDown.countDown();
      }
      else
      {
        logger.error("unsupport command");
      }
    }
  }
  
  private static class CancelExecuteCommand implements Runnable
  {
    private final TestCommand _command;
    private final DataChangeListener _listener;
    private final Future _task;
    private final ZkClient _zkClient;
    private final CountDownLatch _countDown;
    // private final Map<String, Boolean> _testResult;
    
    public CancelExecuteCommand(TestCommand command, CountDownLatch countDown, 
                                DataChangeListener listener, Future task, ZkClient zkClient)
    {
      _command = command;
      _countDown = countDown;
      _listener = listener;
      _task = task;
      _zkClient = zkClient;
    }
    
    @Override
    public void run()
    {
      String znodePath = _command._znodeOpArg._znodePath;
      _zkClient.unsubscribeDataChanges(znodePath, _listener);
      _task.cancel(true);
      _countDown.countDown();
      
      logger.warn("fail to execute command (timeout):" + _command.toString());
    }
  };
 
  public static Map<String, Boolean> executeTest(List<TestCommand> commandList, String zkAddr) 
      throws InterruptedException
  {
    final Map<String, Boolean> testResults = new ConcurrentHashMap<String, Boolean>();
    final ScheduledExecutorService executor = Executors.newScheduledThreadPool(MAX_PARALLEL_TASKS);
    final ScheduledExecutorService timeoutExecutor = Executors.newScheduledThreadPool(MAX_PARALLEL_TASKS);
    final CountDownLatch countDown = new CountDownLatch(commandList.size());
    
    final ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    
    // sort on trigger's start time, stable sort
    Collections.sort(commandList, new Comparator<TestCommand>() {
      @Override
      public int compare(TestCommand o1, TestCommand o2) 
      {
        return (int) (o1._trigger._startTime - o2._trigger._startTime);
      }
    });

    for (TestCommand command : commandList)
    {
      testResults.put(command.toString(), new Boolean(false));
      
      TestTrigger trigger = command._trigger;
      if (trigger._expectValue == null
          || (trigger._expectValue != null && trigger._timeout == 0) )
      {
        executor.schedule(new ExecuteCommand(command, countDown, zkClient, testResults), 
                          trigger._startTime, TimeUnit.MILLISECONDS);
        // no timeout task for command without data-trigger
      }
      else if (trigger._expectValue != null && trigger._timeout > 0)
      {
        // set watcher before test the value so we can't miss any data change
        DataChangeListener listener = new DataChangeListener(command, countDown, zkClient, testResults);
        String znodePath = command._znodeOpArg._znodePath;
        zkClient.subscribeDataChanges(znodePath, listener);
        
        // TODO possible that listener callback happens and succeeds before task is scheduled
        ExecuteCommand cmdExecutor = new ExecuteCommand(command, countDown, zkClient, testResults, listener);
        Future task = executor.schedule(cmdExecutor, trigger._startTime, TimeUnit.MILLISECONDS);
        listener.setTask(task);
        
        Future timeoutTask = timeoutExecutor
            .schedule(new CancelExecuteCommand(command, countDown, listener, task, zkClient), 
                  trigger._startTime + trigger._timeout, TimeUnit.MILLISECONDS);
        listener.setTimeoutTask(timeoutTask);
        cmdExecutor.setTimeoutTask(timeoutTask);
      }
    }
    
    // TODO add timeout
    countDown.await();
    
    return testResults;
  }
  
  
}
