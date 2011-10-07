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
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.tools.ZnodeModDesc.ZnodePropertyType;
import com.linkedin.clustermanager.util.ZKClientPool;

public class ZnodeModExecutor
{
  private enum ZnodeModValueType
  {
    INVALID,
    SINGLE_VALUE,
    LIST_VALUE,
    MAP_VALUE,
    ZNODE_VALUE
  }
  
  private static Logger logger = Logger.getLogger(ZnodeModExecutor.class);
  private static final int MAX_PARALLEL_TASKS = 1;
  
  private final ZnodeModDesc _testDesc;
  private final ZkClient _zkClient;
  
  private final Map<String, Boolean> _testResults = new ConcurrentHashMap<String, Boolean>();
  private final CountDownLatch _verifierCountDown;
  private final CountDownLatch _commandCountDown;
  private final ScheduledExecutorService _executor = Executors.newScheduledThreadPool(MAX_PARALLEL_TASKS);
  private final ScheduledExecutorService _timeoutExecutor = Executors.newScheduledThreadPool(MAX_PARALLEL_TASKS);
  
  private final static PropertyJsonComparator<String> STRING_COMPARATOR 
            = new PropertyJsonComparator<String>(String.class);
  // private final static PropertyJsonComparator<ZNRecord> ZNODE_COMPARATOR 
  //          = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);

  
  private class CommandDataListener implements IZkDataListener
  {
    private final ZnodeModCommand _command;
    private Future _task = null;
    private Future _timeoutTask = null;

    public CommandDataListener(ZnodeModCommand command)
    {
      _command = command;
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
      if (compareAndSet(_command) == true)
      {
        _zkClient.unsubscribeDataChanges(dataPath, this);
        if (_task != null)
        {
          _task.cancel(true);
        }
        if (_timeoutTask != null)
        {
          _timeoutTask.cancel(true);
        }
        _commandCountDown.countDown();
      }
    }
    
    @Override
    public void handleDataDeleted(String dataPath) throws Exception
    {
      // TODO Auto-generated method stub
      
    }
        
  };
  
  private class CommandExecutor implements Runnable
  {
    private final ZnodeModCommand _command;
    private final CommandDataListener _listener;
    private final String _path;
    private final ZnodeModValue _expect;
    private final long _timeout;
    private Future _timeoutTask = null;
    
    
    public CommandExecutor(ZnodeModCommand command)
    {
      this(command, null);
    }
    
    
    public CommandExecutor(ZnodeModCommand command, CommandDataListener listener)
    {
      _command = command;
      _listener = listener;
      _path = command.getZnodePath();
      _expect = command.getTrigger().getExpectValue();
      _timeout = command.getTrigger().getTimeout();
    }
    
    public void setTimeoutTask(Future timeoutTask)
    {
      _timeoutTask = timeoutTask;
    }
    
    @Override
    public void run()
    {
      boolean result = compareAndSet(_command);
      if (result == true)
      {
        if (_expect != null && _listener != null)
        {
          _zkClient.unsubscribeDataChanges(_path, _listener);
        }
        if (_timeout > 0 && _timeoutTask != null)
        {
          _timeoutTask.cancel(true);
        }
        _commandCountDown.countDown();
      }
      else
      {
        if (_timeout == 0)
        {
          logger.warn("fail to execute command (timeout=0):" + _command.toString());
          _commandCountDown.countDown();
        }
      }
    }
  }
  
  private class CommandTimeoutTask implements Runnable
  {
    private final ZnodeModCommand _command;
    private final CommandDataListener _listener;
    private final String _path;
    private final Future _task;
    
    public CommandTimeoutTask(ZnodeModCommand command, CommandDataListener listener, Future task)
    {
      _command = command;
      _listener = listener;
      _path = command.getZnodePath();
      _task = task;
    }
    
    @Override
    public void run()
    {
      _zkClient.unsubscribeDataChanges(_path, _listener);
      _task.cancel(true);
      _commandCountDown.countDown();
      
      logger.warn("fail to execute command (timeout):" + _command);
      
    }
    
  };
 
  private class VerifierDataListener implements IZkDataListener
  {
    private Future _timeoutTask = null;
    private Future _task = null;
    private final ZnodeModVerifier _verifier;

    public VerifierDataListener(ZnodeModVerifier verifier)
    {
      _verifier = verifier;
    }
    
    public void setTimeoutTask(Future timeoutTask)
    {
      _timeoutTask = timeoutTask;
    }
    
    public void setTask(Future task)
    {
      _task = task;
    }
    
    public ZnodeModVerifier getVerifier()
    {
      return _verifier;
    }
    
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception
    {
      ZNRecord record = (ZNRecord)data;
      boolean result = executeVerifier(record, _verifier);
      if (result == true)
      {
        _testResults.put(_verifier.toString(), new Boolean(true));
        _zkClient.unsubscribeDataChanges(dataPath, this);
        if (_task != null)
        {
          _task.cancel(true);
        }
        
        if (_timeoutTask != null)
        {
          _timeoutTask.cancel(true);
        }
        _verifierCountDown.countDown();
      }
    }
    
    @Override
    public void handleDataDeleted(String dataPath) throws Exception
    {
      // TODO Auto-generated method stub
      
    }
        
  };
  
  private class VerifierExecutor implements Runnable
  {
    private final VerifierDataListener _listener;
    private final ZnodeModVerifier _verifier;
    private final String _path;
    private final long _timeout;
    private Future _timeoutTask = null;
    
    public VerifierExecutor(ZnodeModVerifier verifier)
    {
      this(null, verifier);
    }
    
    public VerifierExecutor(VerifierDataListener listener, ZnodeModVerifier verifier)
    {
      _listener = listener;
      _verifier = verifier;
      _path = verifier.getZnodePath();
      _timeout = verifier.getTimeout();
    }
    
    public void setTimeoutTask(Future timeoutTask)
    {
      _timeoutTask = timeoutTask;
    }

    @Override
    public void run()
    {
      ZNRecord record = _zkClient.<ZNRecord>readData(_path, true);

      boolean result = executeVerifier(record, _verifier);
      if (result == true)
      {
        if (_listener != null)
        {
          _zkClient.unsubscribeDataChanges(_path, _listener);
        }
        
        if (_timeout > 0 && _timeoutTask != null)
        {
          _timeoutTask.cancel(true);
        }

        _verifierCountDown.countDown();
      }
      else
      {
        if (_timeout == 0)
        {
          logger.warn("fail to verify (timeout=0):" + _verifier.toString());
          _verifierCountDown.countDown();
        }
      }
    }
    
  }
  
  private class VerifierTimeoutTask implements Runnable // extends TimerTask
  {
    private final VerifierDataListener _listener;
    private final ZnodeModVerifier _verifier;
    private final String _path;
    private final Future _task;
    
    public VerifierTimeoutTask(VerifierDataListener listener, ZnodeModVerifier verifier, Future task)
    {
      _listener = listener;
      _verifier = verifier;
      _path = verifier.getZnodePath();
      _task = task;
    }
    
    @Override
    public void run()
    {
      _zkClient.unsubscribeDataChanges(_path, _listener);
      _task.cancel(true);
      _verifierCountDown.countDown();
      
      logger.warn("fail to verifier (timeout):" + _verifier);
    }
    
  };
  
  public ZnodeModExecutor(ZnodeModDesc testDesc, String zkAddr)
  {
    _testDesc = testDesc;
    _zkClient = ZKClientPool.getZkClient(zkAddr);
    _verifierCountDown = new CountDownLatch(testDesc.getVerifiers().size());
    _commandCountDown = new CountDownLatch(testDesc.getCommands().size());
  }
  
  private ZnodeModValueType getValueType(ZnodePropertyType type, String key)
  {
    ZnodeModValueType valueType = ZnodeModValueType.INVALID;
    switch(type)
    {
    case SIMPLE:
      String keyParts[] = key.split("/");
      if (keyParts.length != 1)
      {
        logger.warn("invalid key for simple field: " + key + ", expect 1 part: key1 (no slash)");
      }
      valueType = ZnodeModValueType.SINGLE_VALUE;
      break;
    case LIST:
      keyParts = key.split("/");
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
      break;
    case MAP:
      keyParts = key.split("/");
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
      break;
    case ZNODE:
      valueType = ZnodeModValueType.ZNODE_VALUE;
    default:
      break;
    }
    return valueType;
  }
  
  private String getSingleValue(ZNRecord record, ZnodePropertyType type, String key)
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
  
  private List<String> getListValue(ZNRecord record, String key)
  {
    return record.getListField(key);
  }
  
  private Map<String, String> getMapValue(ZNRecord record, String key)
  {
    return record.getMapField(key);
  }
  
  // comparator's for single/list/map-value
  private boolean compareSingleValue(String value, String expect)
  {
    return STRING_COMPARATOR.compare(value, expect) == 0;
  }
  
  private boolean compareListValue(List<String> valueList, List<String> expectList)
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
  
  private boolean compareMapValue(Map<String, String> valueMap, Map<String, String> expectMap)
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
  
  private boolean compareZnodeValue(ZNRecord value, ZNRecord expect)
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
      if (!compareMapValue(value.getSimpleFields(), expect.getSimpleFields()))
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
  
  private boolean compareValue(ZNRecord record, ZnodePropertyType type, 
                               String key, ZnodeModValue expect)
  {
    if (expect == null)
    {
      return true;
    }
    
    boolean result = false;
    ZnodeModValueType valueType = getValueType(type, key);
    switch(valueType)
    {
    case SINGLE_VALUE:
      String singleValue = getSingleValue(record, type, key);
      result = compareSingleValue(singleValue, expect.getSingleValue());
      break;
    case LIST_VALUE:
      List<String> listValue = getListValue(record, key);
      result = compareListValue(listValue, expect.getListValue());
      break;
    case MAP_VALUE:
      Map<String, String> mapValue = getMapValue(record, key);
      result = compareMapValue(mapValue, expect.getMapValue());
      break;
    case ZNODE_VALUE:
      result = compareZnodeValue(record, expect.getZnodeValue()); 
      // (ZNODE_COMPARATOR.compare(expect.getZnodeValue(), record) == 0);
      break;
    case INVALID:
      break;
    default:
      break;
    }
    return result;
  }
  
  private void setSingleValue(ZNRecord record, ZnodePropertyType type, String key, String value)
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
  
  private void setListValue(ZNRecord record, String key, List<String> value)
  {
    record.setListField(key, value);
  }
  
  private void setMapValue(ZNRecord record, String key, Map<String, String> value)
  {
    record.setMapField(key, value);
  }
  
  private void removeSingleValue(ZNRecord record, ZnodePropertyType type, String key)
  {
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
  
  private void removeListValue(ZNRecord record, String key)
  {
    record.getListFields().remove(key);
  }
  
  private void removeMapValue(ZNRecord record, String key)
  {
    record.getMapFields().remove(key);
  }
  
  private boolean executeVerifier(ZNRecord record, ZnodeModVerifier verifier)
  {
    // TODO change compare to adopt composite key
    boolean result = compareValue(record, verifier.getPropertyType(), 
                                  verifier.getKey(), verifier.getValue());
    String operation = verifier.getOperation(); 
    if (operation.equals("!="))
    {
      result = !result;
    }
    else if (!operation.equals("=="))
    {
      logger.warn("fail to verify (unsupport operation=" + operation + "):" + verifier);
      result = false;
    }
    
    if (result == true)
    {
      logger.info("verifier result:" + result + ", verifier:" + verifier.toString());
      _testResults.put(verifier.toString(), new Boolean(true));
    }
    return result;
  }
  
  private ZNRecord executeCommand(ZNRecord record, ZnodeModCommand command)
  {
    boolean success = true;
    if (command.getOperation().compareTo("+") == 0)
    {
      if (record == null)
      {
        record = new ZNRecord("command");
      }
      ZnodeModValueType valueType = getValueType(command.getPropertyType(), command.getKey());
      switch(valueType)
      {
      case SINGLE_VALUE:
        setSingleValue(record, command.getPropertyType(), command.getKey(), 
                       command.getUpdateValue().getSingleValue());   
        break;
      case LIST_VALUE:
        setListValue(record, command.getKey(), command.getUpdateValue().getListValue());
        break;
      case MAP_VALUE:
        setMapValue(record, command.getKey(), command.getUpdateValue().getMapValue());
        break;
      case ZNODE_VALUE:
        record = command.getUpdateValue().getZnodeValue();
        break;
      case INVALID:
        break;
      default:
        break;
      }
      
    }
    else if (command.getOperation().compareTo("-") == 0)
    {
      ZnodeModValueType valueType = getValueType(command.getPropertyType(), command.getKey());
      switch(valueType)
      {
      case SINGLE_VALUE:
        removeSingleValue(record, command.getPropertyType(), command.getKey());
        break;
      case LIST_VALUE:
        removeListValue(record, command.getKey());
        break;
      case MAP_VALUE:
        removeMapValue(record, command.getKey());
        break;
      case ZNODE_VALUE:
        // record = null;
        break;
      case INVALID:
        break;
      default:
        break;
      }

    }
    else
    {
      logger.warn("fail to execute command(unsupport operation):" + command);
      success = false;
    }
    
    if (success == true)
    {
      logger.info("execute command result:" + success + ", command:" + command.toString());
      _testResults.put(command.toString(), new Boolean(true));
    }
    return record;
  }
  
  private boolean compareAndSet(ZnodeModCommand command)
  {
    String path = command.getZnodePath();
    ZnodePropertyType type = command.getPropertyType();
    String key = command.getKey();
    ZnodeModValue expect = command.getTrigger().getExpectValue();
    
    if (expect != null && !_zkClient.exists(path))
    {
      return false;
    }
    
    boolean isSucceed = false;
    try
    {
      if (!_zkClient.exists(path))
      {
        _zkClient.createPersistent(path, true);
      }
    }
    catch (ZkNodeExistsException e)
    {
      // OK
    }
      
    try
    {
      Stat stat = new Stat();
      ZNRecord current = _zkClient.<ZNRecord>readData(path, stat);

      if (compareValue(current, type, key, expect))
      {
        ZNRecord update = executeCommand(current, command);
        if (update == null)
        {
         _zkClient.delete(path); 
        }
        else
        {
          _zkClient.writeData(path, update, stat.getVersion());
        }
        isSucceed = true;
      }
    } catch (ZkBadVersionException e)
    {
      // isSucceed = false;
    }

    return isSucceed;
  }
  
  public Map<String, Boolean> executeTest() 
  throws InterruptedException
  {
    // sort on trigger's start time
    List<ZnodeModCommand> commandList = _testDesc.getCommands();
    Collections.sort(commandList, new Comparator<ZnodeModCommand>() {  // stable sort
      @Override
      public int compare(ZnodeModCommand o1, ZnodeModCommand o2) {
        return (int) (o1.getTrigger().getStartTime() - o2.getTrigger().getStartTime());
      }
    });
    
    for (ZnodeModCommand command : commandList)
    {
      _testResults.put(command.toString(), new Boolean(false));
      
      ZnodeModTrigger trigger = command.getTrigger();
      String path = command.getZnodePath();
      if (trigger.getExpectValue() == null
          || (trigger.getExpectValue() != null && trigger.getTimeout() == 0) )
      {
        _executor.schedule(new CommandExecutor(command), command.getTrigger().getStartTime(), 
                              TimeUnit.MILLISECONDS);
        // no timeout task for command without data-trigger
      }
      else if (trigger.getExpectValue() != null && trigger.getTimeout() > 0)
      {
        // set watcher before test the value so we can't miss any data change
        CommandDataListener listener = new CommandDataListener(command);
        _zkClient.subscribeDataChanges(path, listener);
        
        // TODO possible that listener callback happens and succeeds before task is scheduled
        CommandExecutor executor = new CommandExecutor(command, listener);
        long startTime = command.getTrigger().getStartTime();
        Future task = _executor.schedule(executor, startTime, TimeUnit.MILLISECONDS);
        listener.setTask(task);
        
        Future timeoutTask = _timeoutExecutor.schedule(new CommandTimeoutTask(command, listener, task), 
                        startTime + command.getTrigger().getTimeout(), TimeUnit.MILLISECONDS);
        listener.setTimeoutTask(timeoutTask);
        executor.setTimeoutTask(timeoutTask);
      }
    }
    
    _commandCountDown.await();
    
    // execute verifiers
    // sort on verifier's timeout
    List<ZnodeModVerifier> verifierList = _testDesc.getVerifiers();
    Collections.sort(verifierList, new Comparator<ZnodeModVerifier>() {  // stable sort
      @Override
      public int compare(ZnodeModVerifier o1, ZnodeModVerifier o2) {
        return (int) (o1.getTimeout() - o2.getTimeout());
      }
    });
    
    for (ZnodeModVerifier verifier : verifierList)
    {
      _testResults.put(verifier.toString(), new Boolean(false));
      String path = verifier.getZnodePath();

      if (verifier.getTimeout() > 0)
      {
        // set watcher before checking the value so we can't miss any data change
        VerifierDataListener listener = new VerifierDataListener(verifier);
        _zkClient.subscribeDataChanges(path, listener);
        VerifierExecutor executor = new VerifierExecutor(listener, verifier);
        
        // TODO possible that listener callback happens and succeeds before task is scheduled
        Future task = _executor.schedule(executor, 0, TimeUnit.MILLISECONDS);
        listener.setTask(task);
        
        Future timeoutTask = _timeoutExecutor.schedule(new VerifierTimeoutTask(listener, verifier, task), 
                                  verifier.getTimeout(), TimeUnit.MILLISECONDS);
        listener.setTimeoutTask(timeoutTask);
        executor.setTimeoutTask(timeoutTask);
      }
      else
      {
        Future task = _executor.schedule(new VerifierExecutor(verifier), 0, TimeUnit.MILLISECONDS);
      }
    }
    _verifierCountDown.await();
    return _testResults;
  }
}
