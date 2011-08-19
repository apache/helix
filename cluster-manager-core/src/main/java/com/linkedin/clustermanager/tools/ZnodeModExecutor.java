package com.linkedin.clustermanager.tools;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;

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
    MAP_VALUE
  }
  
  private static Logger LOG = Logger.getLogger(ZnodeModExecutor.class);
  private final ZnodeModDesc _testDesc;
  private final ZkClient _zkClient;
  
  private final Timer _timer = new Timer();
  private final Map<String, Boolean> _testResults = new ConcurrentHashMap<String, Boolean>();
  private final CountDownLatch _verifierCountDown;
  private final CountDownLatch _commandCountDown;
  
  private final static PropertyJsonComparator<String> STRING_COMPARATOR = new PropertyJsonComparator<String>(String.class);

  
  private class CommandDataListener implements IZkDataListener
  {
    private CommandTimeoutTask _task = null;
    private final ZnodeModCommand _command;

    public CommandDataListener(ZnodeModCommand command)
    {
      _command = command;
    }
    
    public void setTimeoutTask(CommandTimeoutTask task)
    {
      _task = task;
    }
    
    public ZnodeModCommand getCommand()
    {
      return _command;
    }
    
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception
    {
      ZNRecord record = (ZNRecord)data;
      ZnodeModTrigger trigger = _command.getTrigger();
      if (compareValue(record, _command.getPropertyType(), 
                       _command.getKey(), trigger.getExpectValue()) == true)
      {
        record = executeCommand(record, _command);
       
        try
        {
          _zkClient.writeData(dataPath, record);
          _testResults.put(_command.toString(), new Boolean(true));
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }

        _zkClient.unsubscribeDataChanges(dataPath, this);
        if (_task != null)
        {
          _task.cancel();
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
  
  
  private class CommandTimeoutTask extends TimerTask
  {
    private final CommandDataListener _listener;
    
    public CommandTimeoutTask(CommandDataListener listener)
    {
      listener.setTimeoutTask(this);
      _listener = listener;
    }
    
    @Override
    public void run()
    {
      ZnodeModCommand command = _listener.getCommand();
      String path = command.getZnodePath();
      _zkClient.unsubscribeDataChanges(path, _listener);
      _testResults.put(command.toString(), new Boolean(false));
      _commandCountDown.countDown();
      
      LOG.warn(command + " fails (timeout)");
      
    }
    
  };
  
  
  private class VerifierDataListener implements IZkDataListener
  {
    private VerifierTimeoutTask _task = null;
    private final ZnodeModVerifier _verifier;

    public VerifierDataListener(ZnodeModVerifier verifier)
    {
      _verifier = verifier;
    }
    
    public void setTimeoutTask(VerifierTimeoutTask task)
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
          _task.cancel();
        }
        _testResults.put(_verifier.toString(), new Boolean(true));
        _verifierCountDown.countDown();
      }
    }
    
    @Override
    public void handleDataDeleted(String dataPath) throws Exception
    {
      // TODO Auto-generated method stub
      
    }
        
  };
  
  private class VerifierTimeoutTask extends TimerTask
  {
    private final VerifierDataListener _listener;
    
    public VerifierTimeoutTask(VerifierDataListener listener)
    {
      listener.setTimeoutTask(this);
      _listener = listener;
    }
    
    @Override
    public void run()
    {
      ZnodeModVerifier verifier = _listener.getVerifier();
      String path = verifier.getZnodePath();
      _zkClient.unsubscribeDataChanges(path, _listener);
      _testResults.put(verifier.toString(), new Boolean(false));
      _verifierCountDown.countDown();
      
      LOG.warn(verifier + " fails (timeout)");
      
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
    String keyParts[] = key.split("/");
    switch(type)
    {
    case SIMPLE:
      if (keyParts.length != 1)
      {
        LOG.warn("invalid key for simple field: " + key + ", expect 1 part: key1 (no slash)");
      }
      valueType = ZnodeModValueType.SINGLE_VALUE;
      break;
    case LIST:
      if (keyParts.length < 1 || keyParts.length > 2)
      {
        LOG.warn("invalid key for list field: " + key + ", expect 1 or 2 parts: key1 or key1/index)");
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
            LOG.warn("invalid key for list field: " + key + ", index < 0");
          }
          else
          {
            valueType = ZnodeModValueType.SINGLE_VALUE;
          }
        }
        catch (NumberFormatException e)
        {
          LOG.warn("invalid key for list field: " + key + ", part-2 is NOT an integer");
        }
      }
      break;
    case MAP:
      if (keyParts.length < 1 || keyParts.length > 2)
      {
        LOG.warn("invalid key for map field: " + key + ", expect 1 or 2 parts: key1 or key1/key2)");
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
        LOG.warn("invalid key for list field: " + key + ", map for key part-1 doesn't exist");
        return null;
      }
      int idx = Integer.parseInt(keyParts[1]);
      value = list.get(idx);
      break;
    case MAP:
      Map<String, String> map = record.getMapField(keyParts[0]);
      if (map == null)
      {
        LOG.warn("invalid key for map field: " + key + ", map for key part-1 doesn't exist");
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
  
  private boolean compareValue(ZNRecord record, ZnodePropertyType type, 
                               String key, ZnodeModValue expect)
  {
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
      Map<String, String> mapValue = getMapValue(record,key);
      result = compareMapValue(mapValue, expect.getMapValue());
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
        LOG.warn("invalid key for list field: " + key + ", value for key part-1 doesn't exist");
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
        LOG.warn("invalid key for map field: " + key + ", value for key part-1 doesn't exist");
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
        LOG.warn("invalid key for list field: " + key + ", value for key part-1 doesn't exist");
        return;
      }
      int idx = Integer.parseInt(keyParts[1]);
      list.remove(idx);
      break;
    case MAP:
      Map<String, String> map = record.getMapField(keyParts[0]);
      if (map == null)
      {
        LOG.warn("invalid key for map field: " + key + ", value for key part-1 doesn't exist");
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
    // String value = getValue(record, verifier.getPropertyType(), verifier.getKey());
    
    // TODO: change compare to adopt composite key
    boolean result = compareValue(record, verifier.getPropertyType(), 
                                  verifier.getKey(), verifier.getValue());
    if (verifier.getOperation().compareTo("==") == 0)
    {
      
    }
    else if (verifier.getOperation().compareTo("!=") == 0)
    {
      result = !result;
    }
    else
    {
      LOG.warn(verifier + " fails (unsupport operation)");
      result = false;
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
        record = new ZNRecord();
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
      case INVALID:
        break;
      default:
        break;
      }

    }
    else
    {
      LOG.warn(command + " fails (unsupport operation)");
      success = false;
    }
    
    _testResults.put(command.toString(), new Boolean(success));
    return record;
  }
  
  private void fireAllZnodeChanges(Map<String, ZNRecord> changes)
  {
    for (Map.Entry<String, ZNRecord> entry : changes.entrySet())
    {
      if (entry.getValue() != null)
      {
        try
        {
          _zkClient.writeData(entry.getKey(), entry.getValue());    
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    }
    
    changes.clear();
  }
  
  public Map<String, Boolean> executeTest() 
  throws InterruptedException
  {
    // sort on trigger's start time
    List<ZnodeModCommand> commandList = _testDesc.getCommands();
    Collections.sort(commandList, new Comparator<ZnodeModCommand>() {  // stable sort
      public int compare(ZnodeModCommand o1, ZnodeModCommand o2) {
        return (int) (o1.getTrigger().getStartTime() - o2.getTrigger().getStartTime());
      }
    });
    
    long lastTime = 0;
    final Map<String, ZNRecord> znodeChanges = new HashMap<String, ZNRecord>();
    final Map<String, ZNRecord> znodeCache = new HashMap<String, ZNRecord>();
    
    for (ZnodeModCommand command : commandList)
    {
      ZnodeModTrigger trigger = command.getTrigger();
      if (trigger.getStartTime() <= lastTime)
      {
        String path = command.getZnodePath();
        ZNRecord record = znodeCache.get(path);
        if (record == null)
        {
          try
          {
            record = _zkClient.<ZNRecord>readData(path);
          }
          catch (ZkNoNodeException e)
          {
            _zkClient.createPersistent(path);
          }
          znodeCache.put(path, record);
        }
        

        if (trigger.getExpectValue() == null) // not a data triggered command
        {
          record = executeCommand(record, command);
          znodeCache.put(path, record);
          znodeChanges.put(path, record);
          _commandCountDown.countDown();
        }
        else
        {
          if (trigger.getTimeout() > 0)
          {
            // set watcher before test the value so we can't miss any data change
            CommandDataListener listener = new CommandDataListener(command);
            CommandTimeoutTask task = new CommandTimeoutTask(listener);
            _timer.schedule(task, trigger.getTimeout());
            
            _zkClient.subscribeDataChanges(path, listener);

            if (compareValue(record, command.getPropertyType(), 
                             command.getKey(), trigger.getExpectValue()) == true)
            {
              _zkClient.unsubscribeDataChanges(path, listener);
              task.cancel();
                
              record = executeCommand(record, command);
              znodeCache.put(path, record);
              znodeChanges.put(path, record);
              _commandCountDown.countDown();
            }
          }
          else
          {
            if (compareValue(record, command.getPropertyType(), 
                             command.getKey(), trigger.getExpectValue()) == true)
            {
              record = executeCommand(record, command);
              znodeCache.put(path, record);
              znodeChanges.put(path, record);
              _commandCountDown.countDown();
            }
            else
            {
              LOG.warn(command + " fails (value not expected, no timeout)");
              _testResults.put(command.toString(), new Boolean(false));
              _commandCountDown.countDown();
            }
          }
        }
      }
      else  // trigger._startTime > lastTime
      {
        znodeCache.clear();
        fireAllZnodeChanges(znodeChanges);
        Thread.sleep(trigger.getStartTime() - lastTime);
        lastTime = trigger.getStartTime();
      }
    }
    
    fireAllZnodeChanges(znodeChanges);
    _commandCountDown.await();
    
    // execute verifiers
    // sort on verifier's timeout
    List<ZnodeModVerifier> verifierList = _testDesc.getVerifiers();
    Collections.sort(verifierList, new Comparator<ZnodeModVerifier>() {  // stable sort
      public int compare(ZnodeModVerifier o1, ZnodeModVerifier o2) {
        return (int) (o1.getTimeout() - o2.getTimeout());
      }
    });
    
    for (ZnodeModVerifier verifier : verifierList)
    {
      _testResults.put(verifier.toString(), new Boolean(false));
      String path = verifier.getZnodePath();
      ZNRecord record = _zkClient.<ZNRecord>readData(path, true);
      

      if (verifier.getTimeout() > 0)
      {
        // set watcher before checking the value so we can't miss any data change
        VerifierDataListener listener = new VerifierDataListener(verifier);
        VerifierTimeoutTask task = new VerifierTimeoutTask(listener);
        _timer.schedule(task, verifier.getTimeout());
        
        _zkClient.subscribeDataChanges(path, listener);
              
        boolean result = executeVerifier(record, verifier);
        if (result == true)
        {
          _testResults.put(verifier.toString(), new Boolean(true));
          _verifierCountDown.countDown();
          _zkClient.unsubscribeDataChanges(path, listener);
          task.cancel();
        }
      }
      else
      {
        boolean result = executeVerifier(record, verifier);
        _testResults.put(verifier.toString(), new Boolean(result));
        if (result == false)
        {
          LOG.warn(verifier + " fails (no timeout)");
        }
        _verifierCountDown.countDown();

      }
    }
    
    _verifierCountDown.await();
    return _testResults;
  }
}
