package com.linkedin.clustermanager.tools;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
      String value = getValue(record, _command._propertyType, _command._key);
      ZnodeModTrigger trigger = _command._trigger;
      if (trigger._expectValue == null || trigger._expectValue.compareTo(value) == 0)
      {
        record = executeCommand(record, _command);
        if (record == null)
        {
          _zkClient.delete(dataPath);
          _testResults.put(_command.toString(), new Boolean(true));
        }
        else
        {
          try
          {
            _zkClient.writeData(dataPath, record);
            _testResults.put(_command.toString(), new Boolean(true));
          }
          catch (Exception e)
          {
            e.printStackTrace();
          }
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
      String path = command._znodePath;
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
      String path = verifier._znodePath;
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
    _verifierCountDown = new CountDownLatch(testDesc._verifiers.size());
    _commandCountDown = new CountDownLatch(testDesc._commands.size());
  }
  
  private String getValue(ZNRecord record, ZnodePropertyType type, String key)
  {
    String value = null;
    String keyParts[];
    
    if (record == null)
    {
      return null;
    }
    
    switch(type)
    {
    case SIMPLE:
      value = record.getSimpleField(key);
      break;
    case LIST:
      keyParts = key.split("/");
      if (keyParts.length != 2)
      {
        LOG.warn("key for list field is invalid: " + key + ", expected 2 parts: key1/key2");
        return null;
      }
      try
      {
        int idx = Integer.parseInt(keyParts[1]);
        value = record.getListField(keyParts[0]).get(idx);
      }
      catch (NumberFormatException e)
      {
        LOG.warn("key for list field is invalid: " + key + ", expected a number for key part2");
      }
      break;
    case MAP:
      keyParts = key.split("/");
      if (keyParts.length != 2)
      {
        LOG.warn("key for map field is invalid: " + key + ", expected 2 parts: key1/key2");
        return null;
      }
      Map<String, String> map = record.getMapField(keyParts[0]);
      if (map == null)
      {
        LOG.warn("key for map field is invalid: " + key + ", value for key part1 doesn't exist");
        return null;
      }
      value = map.get(keyParts[1]);
      break;
    default:
      break;
    }
    
    return value;
  }
  
  private void setValue(ZNRecord record, ZnodePropertyType type, String key, String value)
  {
    String keyParts[];
    
    switch(type)
    {
    case SIMPLE:
      record.setSimpleField(key, value);
      break;
    case LIST:
      keyParts = key.split("/");
      if (keyParts.length != 2)
      {
        LOG.warn("key for list field is invalid: " + key + ", expected 2 parts: key1/key2");
        return;
      }
      try
      {
        int idx = Integer.parseInt(keyParts[1]);
        List<String> list = record.getListField(keyParts[0]);
        list.remove(idx);
        list.add(idx, value);
      }
      catch (NumberFormatException e)
      {
        LOG.warn("key for list field is invalid: " + key + ", expected a number for key part2");
        return;
      }
      break;
    case MAP:
      keyParts = key.split("/");
      if (keyParts.length != 2)
      {
        LOG.warn("key for map field is invalid: " + key + ", expected 2 parts: key1/key2");
        return;
      }
      Map<String, String> map = record.getMapField(keyParts[0]);
      if (map == null)
      {
        LOG.warn("key for map field is invalid: " + key + ", value for key part1 doesn't exist");
        return;
      }
      map.put(keyParts[1], value);
      break;
    default:
      break;
    }
  }
  
  private boolean executeVerifier(ZNRecord record, ZnodeModVerifier verifier)
  {
    String value = getValue(record, verifier._propertyType, verifier._key);
    
    boolean result = false;
    if (verifier._operation.compareTo("==") == 0)
    {
      result = STRING_COMPARATOR.compare(value, verifier._value) == 0;
    }
    else if (verifier._operation.compareTo("!=") == 0)
    {
      result = STRING_COMPARATOR.compare(value, verifier._value) != 0;
    }
    
    return result;
  }
  
  private ZNRecord executeCommand(ZNRecord record, ZnodeModCommand command)
  {
    boolean success = true;
    if (command._operation.compareTo("+") == 0)
    {
      if (record == null)
      {
        record = new ZNRecord();
      }
      setValue(record, command._propertyType, command._key, command._updateValue);
    }
    else if (command._operation.compareTo("-") == 0)
    {
      record = null;
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
      if (entry.getValue() == null)
      {
        _zkClient.delete(entry.getKey());
      }
      else
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
    List<ZnodeModCommand> commandList = _testDesc._commands;
    Collections.sort(commandList, new Comparator<ZnodeModCommand>() {  // stable sort
      public int compare(ZnodeModCommand o1, ZnodeModCommand o2) {
        return (int) (o1._trigger._startTime - o2._trigger._startTime);
      }
    });
    
    long lastTime = 0;
    final Map<String, ZNRecord> znodeChanges = new HashMap<String, ZNRecord>();
    final Map<String, ZNRecord> znodeCache = new HashMap<String, ZNRecord>();
    
    for (ZnodeModCommand command : commandList)
    {
      ZnodeModTrigger trigger = command._trigger;
      if (trigger._startTime <= lastTime)
      {
        String path = command._znodePath;
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
        

        if (trigger._expectValue == null) // not a data triggered command
        {
          record = executeCommand(record, command);
          znodeCache.put(path, record);
          znodeChanges.put(path, record);
          _commandCountDown.countDown();
        }
        else
        {
          if (trigger._timeout > 0)
          {
            // set watcher before test the value so we can't miss any data change
            CommandDataListener listener = new CommandDataListener(command);
            CommandTimeoutTask task = new CommandTimeoutTask(listener);
            _timer.schedule(task, trigger._timeout);
            
            _zkClient.subscribeDataChanges(path, listener);
  
            String value = getValue(record, command._propertyType, command._key);
            if (value != null && trigger._expectValue.compareTo(value) == 0)
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
            String value = getValue(record, command._propertyType, command._key);
            if (value != null && trigger._expectValue.compareTo(value) == 0)
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
        Thread.sleep(trigger._startTime - lastTime);
        lastTime = trigger._startTime;
      }
    }
    
    fireAllZnodeChanges(znodeChanges);
    _commandCountDown.await();
    
    // execute verifiers
    // sort on verifier's timeout
    List<ZnodeModVerifier> verifierList = _testDesc._verifiers;
    Collections.sort(verifierList, new Comparator<ZnodeModVerifier>() {  // stable sort
      public int compare(ZnodeModVerifier o1, ZnodeModVerifier o2) {
        return (int) (o1._timeout - o2._timeout);
      }
    });
    
    for (ZnodeModVerifier verifier : verifierList)
    {
      _testResults.put(verifier.toString(), new Boolean(false));
      String path = verifier._znodePath;
      ZNRecord record = _zkClient.<ZNRecord>readData(path, true);
      

      if (verifier._timeout > 0)
      {
        // set watcher before checking the value so we can't miss any data change
        VerifierDataListener listener = new VerifierDataListener(verifier);
        VerifierTimeoutTask task = new VerifierTimeoutTask(listener);
        _timer.schedule(task, verifier._timeout);
        
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
