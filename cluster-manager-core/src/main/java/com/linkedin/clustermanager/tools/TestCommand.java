package com.linkedin.clustermanager.tools;

import com.linkedin.clustermanager.ClusterManager;

public class TestCommand
{
  public enum CommandType 
  {
    MODIFY,
    VERIFY,
    START,
    STOP
  }
  
  public static class NodeOpArg
  {
    public ClusterManager _manager;
    public Thread _thread;
    
    public NodeOpArg(ClusterManager manager, Thread thread)
    {
      _manager = manager;
      _thread = thread;
    }
  }
  
  public TestTrigger _trigger;
  public CommandType _commandType;
  public ZnodeOpArg _znodeOpArg;
  public NodeOpArg _nodeOpArg;
  
  /**
   * 
   * @param type
   * @param arg
   */
  public TestCommand(CommandType type, ZnodeOpArg arg)
  {
    this(type, new TestTrigger(), arg);
  }
  
  /**
   * 
   * @param type
   * @param trigger
   * @param arg
   */
  public TestCommand(CommandType type, TestTrigger trigger, ZnodeOpArg arg)
  {
    _commandType = type;
    _trigger = trigger;
    _znodeOpArg = arg;
  }
  
  /**
   * 
   * @param type
   * @param trigger
   * @param arg
   */
  public TestCommand(CommandType type, TestTrigger trigger, NodeOpArg arg)
  {
    _commandType = type;
    _trigger = trigger;
    _nodeOpArg = arg;
  }
  
  @Override
  public String toString()
  {
    String ret = super.toString().substring(super.toString().lastIndexOf(".") + 1) + " ";
    if (_commandType == CommandType.MODIFY || _commandType == CommandType.VERIFY)
    {
      ret += _commandType.toString() + "|" + _trigger.toString() + "|" + _znodeOpArg.toString();
    }
    else if (_commandType == CommandType.START || _commandType == CommandType.STOP)
    {
      ret += _commandType.toString() + "|" + _trigger.toString() + "|" + _nodeOpArg.toString();
    }
    
    return ret;
  }
}
