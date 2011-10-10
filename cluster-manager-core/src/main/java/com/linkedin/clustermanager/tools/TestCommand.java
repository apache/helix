package com.linkedin.clustermanager.tools;

public class TestCommand
{
  public enum CommandType 
  {
    MODIFY,
    VERIFY,
    START,
    STOP
  }
  
  public TestTrigger _trigger;
  public CommandType _commandType;
  public ZnodeOpArg _znodeOpArg;
  public Thread _threadArg;
  
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
  public TestCommand(CommandType type, TestTrigger trigger, Thread arg)
  {
    _commandType = type;
    _trigger = trigger;
    _threadArg = arg;
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
      ret += _commandType.toString() + "|" + _trigger.toString() + "|" + _threadArg.toString();
    }
    
    return ret;
  }
}
