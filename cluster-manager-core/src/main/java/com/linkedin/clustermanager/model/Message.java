package com.linkedin.clustermanager.model;

import java.util.Date;
import java.util.Map;

import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordDecorator;

/**
 * Message class basically extends ZNRecord but provides additional fields
 *
 * @author kgopalak
 */

public class Message extends ZNRecordDecorator
{
  public enum MessageType
  {
    STATE_TRANSITION,
    USER_DEFINE_MSG,
    CONTROLLER_MSG,
    TASK_REPLY,
    NO_OP
  };

  public enum Attributes
  {
    MSG_ID, SRC_SESSION_ID, TGT_SESSION_ID, SRC_NAME, TGT_NAME,
    MSG_STATE, STATE_UNIT_KEY, STATE_UNIT_GROUP, FROM_STATE, TO_STATE,
    STATE_MODEL_DEF, CREATE_TIMESTAMP, READ_TIMESTAMP, EXECUTE_START_TIMESTAMP, MSG_TYPE,
    MSG_SUBTYPE, CORRELATION_ID, MESSAGE_RESULT, EXE_SESSION_ID, MESSAGE_TIMEOUT, RETRY_COUNT;
  }

  public Message(MessageType type, String msgId)
  {
    this(type.toString(), msgId);
  }

  public Message(String type, String msgId)
  {
    super(new ZNRecord(msgId));
    _record.setSimpleField(Attributes.MSG_TYPE.toString(), type);
    setMsgId(msgId);
    setMsgState("new");
    _record.setSimpleField(Attributes.CREATE_TIMESTAMP.toString(), ""
        + new Date().getTime());
  }

  public Message(ZNRecord record)
  {
    super(record);
    if(getMsgState() == null)
    {
      setMsgState("new");
    }
    if(getCreateTimeStamp() == 0)
    {
      _record.setSimpleField(Attributes.CREATE_TIMESTAMP.toString(), ""
          + new Date().getTime());
    }
  }

  public Message(ZNRecord record, String id)
  {
    super(new ZNRecord(record, id));
    setMsgId(id);
  }

  public void setMsgSubType(String subType)
  {
    _record.setSimpleField(Attributes.MSG_SUBTYPE.toString(), subType);
  }

  public String getMsgSubType()
  {
    return getSimpleFieldAsString(Attributes.MSG_SUBTYPE.toString());
  }

  void setMsgType(MessageType type)
  {
    _record.setSimpleField(Attributes.MSG_TYPE.toString(), type.toString());
  }

  public String getMsgType()
  {
    return getSimpleFieldAsString(Attributes.MSG_TYPE.toString());
  }

  public String getTgtSessionId()
  {
    return getSimpleFieldAsString(Attributes.TGT_SESSION_ID.toString());
  }

  public void setTgtSessionId(String tgtSessionId)
  {
    _record.setSimpleField(Attributes.TGT_SESSION_ID.toString(), tgtSessionId);
  }

  public String getSrcSessionId()
  {
    return getSimpleFieldAsString(Attributes.SRC_SESSION_ID.toString());
  }

  public void setSrcSessionId(String srcSessionId)
  {
    _record.setSimpleField(Attributes.SRC_SESSION_ID.toString(), srcSessionId);
  }

  public String getExecutionSessionId()
  {
    return getSimpleFieldAsString(Attributes.EXE_SESSION_ID.toString());
  }

  public void setExecuteSessionId(String exeSessionId)
  {
    _record.setSimpleField(Attributes.EXE_SESSION_ID.toString(), exeSessionId);
  }

  public String getMsgSrc()
  {
    return getSimpleFieldAsString(Attributes.SRC_NAME.toString());
  }

  public void setSrcName(String msgSrc)
  {
    _record.setSimpleField(Attributes.SRC_NAME.toString(), msgSrc);
  }

  public String getTgtName()
  {
    return getSimpleFieldAsString(Attributes.TGT_NAME.toString());
  }

  public void setMsgState(String msgState)
  {
    _record.setSimpleField(Attributes.MSG_STATE.toString(), msgState);
  }

  public String getMsgState()
  {
    return getSimpleFieldAsString(Attributes.MSG_STATE.toString());
  }

  public void setStateUnitKey(String stateUnitKey)
  {
    _record.setSimpleField(Attributes.STATE_UNIT_KEY.toString(), stateUnitKey);
  }

  public String getStateUnitKey()
  {
    return getSimpleFieldAsString(Attributes.STATE_UNIT_KEY.toString());
  }

  public String getMsgId()
  {
    return getSimpleFieldAsString(Attributes.MSG_ID.toString());
  }

  public void setMsgId(String msgId)
  {
    _record.setSimpleField(Attributes.MSG_ID.toString(), msgId);
  }

  public void setFromState(String state)
  {
    _record.setSimpleField(Attributes.FROM_STATE.toString(), state);
  }

  public String getFromState()
  {
    return getSimpleFieldAsString(Attributes.FROM_STATE.toString());
  }

  public void setToState(String state)
  {
    _record.setSimpleField(Attributes.TO_STATE.toString(), state);
  }

  public String getToState()
  {
    return getSimpleFieldAsString(Attributes.TO_STATE.toString());
  }

  // TODO why do we need this?
  private String getSimpleFieldAsString(String key)
  {
    Object ret = _record.getSimpleField(key);
    return (ret != null) ? ret.toString() : null;
  }

  public void setTgtName(String msgTgt)
  {
    _record.setSimpleField(Attributes.TGT_NAME.toString(), msgTgt);
  }

  public Boolean getDebug()
  {
    return false;
  }

  public Integer getGeneration()
  {
    return 1;
  }

  public void setStateUnitGroup(String stateUnitGroup)
  {
    _record.setSimpleField(Attributes.STATE_UNIT_GROUP.toString(),
        stateUnitGroup);
  }

  public String getStateUnitGroup()
  {
    return getSimpleFieldAsString(Attributes.STATE_UNIT_GROUP.toString());
  }

  public String getResourceGroupName()
  {
    return getSimpleFieldAsString(Attributes.STATE_UNIT_GROUP.toString());
  }

  public String getResourceKey()
  {
    return getSimpleFieldAsString(Attributes.STATE_UNIT_KEY.toString());
  }

  public String getStateModelDef()
  {
    return getSimpleFieldAsString(Attributes.STATE_MODEL_DEF.toString());
  }

  public void setStateModelDef(String stateModelDefName)
  {
    _record.setSimpleField(Attributes.STATE_MODEL_DEF.toString(),
        stateModelDefName);
  }

  public void setReadTimeStamp(long time)
  {
    _record.setSimpleField(Attributes.READ_TIMESTAMP.toString(), "" + time);
  }

  public void setExecuteStartTimeStamp(long time)
  {
    _record.setSimpleField(Attributes.EXECUTE_START_TIMESTAMP.toString(), ""
        + time);
  }

  public long getReadTimeStamp()
  {
    String timestamp = _record.getSimpleField(Attributes.READ_TIMESTAMP.toString());
    if (timestamp == null)
    {
      return 0;
    }
    try
    {
      return Long.parseLong(timestamp);
    } catch (Exception e)
    {
      return 0;
    }

  }

  public long getExecuteStartTimeStamp()
  {
    String timestamp = _record.getSimpleField(Attributes.EXECUTE_START_TIMESTAMP.toString());
    if (timestamp == null)
    {
      return 0;
    }
    try
    {
      return Long.parseLong(timestamp);
    }
    catch (Exception e)
    {
      return 0;
    }
  }

  public long getCreateTimeStamp()
  {
    if (_record.getSimpleField(Attributes.CREATE_TIMESTAMP.toString()) == null)
    {
      return 0;
    }
    try
    {
      return Long.parseLong(_record
          .getSimpleField(Attributes.CREATE_TIMESTAMP.toString()));
    } catch (Exception e)
    {
      return 0;
    }
  }

  public void setCorrelationId(String correlationId)
  {
    _record.setSimpleField(Attributes.CORRELATION_ID.toString(), correlationId);
  }

  public String getCorrelationId()
  {
    return getSimpleFieldAsString(Attributes.CORRELATION_ID.toString());
  }
  
  public int getExecutionTimeout()
  {
    if(!_record.getSimpleFields().containsKey(Attributes.MESSAGE_TIMEOUT.toString()))
    {
      return -1;
    }
    try
    {
      return Integer.parseInt(_record.getSimpleField(Attributes.MESSAGE_TIMEOUT.toString()));
    }
    catch(Exception e)
    {} 
    return -1;
  }
  
  public void setExecutionTimeout(int timeout)
  {
    _record.setSimpleField(Attributes.MESSAGE_TIMEOUT.toString(), "" + timeout);
  }
  
  public void setRetryCount(int retryCount)
  {
    _record.setSimpleField(Attributes.RETRY_COUNT.toString(), "" + retryCount);
  }
  
  public int getRetryCount()
  {
    if(!_record.getSimpleFields().containsKey(Attributes.RETRY_COUNT.toString()))
    {
      return 1;
    }
    try
    {
      return Integer.parseInt(_record.getSimpleField(Attributes.RETRY_COUNT.toString()));
    }
    catch(Exception e)
    {} 
    return 1;
  }

  public Map<String, String> getResultMap()
  {
    return _record.getMapField(Attributes.MESSAGE_RESULT.toString());
  }

  public void setResultMap(Map<String, String> resultMap)
  {
    _record.setMapField(Attributes.MESSAGE_RESULT.toString(), resultMap);
  }

  public static Message createReplyMessage(Message srcMessage, String instanceName,
      Map<String, String> taskResultMap)
  {
    if(srcMessage.getCorrelationId() == null)
    {
      throw new ClusterManagerException("Message " + srcMessage.getMsgId()
                                      + " does not contain correlation id");
    }
    Message replyMessage = new Message(MessageType.TASK_REPLY,"TEMPLATE");
    replyMessage.setCorrelationId(srcMessage.getCorrelationId());
    replyMessage.setTgtName(srcMessage.getMsgSrc());
    replyMessage.setResultMap(taskResultMap);
    replyMessage.setTgtSessionId("*");
    replyMessage.setMsgState("new");

    return replyMessage;
  }

  @Override
  public boolean isValid()
  {
    // TODO: refactor message to state transition message and task-message and 
    // implemement this function separately
    return true;
  }
}
