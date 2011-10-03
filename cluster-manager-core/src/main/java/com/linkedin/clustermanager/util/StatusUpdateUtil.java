package com.linkedin.clustermanager.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
/**
 * Util class to create statusUpdates ZK records and error ZK records. These
 * message records are for diagnostics only, and they are stored on the
 * "StatusUpdates" and "errors" ZNodes in the zookeeper instances.
 * 
 * 
 * */
public class StatusUpdateUtil
{
  static Logger _logger = Logger.getLogger(StatusUpdateUtil.class);
  
  public enum Level
  {
    ERROR, WARNING, INFO
  }
 
  /**
   * Creates an empty ZNRecord as the statusUpdate/error record
   */
  public ZNRecord createEmptyStatusUpdateRecord()
  {
    return new ZNRecord();
  }
  
  /**
   * Create a ZNRecord for a message, which stores the content of the message (stored in simple fields)
   * into the ZNRecord mapFields. In this way, the message update can be merged with the previous status
   * update record in the zookeeper. See ZNRecord.merge() for more details.
   * */
  ZNRecord createMessageLogRecord(Message message)
  {
    ZNRecord result = new ZNRecord();
    String mapFieldKey = "MESSAGE " + message.getMsgId();
    result.setMapField(mapFieldKey, new TreeMap<String, String>());
    
    // Store all the simple fields of the message in the new ZNRecord's map field.
    for(String simpleFieldKey : message.getRecord().getSimpleFields().keySet())
    {
      result.getMapField(mapFieldKey).put(simpleFieldKey, message.getRecord().getSimpleField(simpleFieldKey));
    }
    result.setId(getStatusUpdateSubPath(message));
    return result;
  }
  Map<String, String> _recordedMessages = new ConcurrentHashMap<String, String>();
  
  /**
   * Create a statusupdate that is related to a cluster manager message.
   * 
   * @param message
   *          the related cluster manager message
   * @param level
   *          the error level
   * @param classInfo
   *          class info about the class that reports the status update
   * @param additional
   *          info the additional debug information
   */
  public ZNRecord createMessageStatusUpdateRecord(Message message, Level level,
      Class classInfo, String additionalInfo)
  {
    ZNRecord result = createEmptyStatusUpdateRecord();
    Map<String, String> contentMap = new TreeMap<String, String>();
    
    contentMap.put("Message state", message.getMsgState());
    contentMap.put("AdditionalInfo", additionalInfo);
    contentMap.put("Class", classInfo.toString());
    contentMap.put("MSG_ID", message.getMsgId());

    DateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss.SSSSSS");
    String time = formatter.format(new Date());

    String id = String.format("%4s %26s ", level.toString(), time)
        + getRecordIdForMessage(message);
    
    result.setMapField(id, contentMap);
    
    result.setId(getStatusUpdateSubPath(message));
    return result;
  }
  
  String getRecordIdForMessage(Message message)
  {
    if(message.getMsgType().equals(MessageType.STATE_TRANSITION))
    {
      return message.getStateUnitKey() + " Trans:"
        + message.getFromState().charAt(0) + "->"
        + message.getToState().charAt(0) + "  "
        + UUID.randomUUID().toString();
    }
    else
    {
      return message.getMsgType()
        + UUID.randomUUID().toString();
    }
  }
  /**
   * Create a statusupdate that is related to a cluster manager message, then
   * record it to the zookeeper store.
   * 
   * @param message
   *          the related cluster manager message
   * @param level
   *          the error level
   * @param classInfo
   *          class info about the class that reports the status update
   * @param additional
   *          info the additional debug information
   * @param accessor
   *          the zookeeper data accessor that writes the status update to
   *          zookeeper
   */
  public void logMessageStatusUpdateRecord(Message message, Level level,
      Class classInfo, String additionalInfo, ClusterDataAccessor accessor)
  {
    try
    {
      ZNRecord record = createMessageStatusUpdateRecord(message, level,
        classInfo, additionalInfo);
      publishStatusUpdateRecord(record, message, level, accessor);
    }
    catch(Exception e)
    {
      _logger.error(e);
    }
}

  public void logError(Message message, Class classInfo, String additionalInfo,
      ClusterDataAccessor accessor)
  {
    logMessageStatusUpdateRecord(message, Level.ERROR, classInfo,
        additionalInfo, accessor);
  }
  
  public void logError(Message message, Class classInfo, Exception e, String additionalInfo,
      ClusterDataAccessor accessor)
  {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    logMessageStatusUpdateRecord(message, Level.ERROR, classInfo,
        additionalInfo + sw.toString(), accessor);
  }

  public void logInfo(Message message, Class classInfo, String additionalInfo,
      ClusterDataAccessor accessor)
  {
    logMessageStatusUpdateRecord(message, Level.INFO, classInfo,
        additionalInfo, accessor);
  }

  public void logWarning(Message message, Class classInfo,
      String additionalInfo, ClusterDataAccessor accessor)
  {
    logMessageStatusUpdateRecord(message, Level.WARNING, classInfo,
        additionalInfo, accessor);
  }

  /**
   * Write a status update record to zookeeper to the zookeeper store.
   * 
   * @param record
   *          the status update record
   * @param message
   *          the message to be logged
   * @param level 
   *          the error level of the message update
   * @param accessor
   *          the zookeeper data accessor that writes the status update to
   *          zookeeper
   */
  void publishStatusUpdateRecord(ZNRecord record, Message message,
      Level level, ClusterDataAccessor accessor)
  {
    String instanceName = message.getTgtName();
    String statusUpdateSubPath = getStatusUpdateSubPath(message);
    
    if(!_recordedMessages.containsKey(message.getMsgId()))
    {
      accessor.updateInstanceProperty(instanceName,
          InstancePropertyType.STATUSUPDATES, statusUpdateSubPath, message.getStateUnitKey(), createMessageLogRecord(message));
      _recordedMessages.put(message.getMsgId(), message.getMsgId());
    }
      
    accessor.updateInstanceProperty(instanceName,
        InstancePropertyType.STATUSUPDATES, statusUpdateSubPath, message.getStateUnitKey(), record);
    
    // If the error level is ERROR, also write the record to "ERROR" ZNode
    if (Level.ERROR == level)
    {
      publishErrorRecord(record, message, accessor);
    }
  }
  
  /**
   * Generate the sub-path under STATUSUPDATE or ERROR path for a status update
   * 
   */
  String getStatusUpdateSubPath(Message message)
  {
    if(message.getStateUnitGroup() != null)
      return message.getTgtSessionId() + "__" + message.getStateUnitGroup();
    return message.getTgtSessionId();
  }

  /**
   * Write an error record to zookeeper to the zookeeper store.
   * 
   * @param record
   *          the status update record
   * @param message
   *          the message to be logged
   * @param accessor
   *          the zookeeper data accessor that writes the status update to
   *          zookeeper
   */
  void publishErrorRecord(ZNRecord record, Message message,
      ClusterDataAccessor accessor)
  {
    String instanceName = message.getTgtName();
    String statusUpdateSubPath = getStatusUpdateSubPath(message);
    
    accessor.updateInstanceProperty(instanceName, InstancePropertyType.ERRORS,
        statusUpdateSubPath, message.getStateUnitKey(), record);
  }
}
