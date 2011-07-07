package com.linkedin.clustermanager.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.Message;

/**
 * Util class to create statusUpdates ZK records and error ZK records. These
 * message records are for diagnostics only, and they are stored on the
 * "StatusUpdates" and "errors" ZNodes in the zookeeper instances.
 * 
 * 
 * */
public class StatusUpdateUtil
{
  public enum Level
  {
    ERROR, WARNING, INFO
  }

  /**
   * Creates an empty ZNRecord as the statusUpdate/error record
   * */
  public ZNRecord createEmptyStatusUpdateRecord()
  {
    return new ZNRecord();
  }

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
    result.setSimpleField("MessageId", message.getId());
    result.setSimpleField("From  ", message.getFromState());
    result.setSimpleField("To    ", message.getToState());
    result.setSimpleField("Message status", message.getMsgState());

    DateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss.SSSSSS");
    String time = formatter.format(new Date());
//    time = time + " : " + new Date().getTime() % 1000;
//    time = time.replace('/', '-');
//    time = time.replace(':', '-');
    result.setSimpleField("TimeStamp", time);

    result.setSimpleField("Class", classInfo.toString());
    result.setSimpleField("LEVEL", level.toString());
    result.setSimpleField("Session Id", message.getTgtSessionId());
    String id = String.format("%4s %26s ", level.toString(), time)
        + message.getStateUnitKey() + " Trans:"
        + message.getFromState().charAt(0) + "->"
        + message.getToState().charAt(0) + " ";
    result.setId(id);

    result.setSimpleField("AdditionalInfo", additionalInfo);
    return result;
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
    ZNRecord record = createMessageStatusUpdateRecord(message, level,
        classInfo, additionalInfo);
    publishStatusUpdateRecord(record, message.getTgtName(), accessor);
  }

  public void logError(Message message, Class classInfo, String additionalInfo,
      ClusterDataAccessor accessor)
  {
    logMessageStatusUpdateRecord(message, Level.ERROR, classInfo,
        additionalInfo, accessor);
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
   * @param instanceName
   *          the instance name
   * @param accessor
   *          the zookeeper data accessor that writes the status update to
   *          zookeeper
   */
  public void publishStatusUpdateRecord(ZNRecord record, String instanceName,
      ClusterDataAccessor accessor)
  {
    accessor.setInstanceProperty(instanceName,
        InstancePropertyType.STATUSUPDATES, record.getId(), record);// ,
                                                                    // CreateMode.PERSISTENT_SEQUENTIAL);

    // If the error level is ERROR, also write the record to "ERROR" ZNode
    if (Level.ERROR.toString().equalsIgnoreCase(record.getSimpleField("LEVEL")))
    {
      publishErrorRecord(record, instanceName, accessor);
    }
  }

  /**
   * Write an error record to zookeeper to the zookeeper store.
   * 
   * @param record
   *          the status update record
   * @param instanceName
   *          the instance name
   * @param accessor
   *          the zookeeper data accessor that writes the status update to
   *          zookeeper
   */
  void publishErrorRecord(ZNRecord record, String instanceName,
      ClusterDataAccessor accessor)
  {
    assert (record.getSimpleField("LEVEL").equalsIgnoreCase("ERROR"));
    accessor.setInstanceProperty(instanceName, InstancePropertyType.ERRORS,
        record.getId(), record);// , CreateMode.PERSISTENT_SEQUENTIAL);
  }
}
