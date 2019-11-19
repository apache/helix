package org.apache.helix.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;

/**
 * Messages sent internally among nodes in the system to respond to changes in state.
 */
public class Message extends HelixProperty {
  /**
   * The major categories of messages that are sent
   */
  public enum MessageType {
    STATE_TRANSITION,
    STATE_TRANSITION_CANCELLATION,
    SCHEDULER_MSG,
    USER_DEFINE_MSG,
    CONTROLLER_MSG,
    TASK_REPLY,
    NO_OP,
    PARTICIPANT_ERROR_REPORT,
    PARTICIPANT_SESSION_CHANGE,
    CHAINED_MESSAGE, // this is a message subtype
    RELAYED_MESSAGE
  }

  /**
   * Properties attached to Messages
   */
  public enum Attributes {
    MSG_ID,
    RELAY_MSG_ID,
    SRC_SESSION_ID,
    TGT_SESSION_ID,
    SRC_NAME,
    TGT_NAME,
    SRC_INSTANCE_TYPE,
    MSG_STATE,
    PARTITION_NAME,
    RESOURCE_NAME,
    RESOURCE_GROUP_NAME,
    RESOURCE_TAG,
    FROM_STATE,
    TO_STATE,
    STATE_MODEL_DEF,
    CREATE_TIMESTAMP,
    READ_TIMESTAMP,
    EXECUTE_START_TIMESTAMP,
    MSG_TYPE,
    MSG_SUBTYPE,
    CORRELATION_ID,
    MESSAGE_RESULT,
    EXE_SESSION_ID,
    TIMEOUT,
    RETRY_COUNT,
    STATE_MODEL_FACTORY_NAME,
    BUCKET_SIZE,
    PARENT_MSG_ID, // used for group message mode
    ClusterEventName,
    INNER_MESSAGE,
    RELAY_PARTICIPANTS,
    RELAY_TIME,
    RELAY_FROM,
    EXPIRY_PERIOD,
    SRC_CLUSTER
  }

  /**
   * The current processed state of the message
   */
  public enum MessageState {
    NEW,
    READ, // not used
    UNPROCESSABLE // get exception when create handler
  }

  // default expiry time period for a relay message.
  public static final long RELAY_MESSAGE_DEFAULT_EXPIRY = 5 * 1000; // 5 second

  // This field is not persisted in zk/znode, i.e, the value will only be changed in local cached
  // copy of the message.
  // Currently, the field is only used for invalidating messages in controller's message cache.
  private boolean _expired = false;

  /**
   * Compares the creation time of two Messages
   */
  public static final Comparator<Message> CREATE_TIME_COMPARATOR = new Comparator<Message>() {
    @Override
    public int compare(Message m1, Message m2) {
      return new Long(m1.getCreateTimeStamp()).compareTo(new Long(m2.getCreateTimeStamp()));
    }
  };

  // AtomicInteger _groupMsgCountDown = new AtomicInteger(1);

  /**
   * Instantiate a message
   * @param type the message category
   * @param msgId unique message identifier
   */
  public Message(MessageType type, String msgId) {
    this(type.name(), msgId);
  }

  /**
   * Instantiate a message
   * @param type {@link MessageType} as a string or a custom message type
   * @param msgId unique message identifier
   */
  public Message(String type, String msgId) {
    super(new ZNRecord(msgId));
    _record.setSimpleField(Attributes.MSG_TYPE.toString(), type);
    setMsgId(msgId);
    setMsgState(MessageState.NEW);
    _record.setLongField(Attributes.CREATE_TIMESTAMP.toString(), new Date().getTime());
  }

  /**
   * Instantiate a message
   * @param record a ZNRecord corresponding to a message
   */
  public Message(ZNRecord record) {
    super(record);
    if (getMsgState() == null) {
      setMsgState(MessageState.NEW);
    }
    if (getCreateTimeStamp() == 0) {
      _record.setLongField(Attributes.CREATE_TIMESTAMP.toString(), new Date().getTime());
    }
  }

  /**
   * Set the time that the message was created
   * @param timestamp a UNIX timestamp
   */
  public void setCreateTimeStamp(long timestamp) {
    _record.setLongField(Attributes.CREATE_TIMESTAMP.toString(), timestamp);
  }

  /**
   * Instantiate a message with a new id
   * @param record a ZNRecord corresponding to a message
   * @param id unique message identifier
   */
  public Message(ZNRecord record, String id) {
    super(new ZNRecord(record, id));
    setMsgId(id);
  }

  /**
   * Instantiate a message with a new id
   * @param message message to be copied
   * @param id unique message identifier
   */
  public Message(Message message, String id) {
    super(new ZNRecord(message.getRecord(), id));
    setMsgId(id);
  }

  /**
   * Set a subtype of the message
   * @param subType name of the subtype
   */
  public void setMsgSubType(String subType) {
    _record.setSimpleField(Attributes.MSG_SUBTYPE.toString(), subType);
  }

  /**
   * Get the subtype of the message
   * @return the subtype name, or null
   */
  public String getMsgSubType() {
    return _record.getSimpleField(Attributes.MSG_SUBTYPE.toString());
  }

  /**
   * Set the type of this message
   * @param type {@link MessageType}
   */
  void setMsgType(MessageType type) {
    _record.setSimpleField(Attributes.MSG_TYPE.toString(), type.toString());
  }

  /**
   * Get the type of this message
   * @return String {@link MessageType} or a custom message type
   */
  public String getMsgType() {
    return _record.getSimpleField(Attributes.MSG_TYPE.toString());
  }

  /**
   * Get the session identifier of the destination node
   * @return session identifier
   */
  public String getTgtSessionId() {
    return _record.getSimpleField(Attributes.TGT_SESSION_ID.toString());
  }

  /**
   * Set the session identifier of the destination node
   * @param tgtSessionId session identifier
   */
  public void setTgtSessionId(String tgtSessionId) {
    _record.setSimpleField(Attributes.TGT_SESSION_ID.toString(), tgtSessionId);
  }

  /**
   * Get the session identifier of the source node
   * @return session identifier
   */
  public String getSrcSessionId() {
    return _record.getSimpleField(Attributes.SRC_SESSION_ID.toString());
  }

  /**
   * Set the session identifier of the source node
   * @param srcSessionId session identifier
   */
  public void setSrcSessionId(String srcSessionId) {
    _record.setSimpleField(Attributes.SRC_SESSION_ID.toString(), srcSessionId);
  }

  /**
   * Get the session identifier of the node that executes the message
   * @return session identifier
   */
  public String getExecutionSessionId() {
    return _record.getSimpleField(Attributes.EXE_SESSION_ID.toString());
  }

  /**
   * Set the session identifier of the node that executes the message
   * @param exeSessionId session identifier
   */
  public void setExecuteSessionId(String exeSessionId) {
    _record.setSimpleField(Attributes.EXE_SESSION_ID.toString(), exeSessionId);
  }

  /**
   * Get the instance from which the message originated
   * @return the instance name
   */
  public String getMsgSrc() {
    return _record.getSimpleField(Attributes.SRC_NAME.toString());
  }

  /**
   * Set the type of instance that the source node is
   * @param type {@link InstanceType}
   */
  public void setSrcInstanceType(InstanceType type) {
    _record.setEnumField(Attributes.SRC_INSTANCE_TYPE.toString(), type);
  }

  /**
   * Get the type of instance that the source is
   * @return {@link InstanceType}
   */
  public InstanceType getSrcInstanceType() {
    return _record.getEnumField(Attributes.SRC_INSTANCE_TYPE.toString(), InstanceType.class,
        InstanceType.PARTICIPANT);
  }

  /**
   * Set the name of the source instance
   * @param msgSrc instance name
   */
  public void setSrcName(String msgSrc) {
    _record.setSimpleField(Attributes.SRC_NAME.toString(), msgSrc);
  }

  /**
   * Get the name of the target instance
   * @return instance name
   */
  public String getTgtName() {
    return _record.getSimpleField(Attributes.TGT_NAME.toString());
  }

  /**
   * Set the current state of the message
   * @param msgState {@link MessageState}
   */
  public void setMsgState(MessageState msgState) { // HACK: The "tolowerCase()" call is to make the
    // change backward compatible
    _record.setSimpleField(Attributes.MSG_STATE.toString(), msgState.toString().toLowerCase());
  }

  /**
   * Get the current state of the message
   * @return {@link MessageState}
   */
  public MessageState getMsgState() {
    // HACK: The "toUpperCase()" call is to make the change backward compatible
    if (_record.getSimpleField(Attributes.MSG_STATE.toString()) != null) {
      return MessageState
          .valueOf(_record.getSimpleField(Attributes.MSG_STATE.toString()).toUpperCase());
    }
    return null;
  }

  /**
   * Set the name of the partition this message concerns
   * @param partitionName
   */
  public void setPartitionName(String partitionName) {
    _record.setSimpleField(Attributes.PARTITION_NAME.toString(), partitionName);
  }

  /**
   * Get the unique identifier of this message
   * @return message identifier
   */
  public String getMsgId() {
    return _record.getSimpleField(Attributes.MSG_ID.toString());
  }

  /**
   * Set the unique identifier of this message
   * @param msgId message identifier
   */
  public void setMsgId(String msgId) {
    _record.setSimpleField(Attributes.MSG_ID.toString(), msgId);
  }

  /**
   * Set the "from state" for transition-related messages
   * @param state the state name
   */
  public void setFromState(String state) {
    _record.setSimpleField(Attributes.FROM_STATE.toString(), state);
  }

  /**
   * Get the "from-state" for transition-related messages
   * @return state name, or null for other message types
   */
  public String getFromState() {
    return _record.getSimpleField(Attributes.FROM_STATE.toString());
  }

  /**
   * Set the "to state" for transition-related messages
   * @param state the state name
   */
  public void setToState(String state) {
    _record.setSimpleField(Attributes.TO_STATE.toString(), state);
  }

  /**
   * Get the "to state" for transition-related messages
   * @return state name, or null for other message types
   */
  public String getToState() {
    return _record.getSimpleField(Attributes.TO_STATE.toString());
  }

  /**
   * Set the instance for which this message is targeted
   * @param msgTgt instance name
   */
  public void setTgtName(String msgTgt) {
    _record.setSimpleField(Attributes.TGT_NAME.toString(), msgTgt);
  }

  /**
   * Check for debug mode
   * @return true if enabled, false if disabled
   */
  public Boolean getDebug() {
    return false;
  }

  /**
   * Get the generation that this message corresponds to
   * @return generation number
   */
  public Integer getGeneration() {
    return 1;
  }

  /**
   * Set the resource associated with this message
   * @param resourceName resource name to set
   */
  public void setResourceName(String resourceName) {
    _record.setSimpleField(Attributes.RESOURCE_NAME.toString(), resourceName);
  }

  /**
   * Get the resource associated with this message
   * @return resource name
   */
  public String getResourceName() {
    return _record.getSimpleField(Attributes.RESOURCE_NAME.toString());
  }

  /**
   * Set the resource group associated with this message
   * @param resourceGroupName resource group name to set
   */
  public void setResourceGroupName(String resourceGroupName) {
    _record.setSimpleField(Attributes.RESOURCE_GROUP_NAME.toString(), resourceGroupName);
  }

  /**
   * Get the resource group name associated with this message
   * @return resource group name
   */
  public String getResourceGroupName() {
    return _record.getSimpleField(Attributes.RESOURCE_GROUP_NAME.toString());
  }

  /**
   * Set the resource tag associated with this message
   * @param resourceTag resource tag to set
   */
  public void setResourceTag(String resourceTag) {
    _record.setSimpleField(Attributes.RESOURCE_TAG.toString(), resourceTag);
  }

  /**
   * Get the resource tag associated with this message
   * @return resource tag
   */
  public String getResourceTag() {
    return _record.getSimpleField(Attributes.RESOURCE_TAG.toString());
  }

  /**
   * Get the resource partition associated with this message
   * @return partition name
   */
  public String getPartitionName() {
    return _record.getSimpleField(Attributes.PARTITION_NAME.toString());
  }

  /**
   * Get the state model definition name
   * @return a String reference to the state model definition, e.g. "MasterSlave"
   */
  public String getStateModelDef() {
    return _record.getSimpleField(Attributes.STATE_MODEL_DEF.toString());
  }

  /**
   * Set the state model definition name
   * @param stateModelDefName a reference to the state model definition, e.g. "MasterSlave"
   */
  public void setStateModelDef(String stateModelDefName) {
    _record.setSimpleField(Attributes.STATE_MODEL_DEF.toString(), stateModelDefName);
  }

  /**
   * Set the time that this message was read
   * @param time UNIX timestamp
   */
  public void setReadTimeStamp(long time) {
    _record.setLongField(Attributes.READ_TIMESTAMP.toString(), time);
  }

  /**
   * Set the time that the instance executes tasks as instructed by this message
   * @param time UNIX timestamp
   */
  public void setExecuteStartTimeStamp(long time) {
    _record.setLongField(Attributes.EXECUTE_START_TIMESTAMP.toString(), time);
  }

  /**
   * Get the time that this message was read
   * @return UNIX timestamp
   */
  public long getReadTimeStamp() {
    return _record.getLongField(Attributes.READ_TIMESTAMP.toString(), 0L);
  }

  /**
   * Get the time that execution occurred as a result of this message
   * @return UNIX timestamp
   */
  public long getExecuteStartTimeStamp() {
    return _record.getLongField(Attributes.EXECUTE_START_TIMESTAMP.toString(), 0L);
  }

  /**
   * Get the time that this message was created
   * @return UNIX timestamp
   */
  public long getCreateTimeStamp() {
    return _record.getLongField(Attributes.CREATE_TIMESTAMP.toString(), 0L);
  }

  /**
   * Set a unique identifier that others can use to refer to this message in replies
   * @param correlationId a unique identifier, usually randomly generated
   */
  public void setCorrelationId(String correlationId) {
    _record.setSimpleField(Attributes.CORRELATION_ID.toString(), correlationId);
  }

  /**
   * Get the unique identifier attached to this message for reply matching
   * @return the correlation identifier
   */
  public String getCorrelationId() {
    return _record.getSimpleField(Attributes.CORRELATION_ID.toString());
  }

  /**
   * Get the time to wait before stopping execution of this message
   * @return the timeout in ms, or -1 indicating no timeout
   */
  public int getExecutionTimeout() {
    return _record.getIntField(Attributes.TIMEOUT.toString(), -1);
  }

  /**
   * Set the time to wait before stopping execution of this message
   * @param timeout the timeout in ms, or -1 indicating no timeout
   */
  public void setExecutionTimeout(int timeout) {
    _record.setIntField(Attributes.TIMEOUT.toString(), timeout);
  }

  /**
   * Set the number of times to retry message handling on timeouts
   * @param retryCount maximum number of retries
   */
  public void setRetryCount(int retryCount) {
    _record.setIntField(Attributes.RETRY_COUNT.toString(), retryCount);
  }

  /**
   * Get the number of times to retry message handling on timeouts
   * @return maximum number of retries
   */
  public int getRetryCount() {
    return _record.getIntField(Attributes.RETRY_COUNT.toString(), 0);
  }

  /**
   * Get the results of message execution
   * @return map of result property and value pairs
   */
  public Map<String, String> getResultMap() {
    return _record.getMapField(Attributes.MESSAGE_RESULT.toString());
  }

  /**
   * Set the results of message execution
   * @param resultMap map of result property and value pairs
   */
  public void setResultMap(Map<String, String> resultMap) {
    _record.setMapField(Attributes.MESSAGE_RESULT.toString(), resultMap);
  }

  /**
   * Get the state model factory associated with this message
   * @return the name of the factory
   */
  public String getStateModelFactoryName() {
    return _record.getSimpleField(Attributes.STATE_MODEL_FACTORY_NAME.toString());
  }

  /**
   * Set the state model factory associated with this message
   * @param factoryName the name of the factory
   */
  public void setStateModelFactoryName(String factoryName) {
    _record.setSimpleField(Attributes.STATE_MODEL_FACTORY_NAME.toString(), factoryName);
  }

  // TODO: remove this. impl in HelixProperty
  @Override
  public int getBucketSize() {
    return _record.getIntField(Attributes.BUCKET_SIZE.toString(), 0);
  }

  @Override
  public void setBucketSize(int bucketSize) {
    if (bucketSize > 0) {
      _record.setIntField(Attributes.BUCKET_SIZE.toString(), bucketSize);
    }
  }

  /**
   * Add or change a message attribute
   * @param attr {@link Attributes} attribute name
   * @param val attribute value
   */
  public void setAttribute(Attributes attr, String val) {
    _record.setSimpleField(attr.toString(), val);
  }

  /**
   * Get the value of an attribute
   * @param attr {@link Attributes}
   * @return attribute value
   */
  public String getAttribute(Attributes attr) {
    return _record.getSimpleField(attr.toString());
  }

  /**
   * Create a reply based on an incoming message
   * @param srcMessage the incoming message
   * @param instanceName the instance that is the source of the reply
   * @param taskResultMap the result of executing the incoming message
   * @return the reply Message
   */
  public static Message createReplyMessage(Message srcMessage, String instanceName,
      Map<String, String> taskResultMap) {
    if (srcMessage.getCorrelationId() == null) {
      throw new HelixException(
          "Message " + srcMessage.getMsgId() + " does not contain correlation id");
    }
    Message replyMessage = new Message(MessageType.TASK_REPLY, UUID.randomUUID().toString());
    replyMessage.setCorrelationId(srcMessage.getCorrelationId());
    replyMessage.setResultMap(taskResultMap);
    replyMessage.setTgtSessionId("*");
    replyMessage.setMsgState(MessageState.NEW);
    replyMessage.setSrcName(instanceName);
    if (srcMessage.getSrcInstanceType() == InstanceType.CONTROLLER) {
      replyMessage.setTgtName(InstanceType.CONTROLLER.name());
    } else {
      replyMessage.setTgtName(srcMessage.getMsgSrc());
    }
    return replyMessage;
  }

  /**
   * Add a partition to a collection of partitions associated with this message
   * @param partitionName the partition name to add
   */
  public void addPartitionName(String partitionName) {
    if (_record.getListField(Attributes.PARTITION_NAME.toString()) == null) {
      _record.setListField(Attributes.PARTITION_NAME.toString(), new ArrayList<String>());
    }

    List<String> partitionNames = _record.getListField(Attributes.PARTITION_NAME.toString());
    if (!partitionNames.contains(partitionName)) {
      partitionNames.add(partitionName);
    }
  }

  /**
   * Get a list of partitions associated with this message
   * @return list of partition names
   */
  public List<String> getPartitionNames() {
    List<String> partitionNames = _record.getListField(Attributes.PARTITION_NAME.toString());
    if (partitionNames == null) {
      return Collections.emptyList();
    }

    return partitionNames;
  }

  /**
   * Get the completion time of previous task associated with this message.
   * This applies only when this is a relay message,
   * which specified the completion time of the task running on the participant that sent this relay
   * message.
   * @return
   */
  public long getRelayTime() {
    return _record.getLongField(Attributes.RELAY_TIME.name(), -1);
  }

  /**
   * Set the completion time of previous task associated with this message.
   * This applies only when this is a relay message,
   * which specified the completion time of the task running on the participant that sent this relay
   * message.
   * @param completionTime
   */
  public void setRelayTime(long completionTime) {
    _record.setLongField(Attributes.RELAY_TIME.name(), completionTime);
  }

  /**
   * Attach a relayed message and its destination participant to this message.
   * WARNNING: only content in SimpleFields of relayed message will be carried over and sent,
   * all contents in either ListFields or MapFields will be ignored.
   * @param instance destination participant name
   * @param message relayed message.
   */
  public void attachRelayMessage(String instance, Message message) {
    List<String> relayList = _record.getListField(Attributes.RELAY_PARTICIPANTS.name());
    if (relayList == null) {
      relayList = Collections.EMPTY_LIST;
    }
    Set<String> relayParticipants = new LinkedHashSet<>(relayList);
    relayParticipants.add(instance);
    Map<String, String> messageInfo = message.getRecord().getSimpleFields();
    messageInfo.put(Attributes.RELAY_MSG_ID.name(), message.getId());
    messageInfo.put(Attributes.MSG_SUBTYPE.name(), MessageType.RELAYED_MESSAGE.name());
    messageInfo.put(Attributes.RELAY_FROM.name(), getTgtName());
    messageInfo.put(Attributes.EXPIRY_PERIOD.name(), String.valueOf(RELAY_MESSAGE_DEFAULT_EXPIRY));
    _record.setMapField(instance, messageInfo);
    _record.setListField(Attributes.RELAY_PARTICIPANTS.name(),
        Lists.newArrayList(relayParticipants));
  }

  /**
   * Get relay message attached for the given instance.
   * @param instance
   * @return null if no message for the instance
   */
  public Message getRelayMessage(String instance) {
    Map<String, String> messageInfo = _record.getMapField(instance);
    if (messageInfo != null) {
      String id = messageInfo.get(Attributes.RELAY_MSG_ID.name());
      if (id == null) {
        id = messageInfo.get(Attributes.MSG_ID.name());
        if (id == null) {
          return null;
        }
      }
      ZNRecord record = new ZNRecord(id);
      record.setSimpleFields(messageInfo);
      return new Message(record);
    }

    return null;
  }

  public String getRelaySrcHost() {
    return _record.getSimpleField(Attributes.RELAY_FROM.name());
  }

  /**
   * Get all relay messages attached to this message as a map (instance->message).
   * @return map of instanceName->message, empty map if none.
   */
  public Map<String, Message> getRelayMessages() {
    Map<String, Message> relayMessageMap = new HashMap<>();
    List<String> relayParticipants = _record.getListField(Attributes.RELAY_PARTICIPANTS.name());
    if (relayParticipants != null) {
      for (String p : relayParticipants) {
        Message msg = getRelayMessage(p);
        if (p != null) {
          relayMessageMap.put(p, msg);
        }
      }
    }

    return relayMessageMap;
  }

  /**
   * Whether there are any relay message attached to this message.
   * @return
   */
  public boolean hasRelayMessages() {
    List<String> relayHosts = _record.getListField(Attributes.RELAY_PARTICIPANTS.name());
    return (relayHosts != null && relayHosts.size() > 0);
  }

  /**
   * Whether this message is a relay message.
   * @return
   */
  public boolean isRelayMessage() {
    String subType = _record.getStringField(Attributes.MSG_SUBTYPE.name(), null);
    String relayFrom = _record.getStringField(Attributes.RELAY_FROM.name(), null);
    return MessageType.RELAYED_MESSAGE.name().equals(subType) && (relayFrom != null);
  }

  /**
   * Whether a message is expired.
   * A message is expired if:
   * 1) creationTime + expiryPeriod > current time
   * or
   * 2) relayTime + expiryPeriod > current time iff it is relay message.
   * @return
   */
  public boolean isExpired() {
    if (_expired) {
      return true;
    }

    long expiry = getExpiryPeriod();
    if (expiry < 0) {
      return false;
    }

    long current = System.currentTimeMillis();
    // use relay time if this is a relay message
    if (isRelayMessage()) {
      long relayTime = getRelayTime();
      return (relayTime > 0 && (relayTime + expiry < current));
    }

    return getCreateTimeStamp() + expiry < current;
  }

  /**
   * Set a message to expired.
   * !! CAUTION: The expired field is not persisted into ZNODE,
   * i.e, set this field will only change its value in its local cache version,
   * not the one on ZK, even ZkClient.Set(Message) is called to persist it into ZK.
   * This method should NOT be called by any non-Helix code.
   * @param expired
   */
  public void setExpired(boolean expired) {
    _expired = expired;
  }

  /**
   * Get the expiry period (in milliseconds)
   * @return
   */
  public long getExpiryPeriod() {
    return _record.getLongField(Attributes.EXPIRY_PERIOD.name(), -1);
  }

  /**
   * Set expiry period for this message.
   * A message will be expired after this period of time from either its 1) creationTime or 2)
   * relayTime if it is relay message.
   * Default is -1 if it is not set.
   * @param expiry
   */
  public void setExpiryPeriod(long expiry) {
    _record.setLongField(Attributes.EXPIRY_PERIOD.name(), expiry);
  }

  /**
   * Get the source cluster name
   * @return the source cluster from where the message was sent or null if the message was sent
   *         locally
   */
  public String getSrcClusterName() {
    return _record.getStringField(Attributes.SRC_CLUSTER.name(), null);
  }

  /**
   * Set the source cluster name
   * @param clusterName source cluster name where message was sent from
   */
  public void setSrcClusterName(String clusterName) {
    _record.setSimpleField(Attributes.SRC_CLUSTER.name(), clusterName);
  }

  /**
   * Check if this message is targetted for a controller
   * @return true if this is a controller message, false otherwise
   */
  public boolean isControlerMsg() {
    return getTgtName().equalsIgnoreCase(InstanceType.CONTROLLER.name());
  }

  /**
   * Get the {@link PropertyKey} for this message
   * @param keyBuilder PropertyKey Builder
   * @param instanceName target instance
   * @return message PropertyKey
   */
  public PropertyKey getKey(Builder keyBuilder, String instanceName) {
    if (isControlerMsg()) {
      return keyBuilder.controllerMessage(getId());
    } else {
      return keyBuilder.message(instanceName, getId());
    }
  }

  private boolean isNullOrEmpty(String data) {
    return data == null || data.length() == 0 || data.trim().length() == 0;
  }

  @Override
  public boolean isValid() {
    // TODO: refactor message to state transition message and task-message and
    // implement this function separately

    if (getMsgType().equals(MessageType.STATE_TRANSITION.name())
        || getMsgType().equals(MessageType.STATE_TRANSITION_CANCELLATION.name())) {
      boolean isNotValid = isNullOrEmpty(getTgtName()) || isNullOrEmpty(getPartitionName())
          || isNullOrEmpty(getResourceName()) || isNullOrEmpty(getStateModelDef())
          || isNullOrEmpty(getToState()) || isNullOrEmpty(getFromState())
          || isNullOrEmpty(getTgtSessionId());

      return !isNotValid;
    }
    return true;
  }
}
