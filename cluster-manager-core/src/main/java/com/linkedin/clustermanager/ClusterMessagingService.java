package com.linkedin.clustermanager;

import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.Message;

/**
 * Provides the ability to <br>
 * <li>Send message to a specific component in the cluster[ participant,
 * controller, Router(probably not needed) ]</li> <li>Broadcast message to all
 * nodes</li> <li>Send message to instances that hold a specific resource</li>
 * <li>Asynchronous request response api. Send message with a co-relation id and
 * invoke a method when there is a response. Can support timeout.</li>
 * 
 * @author kgopalak
 * 
 */
public interface ClusterMessagingService
{
  /**
   * Send message matching the specifications mentioned in recipientCriteria. 
   * @param receipientCriteria @See Criteria
   * @param message message to be sent. Some attributes of this message will be changed as required
   * @return returns how many messages were successfully sent.
   */
  int send(Criteria recipientCriteria, Message message);

  /**
   * 
   * @param receipientCriteria
   * @param message
   * @param callbackOnReply
   * @return
   */
  int send(Criteria receipientCriteria, Message message,
      AsyncCallback callbackOnReply);

  /**
   * Send message to a particular instance. The message is guaranteed to be
   * processed by the instance but no guarantee on when it is processed.<br>
   * For example, if the instance is down, it will be processed later when the
   * instance comes up.
   * 
   * @param instance
   * @param message
   * @return false if the instanceName is not present in the cluster
   */
  // boolean sendToInstance(String instanceName, Message message);

  /**
   * Same as sendToInstance method but ensure that it will only be processed by
   * the current session. <br>
   * If the receiver is down and fails before the message is processed, it will
   * not be processed by the instance when it comes back
   * 
   * @param instanceName
   * @param message
   * @return
   */
  // boolean sendToCurrentSession(String instanceName, Message message);

  /**
   * Send message to a particular instance. The message is guaranteed to be
   * processed by the instance but no guarantee on when it is processed.<br>
   * For example, if the instance is down, it will be processed later when the
   * instance comes up.
   * 
   * @param criteria
   *          ,
   * @param message
   * @return false
   */
  // boolean sendToInstanceMatchingCriteria(InstanceStateCriteria criteria,
  // Message message);

  /**
   * Will send one message to all nodes in the cluster.
   * 
   * @param message
   * @return
   */
  // boolean broadcastToAllInstances(Message message);

  /**
   * Will send one message per resource to all nodes in the cluster.
   * 
   * @param message
   * @return
   */
  // boolean broadcastToAllResources(Message message);

  /**
   * This can be used to communicate with the ClusterController. This will be
   * picked up and processed by the current active controller. <br>
   * Cluster manager will inspect MessageType field and do the processing
   * accordingly.
   * 
   * @param message
   * @return
   */
  // boolean sendToController(Message message);

  /**
   * This will send the message to all instances matching the criteria<br>
   * When instances reply to the message AsynCallback will be invoked.
   * Application can specify a timeout in the message, on every callback the
   * implementation must specify if it needs to wait for more messages.
   * 
   * @param criteria
   * @param message
   * @param callback
   * @return
   */
  // boolean sendReceive(InstanceStateCriteria criteria, Message message,
  // AsyncCallback callback);
  
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory);
}
