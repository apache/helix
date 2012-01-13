package com.linkedin.clustermanager.messaging.handling;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.messaging.handling.MessageHandler.ErrorCode;
import com.linkedin.clustermanager.messaging.handling.MessageHandler.ErrorType;
import com.linkedin.clustermanager.model.Message;

/**
 * Provides the base class for all message handlers. 
 * 
 */
public abstract class MessageHandler
{
  public enum ErrorType
  {
    FRAMEWORK, INTERNAL
  }
  
  public enum ErrorCode
  {
    ERROR, CANCEL
  }
  /**
   * The message to be handled 
   */
  protected final Message _message;
  
  /**
   * The context for handling the message. The cluster manager interface can be
   * accessed from NotificationContext 
   */
  protected final NotificationContext _notificationContext;
  
  /**
   * The constructor. The message and notification context must be provided via
   * creation.
   */
  public MessageHandler(Message message, NotificationContext context)
  {
    _message = message;
    _notificationContext = context;
  }
  
  /**
   * Message handling routine. The function is called in a thread pool task in 
   * CMTaskExecutor 
   * 
   * @return returns the CMTaskResult which contains info about the message processing.
   */
  public abstract CMTaskResult handleMessage()  throws InterruptedException;
  
  /**
   * Callback when error happens in the message handling pipeline. 
   * @param type TODO
   * @param retryCountLeft - The number of retries that the framework will 
   * continue trying to handle the message
   * @param ErrorType - denote if the exception happens in framework or happens in the 
   * customer's code
   */
  public abstract void onError(Exception e, ErrorCode code, ErrorType type);
}
