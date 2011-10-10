package com.linkedin.clustermanager.messaging;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.model.Message;

public abstract class AsyncCallback
{

  private static Logger _logger = Logger.getLogger(AsyncCallback.class);
  long _startTimeStamp = 0;
  long _timeout = -1;
  Timer _timer = null;
  List<Message> _messagesSent;
  final List<Message> _messageReplied = new ArrayList<Message>();
  boolean _timedOut = false;
  boolean _isInterrupted = false;

  /**
   * Enforcing timeout to be set
   * 
   * @param timeout
   */
  public AsyncCallback(long timeout)
  {
    _logger.info("Setting time out to " + timeout + " ms");
    _timeout = timeout;
  }

  public final void setTimeout(long timeout)
  {
    _logger.info("Setting time out to " + timeout + " ms");

  }

  public List<Message> getMessageReplied()
  {
    return _messageReplied;
  }

  public boolean isInterrupted()
  {
    return _isInterrupted;
  }

  public void setInterrupted(boolean b)
  {
    _isInterrupted = true;
  }

  public synchronized final void onReply(Message message)
  {
    _logger.info("OnReply msg " + message.getMsgId());
    if (!isDone())
    {
      _messageReplied.add(message);
      try
      {
        onReplyMessage(message);
      }
      catch(Exception e) 
      {
        _logger.error(e);
      }
    }
    if (isDone() && _timer != null)
    {
      _timer.cancel();
      notifyAll();
    }
  }

  /**
   * Default implementation will wait until every message sent gets a response
   * 
   * @return
   */
  public boolean isDone()
  {
    return _messageReplied.size() == _messagesSent.size();
  }

  public boolean isTimedOut()
  {
    return _timedOut;
  }

  final void setMessagesSent(List<Message> generatedMessage)
  {
    _messagesSent = generatedMessage;
  }
  final void startTimer(){
    if (_timer == null && _timeout>0)
    {
      if (_startTimeStamp == 0)
      {
        _startTimeStamp = new Date().getTime();
      }
      _timer = new Timer();
      _timer.schedule(new TimeoutTask(this), _timeout);
    }  
  }
  public abstract void onTimeOut();

  public abstract void onReplyMessage(Message message);

  class TimeoutTask extends TimerTask
  {
    AsyncCallback _callback;

    public TimeoutTask(AsyncCallback asyncCallback)
    {
      _callback = asyncCallback;
    }

    @Override
    public void run()
    {
      try
      {
        synchronized (_callback)
        {
          _callback._timedOut = true;
          _callback.onTimeOut();
          _callback.notifyAll();
        }
      } 
      catch (Exception e)
      {
        _logger.error(e);
      }
    }
  }

}