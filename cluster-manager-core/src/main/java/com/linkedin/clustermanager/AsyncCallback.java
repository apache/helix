package com.linkedin.clustermanager;

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
  boolean _timeOut = false;
  boolean _isInterrupted = false;

  public final void setTimeout(long timeout)
  {
    _logger.info("Setting time out to " + timeout + " ms");
    if(_timer == null && timeout != (long)(-1))
    {
      if(_startTimeStamp == 0)
      {
        _startTimeStamp = new Date().getTime();
      }
      _timeout = timeout;
      assert(_timeout > 0);
      _timer = new Timer();
      _timer.schedule(new TimeoutTask(this), _timeout);
    }
  }
  
  public List<Message> getMessageReplied()
  {
    return _messageReplied;
  }
  
  public boolean isInterrupted()
  {
    return _isInterrupted;
  }
  
  public synchronized final void onReply(Message message)
  {
    _logger.info("OnReply msg " + message.getMsgId());
    if(!isDone())
    {
      _messageReplied.add(message);
      onReplyMessage(message);  
    }
    
    if(isDone() && _timer != null)
    {
      _timer.cancel();
      notifyAll();
    }
  };
  
  public boolean isDone()
  {
    return _messageReplied.size() == _messagesSent.size();
  }
  
  public boolean isTimeOut()
  {
    return _timeOut;
  }

  public final void setMessagesSent(List<Message> generatedMessage)
  {
    _messagesSent = generatedMessage;
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
        synchronized(_callback)
        {
          _callback._timeOut = true;
          _callback.onTimeOut();
          _callback.notifyAll();
        }
      }
      catch(Exception e)
      {
        _logger.error(e);
      }
    }
  }
  
}