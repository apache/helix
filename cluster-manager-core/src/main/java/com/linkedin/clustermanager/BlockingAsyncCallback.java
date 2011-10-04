package com.linkedin.clustermanager;

import com.linkedin.clustermanager.model.Message;

public class BlockingAsyncCallback extends AsyncCallback
{
  public BlockingAsyncCallback()
  {}
  
  public BlockingAsyncCallback(long timeOut)
  {
    setTimeout(timeOut);
  }
  public void onReplyMessage(Message message)
  {
    notifyAll();
  }
  public void onTimeOut()
  {
    notifyAll();
  }

  public void setInterrupted(boolean b)
  {
    _isInterrupted = true;
    
  }
}
