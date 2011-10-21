package com.linkedin.clustermanager.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.model.Message;

public class TestAsyncCallback
{
  class AsyncCallbackSample extends AsyncCallback
  {
    int _onTimeOutCalled = 0;
    int _onReplyMessageCalled = 0;
    @Override
    public void onTimeOut()
    {
      // TODO Auto-generated method stub
      _onTimeOutCalled ++;
    }

    @Override
    public void onReplyMessage(Message message)
    {
      _onReplyMessageCalled++;
    }
  }
  @Test(groups =
  { "unitTest" })
  public void testAsyncCallback() throws Exception
  {
    AsyncCallbackSample callback = new AsyncCallbackSample();
    Assert.assertFalse(callback.isInterrupted());
    Assert.assertFalse(callback.isTimedOut());
    Assert.assertTrue(callback.getMessageReplied().size() == 0);

    int nMsgs = 5;
    
    List<Message> messageSent = new ArrayList<Message>();
    for(int i = 0;i < nMsgs; i++)
    {
      messageSent.add(new Message("Test", UUID.randomUUID().toString()));
    }

    callback.setMessagesSent(messageSent);
    
    for(int i = 0;i < nMsgs; i++)
    {
      Assert.assertFalse(callback.isDone());
      callback.onReply(new Message("TestReply", UUID.randomUUID().toString()));
    }
    Assert.assertTrue(callback.isDone());

    Assert.assertTrue(callback._onTimeOutCalled == 0 );
    
    sleep(50);
    callback = new AsyncCallbackSample();
    callback.setMessagesSent(messageSent);
    callback.setTimeout(1000);
    sleep(50);
    callback.startTimer();
    Assert.assertFalse(callback.isTimedOut());
    for(int i = 0;i < nMsgs - 1; i++)
    {
      sleep(50);
      Assert.assertFalse(callback.isDone());
      Assert.assertTrue(callback._onReplyMessageCalled == i);
      callback.onReply(new Message("TestReply", UUID.randomUUID().toString()));
    }
    sleep(1000);
    Assert.assertTrue(callback.isTimedOut());
    Assert.assertTrue(callback._onTimeOutCalled == 1 );
    Assert.assertFalse(callback.isDone());
    
    callback = new AsyncCallbackSample();
    callback.setMessagesSent(messageSent);
    callback.setTimeout(1000);
    callback.startTimer();
    sleep(50);
    Assert.assertFalse(callback.isTimedOut());
    for(int i = 0;i < nMsgs; i++)
    {
      Assert.assertFalse(callback.isDone());
      sleep(50);
      Assert.assertTrue(callback._onReplyMessageCalled == i);
      callback.onReply(new Message("TestReply", UUID.randomUUID().toString()));
    }
    Assert.assertTrue(callback.isDone());
    sleep(1300);
    Assert.assertFalse(callback.isTimedOut());
    Assert.assertTrue(callback._onTimeOutCalled == 0 );
  }
  
  void sleep(int time)
  {
    try
    {
      Thread.currentThread().sleep(time);
    }
    catch(Exception e)
    {
      System.out.println(e);
    }
  }
}
