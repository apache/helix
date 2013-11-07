package org.apache.helix.messaging;

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
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

public abstract class AsyncCallback {

  private static Logger _logger = Logger.getLogger(AsyncCallback.class);
  long _startTimeStamp = 0;
  protected long _timeout = -1;
  Timer _timer = null;
  List<Message> _messagesSent;
  protected final List<Message> _messageReplied = new ArrayList<Message>();
  boolean _timedOut = false;
  boolean _isInterrupted = false;

  /**
   * Enforcing timeout to be set
   * @param timeout
   */
  public AsyncCallback(long timeout) {
    _logger.info("Setting time out to " + timeout + " ms");
    _timeout = timeout;
  }

  public AsyncCallback() {
    this(-1);
  }

  public final void setTimeout(long timeout) {
    _logger.info("Setting time out to " + timeout + " ms");
    _timeout = timeout;

  }

  public List<Message> getMessageReplied() {
    return _messageReplied;
  }

  public boolean isInterrupted() {
    return _isInterrupted;
  }

  public void setInterrupted(boolean b) {
    _isInterrupted = true;
  }

  public synchronized final void onReply(Message message) {
    _logger.info("OnReply msg " + message.getMessageId());
    if (!isDone()) {
      _messageReplied.add(message);
      try {
        onReplyMessage(message);
      } catch (Exception e) {
        _logger.error(e);
      }
    }
    if (isDone()) {
      if (_timer != null) {
        _timer.cancel();
      }
      notifyAll();
    }
  }

  /**
   * Default implementation will wait until every message sent gets a response
   * @return
   */
  public boolean isDone() {
    return _messageReplied.size() == _messagesSent.size();
  }

  public boolean isTimedOut() {
    return _timedOut;
  }

  final void setMessagesSent(List<Message> generatedMessage) {
    _messagesSent = generatedMessage;
  }

  final void startTimer() {
    if (_timer == null && _timeout > 0) {
      if (_startTimeStamp == 0) {
        _startTimeStamp = new Date().getTime();
      }
      _timer = new Timer(true);
      _timer.schedule(new TimeoutTask(this), _timeout);
    }
  }

  public abstract void onTimeOut();

  public abstract void onReplyMessage(Message message);

  class TimeoutTask extends TimerTask {
    AsyncCallback _callback;

    public TimeoutTask(AsyncCallback asyncCallback) {
      _callback = asyncCallback;
    }

    @Override
    public void run() {
      try {
        synchronized (_callback) {
          _callback._timedOut = true;
          _callback.notifyAll();
          _callback.onTimeOut();
        }
      } catch (Exception e) {
        _logger.error(e);
      } finally {
        if (_timer != null) {
          _timer.cancel();
        }
      }
    }
  }

}
