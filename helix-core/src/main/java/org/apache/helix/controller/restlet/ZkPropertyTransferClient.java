package org.apache.helix.controller.restlet;

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

import java.io.StringWriter;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import org.apache.helix.ZNRecord;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.Client;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.restlet.data.Status;

public class ZkPropertyTransferClient {
  private static Logger LOG = Logger.getLogger(ZkPropertyTransferClient.class);
  public static final int DEFAULT_MAX_CONCURRENTTASKS = 2;
  public static int SEND_PERIOD = 10 * 1000;

  public static final String USE_PROPERTYTRANSFER = "UsePropertyTransfer";

  int _maxConcurrentTasks;
  ExecutorService _executorService;
  Client[] _clients;
  AtomicInteger _requestCount = new AtomicInteger(0);

  // ZNRecord update buffer: key is the zkPath, value is the ZNRecordUpdate
  AtomicReference<ConcurrentHashMap<String, ZNRecordUpdate>> _dataBufferRef =
      new AtomicReference<ConcurrentHashMap<String, ZNRecordUpdate>>();
  Timer _timer;
  volatile String _webServiceUrl = "";

  static {
    org.restlet.engine.Engine.setLogLevel(Level.SEVERE);
  }

  public ZkPropertyTransferClient(int maxConcurrentTasks) {
    _maxConcurrentTasks = maxConcurrentTasks;
    _executorService = Executors.newFixedThreadPool(_maxConcurrentTasks);
    _clients = new Client[_maxConcurrentTasks];
    for (int i = 0; i < _clients.length; i++) {
      _clients[i] = new Client(Protocol.HTTP);
    }
    _timer = new Timer(true);
    _timer.schedule(new SendZNRecordTimerTask(), SEND_PERIOD, SEND_PERIOD);
    _dataBufferRef.getAndSet(new ConcurrentHashMap<String, ZNRecordUpdate>());
  }

  class SendZNRecordTimerTask extends TimerTask {
    @Override
    public void run() {
      sendUpdateBatch();
    }
  }

  public void enqueueZNRecordUpdate(ZNRecordUpdate update, String webserviceUrl) {
    try {
      LOG.info("Enqueue update to " + update.getPath() + " opcode: " + update.getOpcode() + " to "
          + webserviceUrl);
      _webServiceUrl = webserviceUrl;
      update.getRecord().setSimpleField(USE_PROPERTYTRANSFER, "true");
      synchronized (_dataBufferRef) {
        if (_dataBufferRef.get().containsKey(update._path)) {
          ZNRecord oldVal = _dataBufferRef.get().get(update.getPath()).getRecord();
          oldVal = update.getZNRecordUpdater().update(oldVal);
          _dataBufferRef.get().get(update.getPath())._record = oldVal;
        } else {
          _dataBufferRef.get().put(update.getPath(), update);
        }
      }
    } catch (Exception e) {
      LOG.error("", e);
    }
  }

  void sendUpdateBatch() {
    LOG.debug("Actual sending update with " + _dataBufferRef.get().size() + " updates to "
        + _webServiceUrl);
    Map<String, ZNRecordUpdate> updateCache = null;

    synchronized (_dataBufferRef) {
      updateCache = _dataBufferRef.getAndSet(new ConcurrentHashMap<String, ZNRecordUpdate>());
    }

    if (updateCache != null && updateCache.size() > 0) {
      ZNRecordUpdateUploadTask task =
          new ZNRecordUpdateUploadTask(updateCache, _webServiceUrl,
              _clients[_requestCount.intValue() % _maxConcurrentTasks]);
      _requestCount.incrementAndGet();
      _executorService.submit(task);
      LOG.trace("Queue size :" + ((ThreadPoolExecutor) _executorService).getQueue().size());
    }
  }

  public void shutdown() {
    LOG.info("Shutting down ZkPropertyTransferClient");
    _executorService.shutdown();
    _timer.cancel();
    for (Client client : _clients) {
      try {
        client.stop();
      } catch (Exception e) {
        LOG.error("", e);
      }
    }
  }

  class ZNRecordUpdateUploadTask implements Callable<Void> {
    Map<String, ZNRecordUpdate> _updateMap;
    String _webServiceUrl;
    Client _client;

    ZNRecordUpdateUploadTask(Map<String, ZNRecordUpdate> update, String webserviceUrl, Client client) {
      _updateMap = update;
      _webServiceUrl = webserviceUrl;
      _client = client;
    }

    @Override
    public Void call() throws Exception {
      LOG.debug("Actual sending update with " + _updateMap.size() + " updates to " + _webServiceUrl);
      long time = System.currentTimeMillis();
      Reference resourceRef = new Reference(_webServiceUrl);
      Request request = new Request(Method.PUT, resourceRef);

      ObjectMapper mapper = new ObjectMapper();
      StringWriter sw = new StringWriter();
      try {
        mapper.writeValue(sw, _updateMap);
      } catch (Exception e) {
        LOG.error("", e);
      }

      request.setEntity(ZNRecordUpdateResource.UPDATEKEY + "=" + sw, MediaType.APPLICATION_ALL);
      // This is a sync call. See com.noelios.restlet.http.StreamClientCall.sendRequest()
      Response response = _client.handle(request);

      if (response.getStatus().getCode() != Status.SUCCESS_OK.getCode()) {
        LOG.error("Status : " + response.getStatus());
      }
      LOG.info("Using time : " + (System.currentTimeMillis() - time));
      return null;
    }
  }
}
