package org.apache.helix.webapp;

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

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.ZNRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.Client;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.testng.Assert;

public class AdminTestHelper {

  public static class AdminThread {
    Thread _adminThread;
    CountDownLatch _stopCountDown = new CountDownLatch(1);
    String _zkAddr;
    int _port;

    public AdminThread(String zkAddr, int port) {
      _zkAddr = zkAddr;
      _port = port;
    }

    public void start() {
      Thread adminThread = new Thread(new Runnable() {
        @Override
        public void run() {
          HelixAdminWebApp app = null;
          try {
            app = new HelixAdminWebApp(_zkAddr, _port);
            app.start();
            // Thread.currentThread().join();
            _stopCountDown.await();
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            if (app != null) {
              // System.err.println("Stopping HelixAdminWebApp");
              app.stop();
            }
          }
        }
      });

      adminThread.setDaemon(true);
      adminThread.start();
    }

    public void stop() {
      _stopCountDown.countDown();
    }
  }

  public static ZNRecord get(Client client, String url) throws IOException {
    Reference resourceRef = new Reference(url);
    Request request = new Request(Method.GET, resourceRef);
    Response response = client.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();
    result.write(sw);

    String responseStr = sw.toString();
    Assert.assertTrue(responseStr.toLowerCase().indexOf("error") == -1);
    Assert.assertTrue(responseStr.toLowerCase().indexOf("exception") == -1);
    ObjectMapper mapper = new ObjectMapper();
    ZNRecord record = mapper.readValue(new StringReader(responseStr), ZNRecord.class);
    return record;
  }

  public static void delete(Client client, String url) throws IOException {
    Reference resourceRef = new Reference(url);
    Request request = new Request(Method.DELETE, resourceRef);
    Response response = client.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_NO_CONTENT);
  }

  public static ZNRecord post(Client client, String url, String body) throws IOException {
    Reference resourceRef = new Reference(url);
    Request request = new Request(Method.POST, resourceRef);

    request.setEntity(body, MediaType.APPLICATION_ALL);

    Response response = client.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Representation result = response.getEntity();
    StringWriter sw = new StringWriter();

    if (result != null) {
      result.write(sw);
    }
    String responseStr = sw.toString();
    Assert.assertTrue(responseStr.toLowerCase().indexOf("error") == -1);
    Assert.assertTrue(responseStr.toLowerCase().indexOf("exception") == -1);

    ObjectMapper mapper = new ObjectMapper();
    ZNRecord record = mapper.readValue(new StringReader(responseStr), ZNRecord.class);
    return record;
  }
}
