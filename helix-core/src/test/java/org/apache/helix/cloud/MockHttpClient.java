package org.apache.helix.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.InputStream;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.mockito.Matchers;
import org.mockito.Mockito;


/**
 * Mock a http client and provide response using resource file. This is for unit test purpose only.
 */
public class MockHttpClient {
  protected CloseableHttpClient createMockHttpClient(String file) throws Exception {
    InputStream responseInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);

    CloseableHttpResponse mockCloseableHttpResponse = Mockito.mock(CloseableHttpResponse.class);
    CloseableHttpClient mockCloseableHttpClient = Mockito.mock(CloseableHttpClient.class);

    Mockito.when(httpEntity.getContent()).thenReturn(responseInputStream);
    Mockito.when(mockCloseableHttpClient.execute(Matchers.any(HttpGet.class))).thenReturn(mockCloseableHttpResponse);
    Mockito.when(mockCloseableHttpResponse.getEntity()).thenReturn(httpEntity);
    Mockito.when(mockCloseableHttpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);

    return mockCloseableHttpClient;
  }
}