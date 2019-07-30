package org.apache.helix.rest.client;

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

import org.apache.helix.rest.server.HelixRestServer;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The memory efficient factory to create instances for {@link CustomRestClient}
 */
public class CustomRestClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CustomRestClientFactory.class);

  private static HttpClient HTTP_CLIENT_INSTANCE = null;

  private CustomRestClientFactory() {
  }

  /**
   * Because {@link CustomRestClientImpl} has a transient in-memory cache with lifecycle of one rest request, a new
   * instance needs to be created
   *
   * @return an new instance of {@link CustomRestClient}
   */
  public static CustomRestClient get() {
    return new CustomRestClientImpl(getHttpClient());
  }

  private static HttpClient getHttpClient() {
    if (HTTP_CLIENT_INSTANCE == null) {
      synchronized (CustomRestClientFactory.class) {
        if (HTTP_CLIENT_INSTANCE == null) {
          if (HelixRestServer.REST_SERVER_SSL_CONTEXT != null) {
            HTTP_CLIENT_INSTANCE = HttpClients.custom()
                .setSSLContext(HelixRestServer.REST_SERVER_SSL_CONTEXT)
                .setSSLSocketFactory(
                    new SSLConnectionSocketFactory(HelixRestServer.REST_SERVER_SSL_CONTEXT, new NoopHostnameVerifier()))
                .build();
          } else {
            HTTP_CLIENT_INSTANCE = HttpClients.createDefault();
          }
        }
      }
    }
    return HTTP_CLIENT_INSTANCE;
  }
}
