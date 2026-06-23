package org.apache.helix.rest.server.filters;

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

import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCORSFilter {
  private static final String ALLOWED_ORIGIN = "https://admin.example.com";
  private static final String DISALLOWED_ORIGIN = "https://evil.example.com";

  @Test
  public void testResponseAllowedOriginEchoesOriginAndCredentials() throws IOException {
    CORSFilter filter = new CORSFilter(ALLOWED_ORIGIN);
    ContainerRequestContext request = mockRequest("GET", ALLOWED_ORIGIN);
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    ContainerResponseContext response = mock(ContainerResponseContext.class);
    when(response.getHeaders()).thenReturn(headers);

    filter.filter(request, response);

    Assert.assertEquals(headers.getFirst("Access-Control-Allow-Origin"), ALLOWED_ORIGIN);
    Assert.assertEquals(headers.getFirst("Access-Control-Allow-Credentials"), "true");
    Assert.assertTrue(headers.get("Vary").contains("Origin"));
  }

  @Test
  public void testResponseDisallowedOriginAddsNoHeaders() throws IOException {
    CORSFilter filter = new CORSFilter(ALLOWED_ORIGIN);
    ContainerRequestContext request = mockRequest("GET", DISALLOWED_ORIGIN);
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    ContainerResponseContext response = mock(ContainerResponseContext.class);
    when(response.getHeaders()).thenReturn(headers);

    filter.filter(request, response);

    Assert.assertTrue(headers.isEmpty());
  }

  @Test
  public void testResponseWildcardNeverEmitsCredentials() throws IOException {
    CORSFilter filter = new CORSFilter("*");
    ContainerRequestContext request = mockRequest("GET", DISALLOWED_ORIGIN);
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    ContainerResponseContext response = mock(ContainerResponseContext.class);
    when(response.getHeaders()).thenReturn(headers);

    filter.filter(request, response);

    Assert.assertEquals(headers.getFirst("Access-Control-Allow-Origin"), "*");
    Assert.assertNull(headers.getFirst("Access-Control-Allow-Credentials"));
  }

  @Test
  public void testUnconfiguredDeniesAllOrigins() throws IOException {
    CORSFilter filter = new CORSFilter(null);
    ContainerRequestContext request = mockRequest("GET", ALLOWED_ORIGIN);
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    ContainerResponseContext response = mock(ContainerResponseContext.class);
    when(response.getHeaders()).thenReturn(headers);

    filter.filter(request, response);

    Assert.assertTrue(headers.isEmpty());
  }

  @Test
  public void testPreflightAllowedOriginRestrictsMethods() throws IOException {
    CORSFilter filter = new CORSFilter(ALLOWED_ORIGIN);
    ContainerRequestContext request = mockRequest("OPTIONS", ALLOWED_ORIGIN);
    when(request.getHeaderString("Access-Control-Request-Method")).thenReturn("TRACE");
    when(request.getHeaderString("Access-Control-Request-Headers")).thenReturn("X-Custom");

    Response response = capturePreflight(filter, request);

    Assert.assertEquals(response.getStatus(), 200);
    Assert.assertEquals(response.getHeaderString("Access-Control-Allow-Origin"), ALLOWED_ORIGIN);
    Assert.assertEquals(response.getHeaderString("Access-Control-Allow-Credentials"), "true");
    String allowedMethods = response.getHeaderString("Access-Control-Allow-Methods");
    Assert.assertNotNull(allowedMethods);
    Assert.assertFalse(allowedMethods.contains("TRACE"));
    Assert.assertTrue(allowedMethods.contains("GET"));
    Assert.assertEquals(response.getHeaderString("Access-Control-Allow-Headers"), "X-Custom");
  }

  @Test
  public void testPreflightDisallowedOriginEmitsNoCorsHeaders() throws IOException {
    CORSFilter filter = new CORSFilter(ALLOWED_ORIGIN);
    ContainerRequestContext request = mockRequest("OPTIONS", DISALLOWED_ORIGIN);
    when(request.getHeaderString("Access-Control-Request-Method")).thenReturn("GET");

    Response response = capturePreflight(filter, request);

    Assert.assertNull(response.getHeaderString("Access-Control-Allow-Origin"));
    Assert.assertNull(response.getHeaderString("Access-Control-Allow-Methods"));
    Assert.assertNull(response.getHeaderString("Access-Control-Allow-Credentials"));
  }

  private ContainerRequestContext mockRequest(String method, String origin) {
    ContainerRequestContext request = mock(ContainerRequestContext.class);
    when(request.getMethod()).thenReturn(method);
    when(request.getHeaderString("Origin")).thenReturn(origin);
    return request;
  }

  private Response capturePreflight(CORSFilter filter, ContainerRequestContext request)
      throws IOException {
    filter.filter(request);
    ArgumentCaptor<Response> captor = ArgumentCaptor.forClass(Response.class);
    verify(request).abortWith(captor.capture());
    return captor.getValue();
  }
}
