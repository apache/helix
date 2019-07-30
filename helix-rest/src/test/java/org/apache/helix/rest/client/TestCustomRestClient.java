package org.apache.helix.rest.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.junit.Assert;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class TestCustomRestClient {
  private static final Logger LOG = Logger.getLogger(TestCustomRestClient.class.getName());
  private static final String HTTP_LOCALHOST = "http://localhost:1000";
  static {
    LOG.setLevel(Level.INFO);
  }
  @Mock
  HttpClient _httpClient;

  @BeforeMethod
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testGetInstanceStoppableCheck() throws IOException {
    MockCustomRestClient customRestClient = new MockCustomRestClient(_httpClient);
    String jsonResponse = "{\n" + "   \"check1\": \"false\",\n" + "   \"check2\": \"true\"\n" + "}";

    HttpResponse httpResponse = mock(HttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);

    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    customRestClient.setJsonResponse(jsonResponse);
    when(_httpClient.execute(any(HttpPost.class))).thenReturn(httpResponse);

    Map<String, Boolean> healthCheck =
        customRestClient.getInstanceStoppableCheck(HTTP_LOCALHOST, Collections.emptyMap());
    Assert.assertFalse(healthCheck.get("check1"));
    Assert.assertTrue(healthCheck.get("check2"));
  }

  @Test(expectedExceptions = ClientProtocolException.class)
  public void testGetInstanceStoppableCheck_when_url_404() throws IOException {
    MockCustomRestClient customRestClient = new MockCustomRestClient(_httpClient);
    HttpResponse httpResponse = mock(HttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);

    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(_httpClient.execute(any(HttpPost.class))).thenReturn(httpResponse);

    customRestClient.getInstanceStoppableCheck(HTTP_LOCALHOST, Collections.emptyMap());
  }

  @Test(expectedExceptions = IOException.class)
  public void testGetInstanceStoppableCheck_when_response_empty() throws IOException {
    MockCustomRestClient customRestClient = new MockCustomRestClient(_httpClient);
    HttpResponse httpResponse = mock(HttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);

    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(_httpClient.execute(any(HttpPost.class))).thenReturn(httpResponse);
    customRestClient.setJsonResponse("");

    customRestClient.getInstanceStoppableCheck(HTTP_LOCALHOST, Collections.emptyMap());
  }

  @Test
  public void testGetPartitionStoppableCheck() throws IOException {
    MockCustomRestClient customRestClient = new MockCustomRestClient(_httpClient);
    String jsonResponse = "\n" + "{\n" + "   \"db1\": {\n" + "      \"IS_HEALTHY\": \"false\"\n"
        + "   },\n" + "   \"db0\": {\n" + "      \"IS_HEALTHY\": \"true\"\n" + "   }\n" + "}";

    HttpResponse httpResponse = mock(HttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);

    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    customRestClient.setJsonResponse(jsonResponse);
    when(_httpClient.execute(any(HttpPost.class))).thenReturn(httpResponse);

    Map<String, Boolean> partitionHealth = customRestClient.getPartitionStoppableCheck(HTTP_LOCALHOST,
        ImmutableList.of("db0", "db1"), Collections.emptyMap());

    Assert.assertTrue(partitionHealth.get("db0"));
    Assert.assertFalse(partitionHealth.get("db1"));
  }

  @Test(expectedExceptions = ClientProtocolException.class)
  public void testGetPartitionStoppableCheck_when_url_404() throws IOException {
    MockCustomRestClient customRestClient = new MockCustomRestClient(_httpClient);

    HttpResponse httpResponse = mock(HttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);

    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(_httpClient.execute(any(HttpPost.class))).thenReturn(httpResponse);

    customRestClient.getPartitionStoppableCheck(HTTP_LOCALHOST,
        ImmutableList.of("db0", "db1"), Collections.emptyMap());
  }

  @Test(expectedExceptions = IOException.class)
  public void testGetPartitionStoppableCheck_when_response_empty() throws IOException {
    MockCustomRestClient customRestClient = new MockCustomRestClient(_httpClient);
    HttpResponse httpResponse = mock(HttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);

    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(_httpClient.execute(any(HttpPost.class))).thenReturn(httpResponse);
    customRestClient.setJsonResponse("");

    customRestClient.getPartitionStoppableCheck(HTTP_LOCALHOST,
        ImmutableList.of("db0", "db1"), Collections.emptyMap());
  }

  @Test (description = "Validate if the post request has the correct format")
  public void testPostRequestFormat() throws IOException {
    // a popular echo server that echos all the inputs
    // TODO: add a mock rest server
    final String echoServer = "http://httpbin.org/post";
    CustomRestClientImpl customRestClient = new CustomRestClientImpl(HttpClients.createDefault());
    HttpResponse response = customRestClient.post(echoServer, Collections.emptyMap());
    JsonNode json = customRestClient.getJsonObject(response);

    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
    Assert.assertEquals(json.get("headers").get("Accept").asText(), "application/json");
    Assert.assertEquals(json.get("data").asText(), "{}");
  }

  @Test (description = "Validate same http requests will get deduped and only be sent at most once")
  public void testGetPartitionsStoppableCheckSameHttpRequestsShouldDedupe()
      throws IOException, InterruptedException {
    MockCustomRestClient customRestClient = new MockCustomRestClient(_httpClient);
    String jsonResponse = "{\"test_partition\":{\"IS_HEALTHY\": \"false\"}}";

    customRestClient.setJsonResponse(jsonResponse);
    HttpResponse httpResponse = mock(HttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);

    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(_httpClient.execute(any(HttpPost.class))).thenAnswer(new Answer<HttpResponse>() {
      @Override
      public HttpResponse answer(InvocationOnMock invocation) {
        try {
          // intentionally delay the response and verify if same requests will wait for 1st to return
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.info("Ignore the exception in test");
        }
        return httpResponse;
      }
    });

    Runnable runnable = () -> {
      try {
        LOG.info("Executing " + Thread.currentThread().getName());
        customRestClient.getPartitionStoppableCheck(HTTP_LOCALHOST, Collections.emptyList(), Collections.emptyMap());
      } catch (IOException e) {
        LOG.info("Ignore the exception in test");
      }
    };
    long startTime = System.currentTimeMillis();
    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(runnable, "Thread: " + i);
      threads[i].start();
    }
    for (Thread thread : threads) {
      thread.join();
    }

    verify(_httpClient, times(1)).execute(any());
    Assert.assertEquals(customRestClient.getCachedResults().size(), 1);
    LOG.info("Elapsed time: " + (System.currentTimeMillis() - startTime));
  }

  @Test (description = "Validate different http requests will not get deduped nor wait for each other")
  public void testGetPartitionsStoppableCheckDifferentHttpRequests()
      throws IOException, InterruptedException {
    MockCustomRestClient customRestClient = new MockCustomRestClient(_httpClient);

    HttpResponse httpResponse = mock(HttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);

    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(_httpClient.execute(any(HttpPost.class))).thenAnswer(new Answer<HttpResponse>() {
      @Override
      public HttpResponse answer(InvocationOnMock invocation) {
        try {
          // intentionally delay the response and verify if different requests will not wait for each other
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.info("Ignore the exception in test");
        }
        return httpResponse;
      }
    });

    Runnable runnable = () -> {
      try {
        String threadName = Thread.currentThread().getName();
        LOG.info("Executing " + threadName);
        String jsonResponse = String.format("{\"%s\":{\"IS_HEALTHY\": \"false\"}}", threadName);
        customRestClient.setJsonResponse(jsonResponse);
        customRestClient.getPartitionStoppableCheck(HTTP_LOCALHOST, ImmutableList.of(threadName), Collections.emptyMap());
      } catch (IOException e) {
        LOG.info("Ignore the exception in test");
      }
    };
    long startTime = System.currentTimeMillis();
    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(runnable, "Thread: " + i);
      threads[i].start();
    }
    for (Thread thread : threads) {
      thread.join();
    }

    verify(_httpClient, times(10)).execute(any());
    // Cached map has size of 10, and each value is different
    Assert.assertEquals(customRestClient.getCachedResults().values().size(), 10);
    // The total elapsed time should be around 1 second
    LOG.info("Elapsed time: " + (System.currentTimeMillis() - startTime));
  }

  private class MockCustomRestClient extends CustomRestClientImpl {
    private String _jsonResponse = "{}";

    MockCustomRestClient(HttpClient mockHttpClient) {
      super(mockHttpClient);
    }

    synchronized void setJsonResponse(String response) {
      _jsonResponse = response;
    }

    @Override
    protected JsonNode getJsonObject(HttpResponse httpResponse) throws IOException {
      return new ObjectMapper().readTree(_jsonResponse);
    }
  }
}
