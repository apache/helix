package org.apache.helix.cloud.azure;

import javax.net.ssl.SSLException;
import org.apache.helix.HelixCloudProperty;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A dedicated http client for retrieving information from Azure Instance Metadata Service
 */
class AzureHttpUtil {

  private static Logger LOG = LoggerFactory.getLogger(AzureHttpUtil.class.getName());

  static CloseableHttpClient getHttpClient(HelixCloudProperty helixCloudProperty) {
    return getHttpClient(helixCloudProperty.getCloudRequestTimeout(), helixCloudProperty);
  }

  private static CloseableHttpClient getHttpClient(long requestTimeout,
      HelixCloudProperty helixCloudProperty) {
    RequestConfig config = RequestConfig.custom().setConnectionRequestTimeout((int) requestTimeout)
        .setConnectTimeout((int) helixCloudProperty.getCloudConnectionTimeout()).build();
    return HttpClients.custom().setDefaultRequestConfig(config)
        .setRetryHandler(getRetryHandler(helixCloudProperty)).build();
  }

  private static HttpRequestRetryHandler getRetryHandler(HelixCloudProperty helixCloudProperty) {
    return (IOException exception, int executionCount, HttpContext context) -> {
      LOG.warn("Execution count: " + executionCount + ".", exception);
      return !(executionCount >= helixCloudProperty.getCloudMaxRetry()
          || exception instanceof InterruptedIOException
          || exception instanceof UnknownHostException || exception instanceof SSLException);
    };
  }
}
