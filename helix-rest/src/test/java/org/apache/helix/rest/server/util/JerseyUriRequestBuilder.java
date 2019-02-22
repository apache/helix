package org.apache.helix.rest.server.util;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;


public class JerseyUriRequestBuilder {
  private static final String PLACE_HOLDER = "{}";

  private final StringBuilder _uriBuilder;
  private final Map<String, String> _queryParams;
  private final int _requiredParameters;
  private final String _query;

  public JerseyUriRequestBuilder(String uri) {
    String[] uris = uri.split("\\?");
    if (uris.length > 1) {
      _queryParams = Splitter.on('&').trimResults().withKeyValueSeparator("=").split(uris[1]);
      _query = uris[1];
    } else {
      _queryParams = new HashMap<>();
      _query = "";
    }
    _uriBuilder = new StringBuilder(uris[0]);
    _requiredParameters = StringUtils.countMatches(uris[0], PLACE_HOLDER);
  }

  public JerseyUriRequestBuilder format(String... parameters) {
    Preconditions.checkArgument(_requiredParameters == parameters.length);
    for (String param : parameters) {
      int index = _uriBuilder.indexOf(PLACE_HOLDER);
      _uriBuilder.replace(index, index + PLACE_HOLDER.length(), param);
    }

    return this;
  }

  private WebTarget buildWebTarget(JerseyTestNg.ContainerPerClassTest container) {
    WebTarget webTarget = container.target(_uriBuilder.toString());
    for (Map.Entry<String, String> entry : _queryParams.entrySet()) {
      webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
    }

    return webTarget;
  }

  public String get(JerseyTestNg.ContainerPerClassTest container, int expectedReturnStatus, boolean expectBodyReturned) {
    final Response response = buildWebTarget(container).request().get();

    Assert.assertEquals(response.getStatus(), expectedReturnStatus);

    // NOT_FOUND will throw text based html
    if (expectedReturnStatus != Response.Status.NOT_FOUND.getStatusCode()) {
      Assert.assertEquals(response.getMediaType().getType(), "application");
    } else {
      Assert.assertEquals(response.getMediaType().getType(), "text");
    }

    String body = response.readEntity(String.class);
    if (expectBodyReturned) {
      Assert.assertNotNull(body);
    }

    return body;
  }

  public String get(JerseyTestNg.ContainerPerClassTest container) {
    final Response response = buildWebTarget(container).request().get();

    return response.readEntity(String.class);
  }

  public String getPath() {
    if (StringUtils.isEmpty(_query)) {
      return _uriBuilder.toString();
    } else {
      return _uriBuilder.toString() + "?" + _query;
    }
  }

  @Test
  public void testUriBuilderGetPath() {
    JerseyUriRequestBuilder uriBuilder = new JerseyUriRequestBuilder("clusters/{}/instances/{}/messages?stateModelDef=MasterSlave")
        .format("TEST-CLUSTER", "instance1");
    String path = uriBuilder.getPath();
    Assert.assertEquals(path, "clusters/TEST-CLUSTER/instances/instance1/messages?stateModelDef=MasterSlave");
    Assert.assertEquals(uriBuilder._queryParams.get("stateModelDef"), "MasterSlave");
  }
}
