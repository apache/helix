package org.apache.helix.rest.server.util;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Simplify the REST URI construction for Jersey Test Framework
 * Example usage:
 *  new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=disable")
 *         .format(CLUSTER_NAME, INSTANCE_NAME)
 *         .post(...);
 */
public class JerseyUriRequestBuilder {
  private static final String PLACE_HOLDER = "{}";

  private final StringBuilder _uriBuilder;
  private final Map<String, String> _queryParams;
  private final int _requiredParameters;
  private final String _rawQuery;
  // default expected status code and if body returned
  private int _expectedStatusCode = Response.Status.OK.getStatusCode();
  private boolean _isBodyReturnExpected = false;

  public JerseyUriRequestBuilder(String uri) {
    String[] uris = uri.split("\\?");
    if (uris.length > 1) {
      _queryParams = Splitter.on('&').trimResults().withKeyValueSeparator("=").split(uris[1]);
      _rawQuery = uris[1];
    } else {
      _queryParams = new HashMap<>();
      _rawQuery = "";
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

  public JerseyUriRequestBuilder expectedReturnStatusCode(int expectedStatusCode) {
    _expectedStatusCode = expectedStatusCode;
    return this;
  }

  public JerseyUriRequestBuilder isBodyReturnExpected(boolean isBodyReturnExpected) {
    _isBodyReturnExpected = isBodyReturnExpected;
    return this;
  }

  /**
   * Execute get request
   * @param container
   * @return
   */
  public String get(JerseyTestNg.ContainerPerClassTest container) {
    final Response response = buildWebTarget(container).request().get();

    Assert.assertEquals(response.getStatus(), _expectedStatusCode);

    // NOT_FOUND will throw text based html
    if (_expectedStatusCode != Response.Status.NOT_FOUND.getStatusCode()) {
      Assert.assertEquals(response.getMediaType().getType(), "application");
    } else {
      Assert.assertEquals(response.getMediaType().getType(), "text");
    }

    String body = response.readEntity(String.class);
    if (_isBodyReturnExpected) {
      Assert.assertNotNull(body);
    }

    return body;
  }

  public Response getResponse(JerseyTestNg.ContainerPerClassTest container) {
    return buildWebTarget(container).request().get();
  }

  /**
   * Execute put request
   * @param container
   * @param entity
   */
  public void put(JerseyTestNg.ContainerPerClassTest container, Entity entity) {
    final Response response = buildWebTarget(container).request().put(entity);
    Assert.assertEquals(response.getStatus(), _expectedStatusCode);
  }

  /**
   * Execute post request
   * @param container
   * @param entity
   */
  public Response post(JerseyTestNg.ContainerPerClassTest container, Entity entity) {
    final Response response = buildWebTarget(container).request().post(entity);
    Assert.assertEquals(response.getStatus(), _expectedStatusCode);
    return response;
  }

  /**
   * Execute delete request
   * @param container
   */
  public void delete(JerseyTestNg.ContainerPerClassTest container) {
    final Response response = buildWebTarget(container).request().delete();
    Assert.assertEquals(response.getStatus(), _expectedStatusCode);
  }

  private WebTarget buildWebTarget(JerseyTestNg.ContainerPerClassTest container) {
    WebTarget webTarget = container.target(_uriBuilder.toString());
    for (Map.Entry<String, String> entry : _queryParams.entrySet()) {
      webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
    }

    return webTarget;
  }

  private String getPath() {
    if (StringUtils.isEmpty(_rawQuery)) {
      return _uriBuilder.toString();
    } else {
      return _uriBuilder.toString() + "?" + _rawQuery;
    }
  }

  @Test
  public void testUriBuilderGetPath() {
    JerseyUriRequestBuilder uriBuilder = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=disable")
        .format("TEST-CLUSTER", "instance1");
    String path = uriBuilder.getPath();
    Assert.assertEquals(uriBuilder._uriBuilder.toString(), "clusters/TEST-CLUSTER/instances/instance1");
    Assert.assertEquals(path, "clusters/TEST-CLUSTER/instances/instance1?command=disable");
    Assert.assertEquals(uriBuilder._queryParams.get("command"), "disable");
  }
}
