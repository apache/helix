package org.apache.helix.ui.resource;

import io.dropwizard.jersey.caching.CacheControl;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/ping")
@Produces(MediaType.APPLICATION_JSON)
public class PingResource {
  @GET
  @CacheControl(mustRevalidate = true, noCache = true, noStore = true)
  public Response health() {
    return Response.status(Response.Status.OK).entity("pong").build();
  }
}
