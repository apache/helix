package org.apache.helix.rest.server.authValidator;

import javax.ws.rs.container.ContainerRequestContext;


public interface AuthValidator {
  boolean validate(ContainerRequestContext request);
}
