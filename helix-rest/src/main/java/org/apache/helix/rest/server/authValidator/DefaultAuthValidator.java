package org.apache.helix.rest.server.authValidator;

import javax.ws.rs.container.ContainerRequestContext;


public class DefaultAuthValidator implements AuthValidator {
  public boolean validate(ContainerRequestContext request) {
    return true;
  }
}
