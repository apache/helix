package org.apache.helix.rest.acl;

import javax.servlet.http.HttpServletRequest;


public interface AclRegister {
  void createACL(HttpServletRequest request);
}
