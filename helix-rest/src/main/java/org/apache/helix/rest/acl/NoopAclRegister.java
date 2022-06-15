package org.apache.helix.rest.acl;

import javax.servlet.http.HttpServletRequest;


public class NoopAclRegister implements AclRegister{

  public void createACL(HttpServletRequest request) {

  }
}
