package org.apache.helix.rest.common;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.rest.server.resources.helix.AbstractHelixResource;
import org.apache.helix.rest.server.resources.metadata.NamespacesAccessor;

public enum ServletType {
  /**
   * Servlet serving default API endpoints (/admin/v2/clusters/...)
   */
  DEFAULT_SERVLET(HelixRestNamespace.DEFAULT_NAMESPACE_PATH_SPEC, new String[] {
      AbstractHelixResource.class.getPackage().getName(),
      NamespacesAccessor.class.getPackage().getName()
  }),

  /**
   * Servlet serving namespaced API endpoints (/admin/v2/namespaces/{namespaceName})
   */
  COMMON_SERVLET("/namespaces/%s/*", new String[] {
      AbstractHelixResource.class.getPackage().getName(),
  });

  private final String _servletPathSpecTemplate;
  private final String[] _servletPackageArray;

  ServletType(String servletPathSpecTemplate, String[] servletPackageArray) {
    _servletPathSpecTemplate = servletPathSpecTemplate;
    _servletPackageArray = servletPackageArray;
  }

  public String getServletPathSpecTemplate() {
    return _servletPathSpecTemplate;
  }

  public String getServletPath(HelixRestNamespace namespace) {
    return String.format(_servletPathSpecTemplate, namespace.getName());
  }

  public String[] getServletPackageArray() {
    return _servletPackageArray;
  }
}
