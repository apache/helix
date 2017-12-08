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

public class HelixRestUtils {
  /**
   * Generate servlet path spec for a given namespace.
   * @param namespace Name of the namespace
   * @param isDefaultServlet mark this as true to get path spec for the special servlet for default namespace
   * @return servlet path spec
   */
  public static String makeServletPathSpec(String namespace, boolean isDefaultServlet) {
    return isDefaultServlet ? HelixRestNamespace.DEFAULT_NAMESPACE_PATH_SPEC
        : String.format("/namespaces/%s/*", namespace);
  }

  /**
   * Extract namespace information from servlet path. There are 3 cases:
   *  1. /namespaces/namespaceName  ->  return namespaceName
   *  2. /namespaces                ->  return ""
   *  3. this is special servlet for default namespace  ->  return the reserved name for default namespace
   * @param servletPath servletPath
   * @return Namespace name retrieved from servlet spec.
   */
  public static String getNamespaceFromServletPath(String servletPath) {
    if (isDefaultNamespaceServlet(servletPath)) {
      return HelixRestNamespace.DEFAULT_NAMESPACE_NAME;
    }

    String namespaceName = servletPath.replace("/namespaces", "");
    if (namespaceName.isEmpty() || namespaceName.equals("/")) {
      return "";
    } else {
      return namespaceName.replace("/", "");
    }
  }

  private static boolean isDefaultNamespaceServlet(String servletPath) {
    // Special servlet for default namespace has path spec "/*", so servletPath is empty
    return servletPath == null || servletPath.isEmpty();
  }

}
