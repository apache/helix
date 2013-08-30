package org.apache.helix.api;

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

import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.util.HelixUtil;

public class RebalancerRef {
  private final String _rebalancerClassName;

  public RebalancerRef(String rebalancerClassName) {
    _rebalancerClassName = rebalancerClassName;
  }

  /**
   * @return
   */
  public Rebalancer getRebalancer() {
    try {
      return (Rebalancer) (HelixUtil.loadClass(getClass(), _rebalancerClassName).newInstance());
    } catch (InstantiationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public String toString() {
    return _rebalancerClassName;
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof RebalancerRef) {
      return this.toString().equals(((RebalancerRef) that).toString());
    } else if (that instanceof String) {
      return this.toString().equals((String) that);
    }
    return false;
  }

  /**
   * Get a rebalancer class reference
   * @param rebalancerClassName name of the class
   * @return RebalancerRef
   */
  public static RebalancerRef from(String rebalancerClassName) {
    return new RebalancerRef(rebalancerClassName);
  }
}
