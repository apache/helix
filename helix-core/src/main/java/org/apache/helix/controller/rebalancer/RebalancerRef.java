package org.apache.helix.controller.rebalancer;

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

import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Reference to a class that extends {@link HelixRebalancer}. It loads the class automatically.
 */
public class RebalancerRef {
  private static final Logger LOG = Logger.getLogger(RebalancerRef.class);

  @JsonProperty("rebalancerClassName")
  private final String _rebalancerClassName;

  @JsonIgnore
  private Class<? extends HelixRebalancer> _class;

  @JsonCreator
  private RebalancerRef(@JsonProperty("rebalancerClassName") String rebalancerClassName) {
    _rebalancerClassName = rebalancerClassName;
    _class = null;
  }

  /**
   * Get an instantiated HelixRebalancer
   * @return HelixRebalancer or null if instantiation failed
   */
  @JsonIgnore
  public HelixRebalancer getRebalancer() {
    try {
      return (HelixRebalancer) (getRebalancerClass().newInstance());
    } catch (Exception e) {
      LOG.warn("Exception while invoking custom rebalancer class:" + _rebalancerClassName, e);
    }
    return null;
  }

  /**
   * Get the class object of this rebalancer ref
   * @return Class
   */
  @JsonIgnore
  public Class<? extends HelixRebalancer> getRebalancerClass() {
    try {
      if (_class == null) {
        _class =
            HelixUtil.loadClass(getClass(), _rebalancerClassName).asSubclass(HelixRebalancer.class);
      }
    } catch (Exception e) {
      LOG.warn("Exception while loading rebalancer class:" + _rebalancerClassName, e);
    }
    return _class;
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
      return this.toString().equals(that);
    }
    return false;
  }

  /**
   * Get a rebalancer class reference
   * @param rebalancerClassName name of the class
   * @return RebalancerRef or null if name is null
   */
  public static RebalancerRef from(String rebalancerClassName) {
    if (rebalancerClassName == null) {
      return null;
    }
    return new RebalancerRef(rebalancerClassName);
  }

  /**
   * Get a RebalancerRef from a class object
   * @param rebalancerClass class that implements Rebalancer
   * @return RebalancerRef
   */
  public static RebalancerRef from(Class<? extends HelixRebalancer> rebalancerClass) {
    if (rebalancerClass == null) {
      return null;
    }
    return RebalancerRef.from(rebalancerClass.getName());
  }
}
