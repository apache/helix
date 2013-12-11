package org.apache.helix.controller.provisioner;

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
 * Reference to a class that extends {@link Provisioner}. It loads the class automatically.
 */
public class ProvisionerRef {
  private static final Logger LOG = Logger.getLogger(ProvisionerRef.class);

  @JsonProperty("provisionerClassName")
  private final String _provisionerClassName;
  @JsonIgnore
  private Provisioner _provisioner;

  @JsonCreator
  private ProvisionerRef(@JsonProperty("provisionerClassName") String provisionerClassName) {
    _provisionerClassName = provisionerClassName;
    _provisioner = null;
  }

  /**
   * Get an instantiated Provisioner
   * @return Provisioner or null if instantiation failed
   */
  @JsonIgnore
  public Provisioner getProvisioner() {
    if (_provisioner == null) {
      try {
        _provisioner =
            (Provisioner) (HelixUtil.loadClass(getClass(), _provisionerClassName).newInstance());
      } catch (Exception e) {
        LOG.warn("Exception while invoking custom provisioner class:" + _provisionerClassName, e);
      }
    }
    return _provisioner;
  }

  @Override
  public String toString() {
    return _provisionerClassName;
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof ProvisionerRef) {
      return this.toString().equals(((ProvisionerRef) that).toString());
    } else if (that instanceof String) {
      return this.toString().equals(that);
    }
    return false;
  }

  /**
   * Get a provisioner class reference
   * @param provisionerClassName name of the class
   * @return ProvisionerRef or null if name is null
   */
  public static ProvisionerRef from(String provisionerClassName) {
    if (provisionerClassName == null) {
      return null;
    }
    return new ProvisionerRef(provisionerClassName);
  }

  /**
   * Get a ProvisionerRef from a class object
   * @param provisionerClass class that implements Provisioner
   * @return ProvisionerRef
   */
  public static ProvisionerRef from(Class<? extends Provisioner> provisionerClass) {
    if (provisionerClass == null) {
      return null;
    }
    return ProvisionerRef.from(provisionerClass.getName());
  }
}
