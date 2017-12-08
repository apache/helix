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

public class HelixRestNamespace {

  public enum HelixMetadataStoreType {
    ZOOKEEPER
  }

  public enum HelixRestNamespaceProperty {
    NAME,
    METADATA_STORE_TYPE,
    METADATA_STORE_ADDRESS,
    IS_DEFAULT
  }

  /**
   * Namespaced object will have path such as /url_prefix/namespaces/{namespace_name}/clusters/...
   * We are going to have path /url_prefix/clusters/... point to default namespace if there is one
   */
  public static final String DEFAULT_NAMESPACE_PATH_SPEC = "/*";
  public static final String DEFAULT_NAMESPACE_NAME = "default";

  /**
   * Name of Helix namespace
   */
  private String _name;

  /**
   * Type of a metadata store that belongs to Helix namespace
   */
  private HelixMetadataStoreType _metadataStoreType;

  /**
   * Address of metadata store. Should be informat of
   * "[ip-address]:[port]" or "[dns-name]:[port]"
   */
  private String _metadataStoreAddress;

  /**
   * Flag indicating whether this namespace is default or not
   */
  private boolean _isDefault;

  public HelixRestNamespace(String metadataStoreAddress) throws IllegalArgumentException {
    this(DEFAULT_NAMESPACE_NAME, HelixMetadataStoreType.ZOOKEEPER, metadataStoreAddress, true);
  }

  public HelixRestNamespace(String name, HelixMetadataStoreType metadataStoreType, String metadataStoreAddress, boolean isDefault)
      throws IllegalArgumentException {
    _name = name;
    _metadataStoreAddress = metadataStoreAddress;
    _metadataStoreType = metadataStoreType;
    _isDefault = isDefault;
    validate();
  }

  private void validate() throws IllegalArgumentException {
    // TODO: add more strict validation for NAME as this will be part of URL
    if (_name == null || _name.length() == 0) {
      throw new IllegalArgumentException("Name of namespace not provided");
    }
    if (_metadataStoreAddress == null || _metadataStoreAddress.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Metadata store address \"%s\" is not valid for namespace %s", _metadataStoreAddress, _name));
    }
  }

  public boolean isDefault() {
    return _isDefault;
  }

  public String getName() {
    return _name;
  }

  public String getMetadataStoreAddress() {
    return _metadataStoreAddress;
  }

}
