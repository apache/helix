package org.apache.helix.rest.server.resources.zookeeper;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.HelixRestUtils;
import org.apache.helix.rest.metadatastore.MetadataStoreDirectory;
import org.apache.helix.rest.metadatastore.ZkMetadataStoreDirectory;
import org.apache.helix.rest.metadatastore.constant.MetadataStoreDirectoryConstants;
import org.apache.helix.rest.metadatastore.exceptions.InvalidRoutingDataException;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides REST endpoints for accessing metadata store directory service,
 * which responds to read/write requests of metadata store realms, sharding keys, etc..
 */
@Path("")
public class MetadataStoreDirectoryAccessor extends AbstractResource {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataStoreDirectoryAccessor.class);

  private HelixRestNamespace _namespace;
  private MetadataStoreDirectory _metadataStoreDirectory;

  /**
   * Gets all metadata store realms in a namespace with the endpoint.
   *
   * @return Json representation of all realms.
   */
  @GET
  @Path("/metadata-store-realms")
  public Response getAllMetadataStoreRealms() {
    Map<String, Collection<String>> responseMap = new HashMap<>(1);
    try {
      Collection<String> realms =
          getMetadataStoreDirectory().getAllMetadataStoreRealms(getHelixNamespace().getName());
      responseMap.put(
          MetadataStoreDirectoryConstants.METADATA_STORE_REALMS_NAME, realms);
    } catch (NoSuchElementException ex) {
      return notFound(ex.getMessage());
    }

    return JSONRepresentation(responseMap);
  }

  /**
   * Gets sharding keys mapped at path "HTTP GET /sharding-keys" which returns all sharding keys in
   * a namespace, or path "HTTP GET /sharding-keys?realm={realmName}" which returns sharding keys in
   * a realm.
   *
   * @param realm Query param in endpoint path
   * @return Json representation of a map: shardingKeys -> collection of sharding keys.
   */
  @GET
  @Path("/sharding-keys")
  public Response getShardingKeys(@QueryParam("realm") String realm) {
    Map<String, Object> responseMap;
    Collection<String> shardingKeys;
    try {
      // If realm is not set in query param, the endpoint is: "/sharding-keys"
      // to get all sharding keys in a namespace.
      if (realm == null) {
        shardingKeys =
            getMetadataStoreDirectory().getAllShardingKeys(getHelixNamespace().getName());
        // To avoid allocating unnecessary resource, limit the map's capacity only for
        // SHARDING_KEYS_NAME.
        responseMap = new HashMap<>(1);
      } else {
        // For endpoint: "/sharding-keys?realm={realmName}"
        shardingKeys = getMetadataStoreDirectory()
            .getAllShardingKeysInRealm(getHelixNamespace().getName(), realm);
        // To avoid allocating unnecessary resource, limit the map's capacity only for
        // SHARDING_KEYS_NAME and "metadataStoreRealm".
        responseMap = new HashMap<>(2);
        responseMap.put("metadataStoreRealm", realm);
      }
    } catch (NoSuchElementException ex) {
      return notFound(ex.getMessage());
    }

    responseMap.put(MetadataStoreDirectoryConstants.SHARDING_KEYS_NAME, shardingKeys);

    return JSONRepresentation(responseMap);
  }

  private MetadataStoreDirectory getMetadataStoreDirectory() {
    if (_metadataStoreDirectory == null) {
      synchronized (this) {
        if (_metadataStoreDirectory == null) {
          Map<String, String> routingZkAddressMap = ImmutableMap
              .of(getHelixNamespace().getName(), getHelixNamespace().getMetadataStoreAddress());
          try {
            _metadataStoreDirectory = new ZkMetadataStoreDirectory(routingZkAddressMap);
          } catch (InvalidRoutingDataException ex) {
            // In this case, the InvalidRoutingDataException should not happen because routing
            // ZK address is always valid here.
            LOG.warn("Unable to create metadata store directory for routing address: {}",
                routingZkAddressMap, ex);
          }
        }
      }
    }

    return _metadataStoreDirectory;
  }

  private HelixRestNamespace getHelixNamespace() {
    if (_namespace == null) {
      // A default servlet does not have context property key METADATA, so the namespace
      // is retrieved from property ALL_NAMESPACES.
      if (HelixRestUtils.isDefaultServlet(_servletRequest.getServletPath())) {
        // It is safe to ignore uncheck warnings for this cast.
        @SuppressWarnings("unchecked")
        List<HelixRestNamespace> namespaces =
            (List<HelixRestNamespace>) _application.getProperties()
                .get(ContextPropertyKeys.ALL_NAMESPACES.name());
        for (HelixRestNamespace ns : namespaces) {
          if (HelixRestNamespace.DEFAULT_NAMESPACE_NAME.equals(ns.getName())) {
            _namespace = ns;
            break;
          }
        }
      } else {
        // Get namespace from property METADATA for a common servlet.
        _namespace = (HelixRestNamespace) _application.getProperties()
            .get(ContextPropertyKeys.METADATA.name());
      }
    }

    return _namespace;
  }
}
