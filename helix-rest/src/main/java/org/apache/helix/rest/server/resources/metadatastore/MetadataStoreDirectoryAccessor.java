package org.apache.helix.rest.server.resources.metadatastore;

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
import javax.annotation.PostConstruct;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.HelixRestUtils;
import org.apache.helix.rest.metadatastore.MetadataStoreDirectory;
import org.apache.helix.rest.metadatastore.ZkMetadataStoreDirectory;
import org.apache.helix.rest.metadatastore.constant.MetadataStoreRoutingConstants;
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

  private String _namespace;
  private MetadataStoreDirectory _metadataStoreDirectory;

  @PostConstruct
  private void postConstruct() {
    HelixRestNamespace helixRestNamespace = getHelixNamespace();
    _namespace = helixRestNamespace.getName();

    buildMetadataStoreDirectory(_namespace, helixRestNamespace.getMetadataStoreAddress());
  }

  /**
   * Gets all metadata store realms in a namespace with the endpoint.
   *
   * @return Json representation of all realms.
   */
  @GET
  @Path("/metadata-store-realms")
  public Response getAllMetadataStoreRealms() {
    Map<String, Collection<String>> responseMap;
    try {
      Collection<String> realms = _metadataStoreDirectory.getAllMetadataStoreRealms(_namespace);

      responseMap = new HashMap<>(1);
      responseMap.put(MetadataStoreRoutingConstants.METADATA_STORE_REALMS, realms);
    } catch (NoSuchElementException ex) {
      return notFound(ex.getMessage());
    }

    return JSONRepresentation(responseMap);
  }

  @PUT
  @Path("/metadata-store-realms/{realm}")
  public Response addMetadataStoreRealm(@PathParam("realm") String realm) {
    try {
      _metadataStoreDirectory.addMetadataStoreRealm(_namespace, realm);
    } catch (IllegalArgumentException ex) {
      return notFound(ex.getMessage());
    }

    return created();
  }

  @DELETE
  @Path("/metadata-store-realms/{realm}")
  public Response deleteMetadataStoreRealm(@PathParam("realm") String realm) {
    try {
      _metadataStoreDirectory.deleteMetadataStoreRealm(_namespace, realm);
    } catch (IllegalArgumentException ex) {
      return notFound(ex.getMessage());
    }

    return OK();
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
        shardingKeys = _metadataStoreDirectory.getAllShardingKeys(_namespace);
        // To avoid allocating unnecessary resource, limit the map's capacity only for
        // SHARDING_KEYS.
        responseMap = new HashMap<>(1);
      } else {
        // For endpoint: "/sharding-keys?realm={realmName}"
        shardingKeys = _metadataStoreDirectory.getAllShardingKeysInRealm(_namespace, realm);
        // To avoid allocating unnecessary resource, limit the map's capacity only for
        // SHARDING_KEYS and SINGLE_METADATA_STORE_REALM.
        responseMap = new HashMap<>(2);
        responseMap.put(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM, realm);
      }
    } catch (NoSuchElementException ex) {
      return notFound(ex.getMessage());
    }

    responseMap.put(MetadataStoreRoutingConstants.SHARDING_KEYS, shardingKeys);

    return JSONRepresentation(responseMap);
  }

  @PUT
  @Path("/metadata-store-realms/{realm}/sharding-keys/{sharding-key: .+}")
  public Response addShardingKey(@PathParam("realm") String realm,
      @PathParam("sharding-key") String shardingKey) {
    try {
      _metadataStoreDirectory.addShardingKey(_namespace, realm, shardingKey);
    } catch (IllegalArgumentException ex) {
      return notFound(ex.getMessage());
    }

    return created();
  }

  @DELETE
  @Path("/metadata-store-realms/{realm}/sharding-keys/{sharding-key: .+}")
  public Response deleteShardingKey(@PathParam("realm") String realm,
      @PathParam("sharding-key") String shardingKey) {
    try {
      _metadataStoreDirectory.deleteShardingKey(_namespace, realm, shardingKey);
    } catch (IllegalArgumentException ex) {
      return notFound(ex.getMessage());
    }

    return OK();
  }

  private HelixRestNamespace getHelixNamespace() {
    HelixRestNamespace helixRestNamespace = null;
    // A default servlet does not have context property key METADATA, so the namespace
    // is retrieved from property ALL_NAMESPACES.
    if (HelixRestUtils.isDefaultServlet(_servletRequest.getServletPath())) {
      // It is safe to ignore uncheck warnings for this cast.
      @SuppressWarnings("unchecked")
      List<HelixRestNamespace> namespaces = (List<HelixRestNamespace>) _application.getProperties()
          .get(ContextPropertyKeys.ALL_NAMESPACES.name());
      for (HelixRestNamespace ns : namespaces) {
        if (HelixRestNamespace.DEFAULT_NAMESPACE_NAME.equals(ns.getName())) {
          helixRestNamespace = ns;
          break;
        }
      }
    } else {
      // Get namespace from property METADATA for a common servlet.
      helixRestNamespace = (HelixRestNamespace) _application.getProperties()
          .get(ContextPropertyKeys.METADATA.name());
    }

    return helixRestNamespace;
  }

  private void buildMetadataStoreDirectory(String namespace, String address) {
    Map<String, String> routingZkAddressMap = ImmutableMap.of(namespace, address);
    try {
      _metadataStoreDirectory = new ZkMetadataStoreDirectory(routingZkAddressMap);
    } catch (InvalidRoutingDataException ex) {
      // In this case, the InvalidRoutingDataException should not happen because routing
      // ZK address is always valid here.
      LOG.warn("Unable to create metadata store directory for routing ZK address: {}",
          routingZkAddressMap, ex);
    }
  }
}
