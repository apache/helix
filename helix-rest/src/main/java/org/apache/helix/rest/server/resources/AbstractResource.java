package org.apache.helix.rest.server.resources;

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

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.server.ServerContext;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.SerializationConfig;

public class AbstractResource {
  @Context
  private Application _application;

  private ZkClient _zkClient;

  protected ZkClient getZkClient() {
    if (_zkClient == null) {
      ServerContext serverContext = (ServerContext) _application.getProperties()
          .get(ContextPropertyKeys.SERVER_CONTEXT.name());
      _zkClient = serverContext.getZkClient();
    }
    return _zkClient;
  }

  protected HelixAdmin getHelixAdmin () {
    ZkClient zkClient = getZkClient();
    HelixAdmin helixAdmin = new ZKHelixAdmin(zkClient);
    return helixAdmin;
  }

  protected ConfigAccessor getConfigAccessor () {
    ZkClient zkClient = getZkClient();
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    return configAccessor;
  }

  protected static String toJson(ZNRecord record)
      throws IOException {
    return ObjectToJson(record);
  }

  protected static String toJson(Map<String, ?> dataMap)
      throws IOException {
    return ObjectToJson(dataMap);
  }

  protected static String ObjectToJson(Object object)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, object);

    return sw.toString();
  }
}
