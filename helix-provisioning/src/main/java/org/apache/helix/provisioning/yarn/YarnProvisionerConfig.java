package org.apache.helix.provisioning.yarn;

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

import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.provisioner.ProvisionerRef;
import org.apache.helix.controller.serializer.DefaultStringSerializer;
import org.apache.helix.controller.serializer.StringSerializer;
import org.codehaus.jackson.annotate.JsonProperty;

public class YarnProvisionerConfig implements ProvisionerConfig {

  private ResourceId _resourceId;
  private Class<? extends StringSerializer> _serializerClass;
  private ProvisionerRef _provisionerRef;
  private Integer _numContainers;

  public YarnProvisionerConfig(@JsonProperty("resourceId") ResourceId resourceId) {
    _resourceId = resourceId;
    _serializerClass = DefaultStringSerializer.class;
    _provisionerRef = ProvisionerRef.from(YarnProvisioner.class.getName());
  }

  public void setNumContainers(int numContainers) {
    _numContainers = numContainers;
  }

  public Integer getNumContainers() {
    return _numContainers;
  }

  @Override
  public ResourceId getResourceId() {
    return _resourceId;
  }

  @Override
  public ProvisionerRef getProvisionerRef() {
    return _provisionerRef;
  }

  public void setProvisionerRef(ProvisionerRef provisionerRef) {
    _provisionerRef = provisionerRef;
  }

  @Override
  public Class<? extends StringSerializer> getSerializerClass() {
    return _serializerClass;
  }

  public void setSerializerClass(Class<? extends StringSerializer> serializerClass) {
    _serializerClass = serializerClass;
  }

}
