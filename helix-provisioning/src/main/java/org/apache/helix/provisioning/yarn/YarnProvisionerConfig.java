package org.apache.helix.provisioning.yarn;

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
