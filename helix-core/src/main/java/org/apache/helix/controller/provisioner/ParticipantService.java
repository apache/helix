package org.apache.helix.controller.provisioner;

public interface ParticipantService {

  boolean init(ServiceConfig serviceConfig);
  
  boolean start();
  
  boolean stop();
}
