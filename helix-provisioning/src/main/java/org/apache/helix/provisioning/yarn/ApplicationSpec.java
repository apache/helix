package org.apache.helix.provisioning.yarn;

public class ApplicationSpec {

  int minContainers;

  int maxContainers;

  String serviceClass;
    
  String targetProvider;
  
  String stateModel;
  
  String taskClass;

  int numTasks;
  
}
