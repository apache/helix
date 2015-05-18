package org.apache.helix.controller.provisioner;


public interface ContainerProvider {
	
	ContainerId allocateContainer(ContainerSpec spec);
	
	boolean deallocateContainer(ContainerId containerId);
	
	boolean startContainer(ContainerId containerId);
	
	boolean stopContainer(ContainerId containerId);
	
	ContainerState getContainerState(ContainerId  containerId); 
}
