package com.linkedin.clustermanager;

/**
 * CONTROLLER:     cluster managing component is a controller
 * PARTICIPANT:    participate in the cluster state changes
 * SPECTATOR:      interested in the state changes in the cluster
 * CONTROLLER_PARTICIPANT:
 *  special participant that becomes controller once elected as leader
 *  used in cluster controller for distributed mode {@ClusterManagerMain}
 *
 */
public enum InstanceType
{
  CONTROLLER,
  PARTICIPANT,
  SPECTATOR,
  CONTROLLER_PARTICIPANT 
}
