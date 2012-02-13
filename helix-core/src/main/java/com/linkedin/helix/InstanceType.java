package com.linkedin.helix;

/**
 * CONTROLLER:     cluster managing component is a controller
 * PARTICIPANT:    participate in the cluster state changes
 * SPECTATOR:      interested in the state changes in the cluster
 * CONTROLLER_PARTICIPANT:
 *  special participant that competes for the leader of CONTROLLER_CLUSTER
 *  used in cluster controller of distributed mode {@HelixControllerMain}
 *
 */
public enum InstanceType
{
  CONTROLLER,
  PARTICIPANT,
  SPECTATOR,
  CONTROLLER_PARTICIPANT 
}
