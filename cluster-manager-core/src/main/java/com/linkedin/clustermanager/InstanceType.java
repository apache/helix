package com.linkedin.clustermanager;

public enum InstanceType
{
    CONTROLLER, // cluster managing component is a controller.
    PARTICIPANT, // participate in the cluster state changes
    SPECTATOR
    // interested in the state changes in the cluster
}
