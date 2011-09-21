package com.linkedin.clustermanager;

public class Criteria
{
  /**
   * This can be CONTROLLER, PARTICIPANT, ROUTER
   * Cannot be null
   */
  InstanceType recipientInstanceType;
  /**
   * If true this will only be process by the instance that was running when the message was sent. If the instance process dies and comes back up it will be ignored.
   */
  boolean sessionSpecific;
  /**
   * applicable only in case PARTICIPANT use * to broadcast to all instances
   */
  String instanceName;
  /**
   * Name of the resourceGroup. Use * to send message to all resource groups owned by an instance.
   */
  String resourceGroup;
  /**
   * Resource partition. Use * to send message to all partitions of a given resourceGroup
   */
  String resourceKey;
  /**
   * State of the resource 
   */
  String resourceState;
  public InstanceType getRecipientInstanceType()
  {
    return recipientInstanceType;
  }
  public void setRecipientInstanceType(InstanceType recipientInstanceType)
  {
    this.recipientInstanceType = recipientInstanceType;
  }
  public boolean isSessionSpecific()
  {
    return sessionSpecific;
  }
  public void setSessionSpecific(boolean sessionSpecific)
  {
    this.sessionSpecific = sessionSpecific;
  }
  public String getInstanceName()
  {
    return instanceName;
  }
  public void setInstanceName(String instanceName)
  {
    this.instanceName = instanceName;
  }
  public String getResourceGroup()
  {
    return resourceGroup;
  }
  public void setResourceGroup(String resourceGroupName)
  {
    this.resourceGroup = resourceGroupName;
  }
  public String getResourceKey()
  {
    return resourceKey;
  }
  public void setResourceKey(String resourceKey)
  {
    this.resourceKey = resourceKey;
  }
  public String getResourceState()
  {
    return resourceState;
  }
  public void setResourceState(String resourceState)
  {
    this.resourceState = resourceState;
  }
  
}
