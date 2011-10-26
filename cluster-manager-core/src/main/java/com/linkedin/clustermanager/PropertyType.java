package com.linkedin.clustermanager;

import com.linkedin.clustermanager.ClusterDataAccessor.Type;

public enum PropertyType
{
  // @formatter:off
  // CLUSTER PROPERTIES
  CONFIGS(Type.CLUSTER, true, false), 
  LIVEINSTANCES(Type.CLUSTER, false,false),
  INSTANCES(Type.CLUSTER, true, false),
  IDEALSTATES(Type.CLUSTER,false, false), 
  EXTERNALVIEW(Type.CLUSTER, true, false), 
  STATEMODELDEFS(Type.CLUSTER, true, false), 
  CONTROLLER(Type.CLUSTER, true, false),
  PROPERTYSTORE(Type.CLUSTER, true, false),
  //INSTANCE PROPERTIES
  MESSAGES(Type.INSTANCE, true, true, true), 
  CURRENTSTATES(Type.INSTANCE, true,true, false),
  STATUSUPDATES(Type.INSTANCE, true, true, false), 
  ERRORS(Type.INSTANCE, true, true), 
  HEALTHREPORT(Type.INSTANCE, true, false,false), 
  //CONTROLLER PROPERTY
  LEADER(Type.CONTROLLER, false, false, true, true), 
  HISTORY(Type.CONTROLLER, true, true, true),
  PAUSE(Type.CONTROLLER, false,false, true),
  MESSAGES_CONTROLLER(Type.CONTROLLER, true, false, true), 
  STATUSUPDATES_CONTROLLER(Type.CONTROLLER, true, true, true), 
  ERRORS_CONTROLLER(Type.CONTROLLER,true, true, true);
  // @formatter:on
  Type type;
  boolean isPersistent;

  boolean mergeOnUpdate;

  boolean updateOnlyOnExists;

  boolean createIfAbsent;

  
  private PropertyType(Type type, boolean isPersistent, boolean mergeOnUpdate)
  {
    this(type, isPersistent, mergeOnUpdate, false);
  }

  public boolean isCreateIfAbsent() {
		return createIfAbsent;
	}

	public void setCreateIfAbsent(boolean createIfAbsent) {
		this.createIfAbsent = createIfAbsent;
	}
	private PropertyType(Type type, boolean isPersistent,
      boolean mergeOnUpdate, boolean updateOnlyOnExists){
	   this(type, isPersistent, mergeOnUpdate, false, false);
	}
	private PropertyType(Type type, boolean isPersistent,
      boolean mergeOnUpdate, boolean updateOnlyOnExists,boolean createIfAbsent)
  {
    this.type = type;
    this.isPersistent = isPersistent;
    this.mergeOnUpdate = mergeOnUpdate;
    this.updateOnlyOnExists = updateOnlyOnExists;
		this.createIfAbsent = createIfAbsent;
  }

  public Type getType()
  {
    return type;
  }

  public void setType(Type type)
  {
    this.type = type;
  }

  public boolean isPersistent()
  {
    return isPersistent;
  }

  public void setPersistent(boolean isPersistent)
  {
    this.isPersistent = isPersistent;
  }

  public boolean isMergeOnUpdate()
  {
    return mergeOnUpdate;
  }

  public void setMergeOnUpdate(boolean mergeOnUpdate)
  {
    this.mergeOnUpdate = mergeOnUpdate;
  }

  public boolean isUpdateOnlyOnExists()
  {
    return updateOnlyOnExists;
  }

  public void setUpdateOnlyOnExists(boolean updateOnlyOnExists)
  {
    this.updateOnlyOnExists = updateOnlyOnExists;
  }
  
}