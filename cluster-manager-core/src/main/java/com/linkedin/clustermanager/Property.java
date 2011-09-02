package com.linkedin.clustermanager;

import static com.linkedin.clustermanager.Property.Type.*;

public enum Property
{
	//@formatter:off

	//CLUSTER PROPERTY
	CONFIGS(CLUSTER,true, false,"/{cluster}/CONFIGS"), 
	LIVEINSTANCES(CLUSTER,false, false,"/{cluster}/LIVEINSTANCES"), 
	IDEALSTATES(CLUSTER,true, false,"/{cluster}/IDEALSTATES"), 
	EXTERNALVIEW(CLUSTER,true, false,"/{cluster}/EXTERNALVIEW"), 
	STATEMODELDEFS(CLUSTER,true, false,"/{cluster}/STATEMODELDEFS"), 
	//INSTANCE PROPERTY
	MESSAGES(INSTANCE,true, true, true,"/{cluster}/INSTANCES/{instanceName}/MESSAGES"),
	CURRENTSTATES(INSTANCE,true, true, false,"/{cluster}/INSTANCES/{instanceName}/CURRENTSTATES"), 
	SESSIONCURRENTSTATES(INSTANCE,true, true, false,"/{cluster}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}"), 
	STATUSUPDATES(INSTANCE,true, true, false,"/{cluster}/INSTANCES/{instanceName}/STATUSUPDATES"), 
	SESSIONSTATUSUPDATES(INSTANCE,true, true, false,"/{cluster}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}_{resourceGroup}"), 
  ERRORS(INSTANCE,true, true,"/{cluster}/INSTANCES/{instanceName}/ERRORS/{sessionId}_{resourceGroup}"),
	//CONTROLLER PROPERTY
	CONTROLLER_EPHEMERAL_PROP(CONTROLLER,false, false, true,"/{cluster}/CONTROLLERS/EphemeralProps"),
	CONTROLLER_PERSISTENT_PROP(CONTROLLER,true, true, true,"/{cluster}/CONTROLLERS/PersistenProps"),

	//CONTROLLER PROPERTY
	SPECTATOR_EPHEMERAL_PROP(CONTROLLER,false, false, true,"/{cluster}/CONTROLLERS/EphemeralProps"),
	SPECTATORS(CONTROLLER,true, true, true,"/{cluster}/CONTROLLERS/PersistenProps");

  //@formatter:on
	Type type;

	boolean isPersistent;

	boolean mergeOnUpdate;

	boolean updateOnlyOnExists;

	String pathFormat;

	private Property(Type type, boolean isPersistent, boolean mergeOnUpdate,
	    String pathFormat)
	{
		this(type, isPersistent, mergeOnUpdate, false, pathFormat);
	}

	private Property(Type type, boolean isPersistent, boolean mergeOnUpdate,
	    boolean updateOnlyOnExists, String pathFormat)
	{
		this.type = type;
		this.isPersistent = isPersistent;
		this.mergeOnUpdate = mergeOnUpdate;
		this.updateOnlyOnExists = updateOnlyOnExists;
		this.pathFormat = pathFormat;
	}

	enum Type
	{
		CLUSTER, INSTANCE, CONTROLLER, SPECTATOR
	}

	static Param cluster(String clusterName)
	{
		return new Param("clusterName", clusterName);
	}

	static class Param
	{
		public Param(String paramKey, String paramValue)
		{
			this.paramKey = paramKey;
			this.paramValue = paramValue;
		}

		String paramKey;
		String paramValue;
	}
}
