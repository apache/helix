/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix;


enum Type
{
  CLUSTER, INSTANCE, CONTROLLER, RESOURCE;
}

public enum PropertyType
{

  // @formatter:off
  // CLUSTER PROPERTIES
  CONFIGS(Type.CLUSTER, true, false, false, false, true),
  LIVEINSTANCES(Type.CLUSTER, false, false, false, true, true),
  INSTANCES(Type.CLUSTER, true, false),
  IDEALSTATES(Type.CLUSTER, true, false, false, false, true),
  EXTERNALVIEW(Type.CLUSTER, true, false),
  STATEMODELDEFS(Type.CLUSTER, true, false, false, false, true),
  CONTROLLER(Type.CLUSTER, true, false),
  PROPERTYSTORE(Type.CLUSTER, true, false),
  HELIX_PROPERTYSTORE(Type.CLUSTER, true, false),

  // INSTANCE PROPERTIES
  MESSAGES(Type.INSTANCE, true, true, true),
  CURRENTSTATES(Type.INSTANCE, true, true, false, false, true),
  STATUSUPDATES(Type.INSTANCE, true, true, false, false, false, true),
  ERRORS(Type.INSTANCE, true, true),
  HEALTHREPORT(Type.INSTANCE, true, false, false, false, false, true),

  // CONTROLLER PROPERTY
  LEADER(Type.CONTROLLER, false, false, true, true),
  HISTORY(Type.CONTROLLER, true, true, true),
  PAUSE(Type.CONTROLLER, true, false, true),
  MESSAGES_CONTROLLER(Type.CONTROLLER, true, false, true),
  STATUSUPDATES_CONTROLLER(Type.CONTROLLER, true, true, true),
  ERRORS_CONTROLLER(Type.CONTROLLER, true, true, true),
  PERSISTENTSTATS(Type.CONTROLLER, true, false, false, false),
  ALERTS(Type.CONTROLLER, true, false, false, false),
  ALERT_STATUS(Type.CONTROLLER, true, false, false, false),
  ALERT_HISTORY(Type.CONTROLLER, true, false, false, false);

  // @formatter:on

  Type    type;
  
  boolean isPersistent;

  boolean mergeOnUpdate;

  boolean updateOnlyOnExists;

  boolean createOnlyIfAbsent;

  /**
   * "isCached" defines whether the property is cached in data accessor if data is cached,
   * then read from zk can be optimized
   */
  boolean isCached;

  boolean usePropertyTransferServer;

  private PropertyType(Type type, boolean isPersistent, boolean mergeOnUpdate)
  {
    this(type, isPersistent, mergeOnUpdate, false);
  }

  private PropertyType(Type type,
                       boolean isPersistent,
                       boolean mergeOnUpdate,
                       boolean updateOnlyOnExists)
  {
    this(type, isPersistent, mergeOnUpdate, false, false);
  }

  private PropertyType(Type type,
                       boolean isPersistent,
                       boolean mergeOnUpdate,
                       boolean updateOnlyOnExists,
                       boolean createOnlyIfAbsent)
  {
    this(type, isPersistent, mergeOnUpdate, updateOnlyOnExists, createOnlyIfAbsent, false);
  }

  private PropertyType(Type type,
                       boolean isPersistent,
                       boolean mergeOnUpdate,
                       boolean updateOnlyOnExists,
                       boolean createOnlyIfAbsent,
                       boolean isCached)
  {
    this(type,
         isPersistent,
         mergeOnUpdate,
         updateOnlyOnExists,
         createOnlyIfAbsent,
         isCached,
         false);
  }

  private PropertyType(Type type,
                       boolean isPersistent,
                       boolean mergeOnUpdate,
                       boolean updateOnlyOnExists,
                       boolean createOnlyIfAbsent,
                       boolean isCached,
                       boolean isAsyncWrite)
  {
    this.type = type;
    this.isPersistent = isPersistent;
    this.mergeOnUpdate = mergeOnUpdate;
    this.updateOnlyOnExists = updateOnlyOnExists;
    this.createOnlyIfAbsent = createOnlyIfAbsent;
    this.isCached = isCached;
    this.usePropertyTransferServer = isAsyncWrite;
  }

  public boolean isCreateOnlyIfAbsent()
  {
    return createOnlyIfAbsent;
  }

  public void setCreateIfAbsent(boolean createIfAbsent)
  {
    this.createOnlyIfAbsent = createIfAbsent;
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

  public boolean isCached()
  {
    return isCached;
  }

  public boolean usePropertyTransferServer()
  {
    return usePropertyTransferServer;
  }

}
