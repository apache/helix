package org.apache.helix;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.Map;

/**
 * Metadata associated with a notification event and the current state of the cluster
 */
public class NotificationContext {
  /**
   * keys used for object map
   */
  public enum MapKey {
    TASK_EXECUTOR,
    CURRENT_STATE_UPDATE,
    HELIX_TASK_RESULT
  }

  private Map<String, Object> _map;

  private HelixManager _manager;
  private Type _type;
  private String _pathChanged;
  private String _eventName;

  /**
   * Get the name associated with the event
   * @return event name
   */
  public String getEventName() {
    return _eventName;
  }

  /**
   * Set the name associated with the event
   * @param eventName the event name
   */
  public void setEventName(String eventName) {
    _eventName = eventName;
  }

  /**
   * Instantiate with a HelixManager
   * @param manager {@link HelixManager} object
   */
  public NotificationContext(HelixManager manager) {
    _manager = manager;
    _map = new HashMap<String, Object>();
  }

  /**
   * Get the HelixManager associated with this notification
   * @return {@link HelixManager} object
   */
  public HelixManager getManager() {
    return _manager;
  }

  /**
   * Get a map describing the update (keyed on {@link MapKey})
   * @return the object map describing the update
   */
  public Map<String, Object> getMap() {
    return _map;
  }

  /**
   * Get the type of the notification
   * @return the notification type
   */
  public Type getType() {
    return _type;
  }

  /**
   * Set the HelixManager associated with this notification
   * @param manager {@link HelixManager} object
   */
  public void setManager(HelixManager manager) {
    this._manager = manager;
  }

  /**
   * Add notification metadata
   * @param key String value of a {@link MapKey}
   * @param value
   */
  public void add(String key, Object value) {
    _map.put(key, value);
  }

  /**
   * Set the notification map
   * @param map
   */
  public void setMap(Map<String, Object> map) {
    this._map = map;
  }

  /**
   * Set the notification type
   * @param {@link Type} object
   */
  public void setType(Type type) {
    this._type = type;
  }

  /**
   * Get a notification attribute
   * @param key String from a {@link MapKey}
   * @return
   */
  public Object get(String key) {
    return _map.get(key);
  }

  /**
   * Valid types of notifications
   */
  public enum Type {
    INIT,
    CALLBACK,
    FINALIZE
  }

  /**
   * Get the path changed status
   * @return String corresponding to the path change
   */
  public String getPathChanged() {
    return _pathChanged;
  }

  /**
   * Set the path changed status
   * @param pathChanged
   */
  public void setPathChanged(String pathChanged) {
    this._pathChanged = pathChanged;
  }
}
