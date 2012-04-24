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
package com.linkedin.helix.store;

public interface PropertyChangeListener<T>
{
  /**
   * Callback function when there a change in any property that starts with key
   * Its upto the implementation to handle the following different cases * key
   * is a simple key and does not have any children. PropertyStore.get(key) must
   * be used to retrieve the value * key is a prefix and has children.
   * PropertyStore.getProperties(key) must be used to retrieve all the children
   * Its important to know that PropertyStore will not be able to provide the
   * delta[old value,new value] or what child was added/deleted The
   * implementation must take care of the fact that there might be callback for
   * every child thats added/deleted. General way applications handle this is
   * keep a local cache of keys and compare against the latest keys.
   * 
   * @param key
   */
  void onPropertyChange(String key);
}
