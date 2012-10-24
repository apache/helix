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
package org.apache.helix.store;

import org.apache.helix.manager.zk.ByteArraySerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.file.FilePropertyStore;
import org.apache.helix.store.zk.ZKPropertyStore;
import org.apache.log4j.Logger;


public class PropertyStoreFactory
{
  private static Logger LOG = Logger.getLogger(PropertyStoreFactory.class);

  public static <T extends Object> PropertyStore<T> getZKPropertyStore(String zkAddress,
                                                                       PropertySerializer<T> serializer,
                                                                       String rootNamespace)
  {
    if (zkAddress == null || serializer == null || rootNamespace == null)
    {
      throw new IllegalArgumentException("zkAddress|serializer|rootNamespace can't be null");
    }

    LOG.info("Get a zk property store. zkAddr: " + zkAddress + ", root: " + rootNamespace);
    ZkClient zkClient =
        new ZkClient(zkAddress,
                     ZkClient.DEFAULT_SESSION_TIMEOUT,
                     ZkClient.DEFAULT_CONNECTION_TIMEOUT,
                     new ByteArraySerializer());
    return new ZKPropertyStore<T>(zkClient, serializer, rootNamespace);
  }

  public static <T extends Object> PropertyStore<T> getFilePropertyStore(PropertySerializer<T> serializer,
                                                                         String rootNamespace,
                                                                         PropertyJsonComparator<T> comparator)
  {
    if (comparator == null || serializer == null || rootNamespace == null)
    {
      throw new IllegalArgumentException("arguments can't be null");
    }

    LOG.info("Get a file property store. root: " + rootNamespace);
    FilePropertyStore<T> store =
        new FilePropertyStore<T>(serializer, rootNamespace, comparator);
    return store;

  }

}
