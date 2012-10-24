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
package org.apache.helix;

/**
 * factory that creates cluster managers
 *
 * for zk-based cluster managers, the getZKXXX(..zkClient) that takes a zkClient parameter
 *   are intended for session expiry test purpose
 */
import org.apache.helix.manager.file.DynamicFileHelixManager;
import org.apache.helix.manager.file.StaticFileHelixManager;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.store.file.FilePropertyStore;
import org.apache.log4j.Logger;


public final class HelixManagerFactory
{
  private static final Logger logger = Logger.getLogger(HelixManagerFactory.class);

  /**
   * Construct a zk-based cluster manager enforce all types (PARTICIPANT, CONTROLLER, and
   * SPECTATOR to have a name
   * 
   * @param clusterName
   * @param instanceName
   * @param type
   * @param zkAddr
   * @return
   * @throws Exception
   */
  public static HelixManager getZKHelixManager(String clusterName,
                                               String instanceName,
                                               InstanceType type,
                                               String zkAddr) throws Exception
  {
    return new ZKHelixManager(clusterName, instanceName, type, zkAddr);
  }

  /**
   * Construct a file-based cluster manager using a static cluster-view file the
   * cluster-view file contains pre-computed state transition messages from initial
   * OFFLINE states to ideal states
   * 
   * @param clusterName
   * @param instanceName
   * @param type
   * @param clusterViewFile
   * @return
   * @throws Exception
   */
  @Deprecated
  public static HelixManager getStaticFileHelixManager(String clusterName,
                                                       String instanceName,
                                                       InstanceType type,
                                                       String clusterViewFile) throws Exception
  {
    if (type != InstanceType.PARTICIPANT)
    {
      throw new IllegalArgumentException("Static file-based cluster manager doesn't support type other than participant");
    }
    return new StaticFileHelixManager(clusterName, instanceName, type, clusterViewFile);
  }

  /**
   * Construct a dynamic file-based cluster manager
   * 
   * @param clusterName
   * @param instanceName
   * @param type
   * @param file
   *          property store: all dynamic-file based participants/controller shall use the
   *          same file property store to avoid race condition in updating files
   * @return
   * @throws Exception
   */
  @Deprecated
  public static HelixManager getDynamicFileHelixManager(String clusterName,
                                                        String instanceName,
                                                        InstanceType type,
                                                        FilePropertyStore<ZNRecord> store) throws Exception
  {
    if (type != InstanceType.PARTICIPANT && type != InstanceType.CONTROLLER)
    {
      throw new IllegalArgumentException("Dynamic file-based cluster manager doesn't support types other than participant and controller");
    }

    return new DynamicFileHelixManager(clusterName, instanceName, type, store);
  }
}
