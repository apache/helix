package org.apache.helix.zookeeper.zkclient;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;
import org.apache.zookeeper.WatchedEvent;


/**
 * An {@link IZkChildListener} can be registered at a {@link ZkClient} for listening on zk child changes for a given
 * path.
 *
 * Node: Also this listener re-subscribes it watch for the path on each zk event (zk watches are one-timers) is is not
 * guaranteed that events on the path are missing (see http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches). An
 * implementation of this class should take that into account.
 *
 * Deprecated: This interface is kept to maintain backward compatibility, please use {@link IZkChildEventListener}.
 */
@Deprecated
public interface IZkChildListener extends IZkChildEventListener {

    /**
     * Called when the children of the given path changed.
     *
     * @param parentPath
     *            The parent path
     * @param currentChildren
     *            The children or null if the root node (parent path) was deleted.
     * @throws Exception
     */
    void handleChildChange(String parentPath, List<String> currentChildren) throws Exception;

    /**
     * Called when the children of the given path changed.
     *
     * @param parentPath The parent path
     * @param currentChildren The children or null if the root node (parent path) was deleted.
     * @param event The watched event
     */
    default void handleChildChange(String parentPath, List<String> currentChildren, WatchedEvent event)
        throws Exception {
        handleChildChange(parentPath, currentChildren);
    }
}
