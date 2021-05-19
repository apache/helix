package org.apache.helix.api.status;

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

/**
 * Represents the management mode of the cluster:
 * 1. what type of mode it targets to be;
 * 2. what progress status it is now.
 */
public class ClusterManagementMode {
    /** Represents  */
    public enum Type {
        /** Cluster is not in any pause or maintenance mode */
        NORMAL,

        /**
         * Puts a cluster into pause mode, which will pause controller and participants.
         * This can be used to retain the cluster state.
         */
        CLUSTER_PAUSE,

        /** Pause controller only, but not participants. */
        CONTROLLER_PAUSE,

        /** Put cluster into maintenance mode. */
        MAINTENANCE
    }

    /** Current status of the cluster mode */
    public enum Status {
        /** Cluster is in progress to the target {@link Type} of mode */
        IN_PROGRESS,

        /** Cluster is fully stable in the target {@link Type} of mode */
        COMPLETED
    }

    private final Type mode;
    private final Status status;

    public ClusterManagementMode(Type mode, Status status) {
        this.mode = mode;
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    public Type getMode() {
        return mode;
    }
}
