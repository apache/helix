package org.apache.helix.zookeeper.zkclient.annotation;

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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * An annotation used to prefetch changed data for
 * {@link org.apache.helix.zookeeper.zkclient.IZkDataListener} in ZkClient.
 * By default, prefetch is enabled: when ZkClient handles a data change event,
 * ZkClient will read data and pass data object to
 * {@link org.apache.helix.zookeeper.zkclient.IZkDataListener#handleDataChange(String, Object)}.
 * If disabled({@code false}): ZkClient will not read data, so data object is passed as null.
 * <p>
 * Example:
 * <pre>
 * {@code @PreFetch(enabled = false)}
 *  public class MyCallback implements IZkDataListener
 * </pre>
 *
 * {@code @PreFetch(enabled = false)} means data will not be prefetched in ZkClient before
 * handling data change: data is null in
 * {@link org.apache.helix.zookeeper.zkclient.IZkDataListener#handleDataChange(String, Object)}
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface PreFetchChangedData {
  boolean enabled() default true;
}
