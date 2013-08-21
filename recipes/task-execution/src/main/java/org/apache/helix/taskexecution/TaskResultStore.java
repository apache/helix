package org.apache.helix.taskexecution;

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

import java.util.List;
import java.util.Map;

public interface TaskResultStore {
  public boolean exists(String key) throws Exception;

  public long llen(String key) throws Exception;

  public void rpush(String key, String value) throws Exception;

  public List<String> lrange(final String key, final long start, final long end) throws Exception;

  public void ltrim(final String key, final long start, final long end) throws Exception;

  public Long hincrBy(final String key, final String field, final long value) throws Exception;

  public Map<String, String> hgetAll(final String key) throws Exception;
}
